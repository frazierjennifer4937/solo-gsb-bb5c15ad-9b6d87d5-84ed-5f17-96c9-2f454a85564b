"""Persistence and the ordered catch-up merge algorithm."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from . import db


class ConflictError(Exception):
    """Raised when a replay/clash contradicts what is already stored (HTTP 409)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class StreamState:
    stream_id: str
    balance: int
    next_sequence: int
    pending_sequences: list[int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream_id": self.stream_id,
            "balance": self.balance,
            "next_sequence": self.next_sequence,
            "pending_sequences": self.pending_sequences,
        }


@dataclass(frozen=True)
class ForkResult:
    source_stream_id: str
    fork_id: str
    target_stream_id: str
    through_sequence: int
    balance: int
    next_sequence: int
    replayed: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_stream_id": self.source_stream_id,
            "fork_id": self.fork_id,
            "target_stream_id": self.target_stream_id,
            "through_sequence": self.through_sequence,
            "balance": self.balance,
            "next_sequence": self.next_sequence,
            "replayed": self.replayed,
        }


def _get_stream(conn: sqlite3.Connection, stream_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT stream_id, balance, next_sequence FROM streams WHERE stream_id = ?",
        (stream_id,),
    ).fetchone()


def _get_event(
    conn: sqlite3.Connection, stream_id: str, event_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT event_id, sequence, delta, applied "
        "FROM events WHERE stream_id = ? AND event_id = ?",
        (stream_id, event_id),
    ).fetchone()


def _get_event_by_sequence(
    conn: sqlite3.Connection, stream_id: str, sequence: int
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT event_id, sequence, delta, applied "
        "FROM events WHERE stream_id = ? AND sequence = ?",
        (stream_id, sequence),
    ).fetchone()


def _pending_sequences(conn: sqlite3.Connection, stream_id: str) -> list[int]:
    rows = conn.execute(
        "SELECT sequence FROM events WHERE stream_id = ? AND applied = 0 "
        "ORDER BY sequence ASC",
        (stream_id,),
    ).fetchall()
    return [row["sequence"] for row in rows]


def get_stream_state(stream_id: str) -> StreamState | None:
    with db.read_connection() as conn:
        stream = _get_stream(conn, stream_id)
        if stream is None:
            return None
        return StreamState(
            stream_id=stream["stream_id"],
            balance=stream["balance"],
            next_sequence=stream["next_sequence"],
            pending_sequences=_pending_sequences(conn, stream_id),
        )


def submit_event(
    stream_id: str, event_id: str, sequence: int, delta: int
) -> tuple[StreamState, bool]:
    """Persist one event and run contiguous catch-up.

    Returns ``(state, replayed)`` where ``replayed`` is True when the event was
    already stored (an idempotent replay). Raises ``ConflictError`` on 409 cases.
    """
    with db.write_transaction() as conn:
        stream = _get_stream(conn, stream_id)
        if stream is None:
            conn.execute(
                "INSERT INTO streams (stream_id, balance, next_sequence) VALUES (?, 0, 0)",
                (stream_id,),
            )
            balance, next_sequence = 0, 0
        else:
            balance, next_sequence = stream["balance"], stream["next_sequence"]

        existing = _get_event(conn, stream_id, event_id)
        if existing is not None:
            # Idempotent replay: same event_id must carry identical content.
            if existing["sequence"] != sequence or existing["delta"] != delta:
                raise ConflictError(
                    "replay_conflict",
                    "event_id already exists with different sequence/delta",
                )
            replayed = True
        else:
            clash = _get_event_by_sequence(conn, stream_id, sequence)
            if clash is not None:
                # The UNIQUE(stream_id, sequence) index also guards this, but
                # producing a clear error here keeps the response informative.
                raise ConflictError(
                    "sequence_conflict",
                    f"sequence {sequence} already occupied by event_id "
                    f"{clash['event_id']!r}",
                )
            conn.execute(
                "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
                "VALUES (?, ?, ?, ?, 0)",
                (stream_id, event_id, sequence, delta),
            )
            replayed = False

        # Contiguous catch-up, executed inside the same transaction.
        expected = next_sequence
        while True:
            row = conn.execute(
                "SELECT event_id, delta FROM events "
                "WHERE stream_id = ? AND sequence = ? AND applied = 0",
                (stream_id, expected),
            ).fetchone()
            if row is None:
                break
            balance += row["delta"]
            conn.execute(
                "UPDATE events SET applied = 1 "
                "WHERE stream_id = ? AND sequence = ?",
                (stream_id, expected),
            )
            expected += 1

        conn.execute(
            "UPDATE streams SET balance = ?, next_sequence = ? WHERE stream_id = ?",
            (balance, expected, stream_id),
        )

        state = StreamState(
            stream_id=stream_id,
            balance=balance,
            next_sequence=expected,
            pending_sequences=_pending_sequences(conn, stream_id),
        )
        return state, replayed


def _prefix_balance(conn: sqlite3.Connection, stream_id: str, through: int) -> int:
    """Sum deltas of the applied events with sequence in ``[0, through)``."""
    row = conn.execute(
        "SELECT COALESCE(SUM(delta), 0) AS balance FROM events "
        "WHERE stream_id = ? AND applied = 1 AND sequence >= 0 AND sequence < ?",
        (stream_id, through),
    ).fetchone()
    return int(row["balance"])


def _get_fork(
    conn: sqlite3.Connection, source_stream_id: str, fork_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT source_stream_id, fork_id, target_stream_id, through_sequence "
        "FROM forks WHERE source_stream_id = ? AND fork_id = ?",
        (source_stream_id, fork_id),
    ).fetchone()


def fork_stream(
    source_stream_id: str, fork_id: str, target_stream_id: str, through_sequence: int
) -> ForkResult | None:
    """Copy the source stream's applied prefix ``[0, through_sequence)`` into a new
    appendable target stream, all inside one ``BEGIN IMMEDIATE`` transaction.

    Returns ``None`` when the source stream does not exist (HTTP 404). Raises
    :class:`ConflictError` (HTTP 409) when:

    * the same ``fork_id`` is retried with different parameters
      (``fork_parameters_conflict``);
    * ``through_sequence`` is outside ``0..source.next_sequence``
      (``through_sequence_out_of_range``);
    * the target stream already exists (``target_stream_exists``).

    A retry carrying the exact same parameters returns the existing fork.
    """
    with db.write_transaction() as conn:
        stream = _get_stream(conn, source_stream_id)
        if stream is None:
            return None

        existing_fork = _get_fork(conn, source_stream_id, fork_id)
        if existing_fork is not None:
            # Idempotent retry: parameters must match the stored fork.
            if (
                existing_fork["target_stream_id"] != target_stream_id
                or existing_fork["through_sequence"] != through_sequence
            ):
                raise ConflictError(
                    "fork_parameters_conflict",
                    f"fork_id {fork_id!r} already exists for stream "
                    f"{source_stream_id!r} with different parameters",
                )
            replayed = True
            through_sequence = existing_fork["through_sequence"]
            target_stream_id = existing_fork["target_stream_id"]
            # The fork-point snapshot is immutable: the source events it was
            # built from never change, so derive the initial balance from them
            # rather than the target's current balance (it may have advanced).
            balance = _prefix_balance(conn, source_stream_id, through_sequence)
            return ForkResult(
                source_stream_id=source_stream_id,
                fork_id=fork_id,
                target_stream_id=target_stream_id,
                through_sequence=through_sequence,
                balance=balance,
                next_sequence=through_sequence,
                replayed=replayed,
            )

        if not 0 <= through_sequence <= stream["next_sequence"]:
            raise ConflictError(
                "through_sequence_out_of_range",
                f"through_sequence must be within 0..{stream['next_sequence']} "
                f"(next_sequence of {source_stream_id!r}), got {through_sequence}",
            )

        if _get_stream(conn, target_stream_id) is not None:
            raise ConflictError(
                "target_stream_exists",
                f"target stream {target_stream_id!r} already exists",
            )

        balance = _prefix_balance(conn, source_stream_id, through_sequence)

        # Insert target stream, fork record and the copied events atomically.
        conn.execute(
            "INSERT INTO streams (stream_id, balance, next_sequence) VALUES (?, ?, ?)",
            (target_stream_id, balance, through_sequence),
        )
        conn.execute(
            "INSERT INTO forks (source_stream_id, fork_id, target_stream_id, "
            "through_sequence, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                source_stream_id,
                fork_id,
                target_stream_id,
                through_sequence,
                int(time.time() * 1000),
            ),
        )
        conn.execute(
            "INSERT INTO events "
            "(stream_id, event_id, sequence, delta, applied) "
            "SELECT ?, event_id, sequence, delta, 1 FROM events "
            "WHERE stream_id = ? AND applied = 1 AND sequence >= 0 AND sequence < ?",
            (target_stream_id, source_stream_id, through_sequence),
        )

        return ForkResult(
            source_stream_id=source_stream_id,
            fork_id=fork_id,
            target_stream_id=target_stream_id,
            through_sequence=through_sequence,
            balance=balance,
            next_sequence=through_sequence,
            replayed=False,
        )
