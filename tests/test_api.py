"""End-to-end API tests (requires fastapi + httpx, installed via requirements-dev.txt)."""

from __future__ import annotations

import os
import tempfile

import pytest


@pytest.fixture(scope="session")
def client(tmp_path_factory):
    db_dir = tmp_path_factory.mktemp("db")
    os.environ["DATABASE_PATH"] = str(db_dir / "test_events.db")

    from fastapi.testclient import TestClient  # noqa: E402

    from app.main import app  # noqa: E402

    with TestClient(app) as c:
        yield c


def _post(client, stream_id, event_id, sequence, delta):
    return client.post(
        f"/streams/{stream_id}/events",
        json={"event_id": event_id, "sequence": sequence, "delta": delta},
    )


def test_in_order_events_accumulate(client):
    r = _post(client, "s-ordered", "e0", 0, 100)
    assert r.status_code == 201
    assert r.json()["stream"] == {
        "stream_id": "s-ordered",
        "balance": 100,
        "next_sequence": 1,
        "pending_sequences": [],
    }

    r = _post(client, "s-ordered", "e1", 1, -30)
    assert r.status_code == 201
    assert r.json()["stream"]["balance"] == 70
    assert r.json()["stream"]["next_sequence"] == 2


def test_gap_is_buffered_then_caught_up_atomically(client):
    # Sequence 1 arrives before sequence 0: nothing is applied yet.
    r = _post(client, "s-gap", "e1", 1, 200)
    assert r.status_code == 201
    body = r.json()
    assert body["stream"]["balance"] == 0
    assert body["stream"]["next_sequence"] == 0
    assert body["stream"]["pending_sequences"] == [1]

    r = client.get("/streams/s-gap")
    assert r.json()["pending_sequences"] == [1]

    # Missing event arrives: both events are applied in the same transaction.
    r = _post(client, "s-gap", "e0", 0, 50)
    assert r.status_code == 201
    body = r.json()
    assert body["stream"]["balance"] == 250
    assert body["stream"]["next_sequence"] == 2
    assert body["stream"]["pending_sequences"] == []


def test_replay_is_idempotent(client):
    _post(client, "s-rep", "e0", 0, 10)

    r = _post(client, "s-rep", "e0", 0, 10)
    assert r.status_code == 200
    assert r.json()["replayed"] is True
    assert r.json()["stream"]["balance"] == 10
    assert r.json()["stream"]["next_sequence"] == 1


def test_replay_with_different_content_returns_409(client):
    _post(client, "s-conf", "e0", 0, 10)

    r = _post(client, "s-conf", "e0", 1, 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "replay_conflict"

    r = _post(client, "s-conf", "e0", 0, 999)
    assert r.status_code == 409


def test_duplicate_sequence_different_event_id_returns_409(client):
    _post(client, "s-dup", "e0", 0, 10)

    r = _post(client, "s-dup", "other-event", 0, 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "sequence_conflict"
    # The conflicting event was never stored.
    assert client.get("/streams/s-dup").json()["next_sequence"] == 1


def test_get_unknown_stream_returns_404(client):
    r = client.get("/streams/does-not-exist")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_validation_rejects_bad_payloads(client):
    assert _post(client, "s-val", "", 0, 1).status_code == 422
    assert _post(client, "s-val", "e", -1, 1).status_code == 422
    assert _post(client, "s-val", "e", 0, "not-an-int").status_code == 422
    assert _post(client, "s-val", "e", True, 1).status_code == 422


def test_large_out_of_order_batch_catches_up(client):
    # Submit 1..9 (all buffered because 0 is missing), then 0.
    for seq in range(1, 10):
        r = _post(client, "s-batch", f"e{seq}", seq, seq)
        assert r.json()["stream"]["next_sequence"] == 0
    assert client.get("/streams/s-batch").json()["pending_sequences"] == list(range(1, 10))

    r = _post(client, "s-batch", "e0", 0, 0)
    body = r.json()
    assert body["stream"]["next_sequence"] == 10
    assert body["stream"]["balance"] == sum(range(10))
    assert body["stream"]["pending_sequences"] == []


def test_concurrent_submissions_apply_each_sequence_once(client):
    import threading

    def post(seq):
        _post(client, "s-conc", f"e{seq}", seq, 1)

    threads = [threading.Thread(target=post, args=(seq,)) for seq in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state = client.get("/streams/s-conc").json()
    assert state["next_sequence"] == 20
    assert state["balance"] == 20
    assert state["pending_sequences"] == []


# --------------------------------------------------------------------------- #
# Consumer cursor: claim / ack leases
# --------------------------------------------------------------------------- #


def _seed_applied_events(client, stream_id, count, start=0):
    for seq in range(start, start + count):
        r = _post(client, stream_id, f"{stream_id}-e{seq}", seq, seq + 1)
        assert r.status_code in (200, 201)


def _claim(client, stream_id, consumer_id, max_events):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/claim",
        json={"max_events": max_events},
    )


def _ack(client, stream_id, consumer_id, token):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/ack",
        json={"lease_token": token},
    )


def test_claim_returns_contiguous_applied_batch(client):
    _seed_applied_events(client, "c-basic", 5)

    r = _claim(client, "c-basic", "worker-1", 3)
    assert r.status_code == 200
    body = r.json()
    assert body["from_sequence"] == 0
    assert body["to_sequence"] == 3
    assert [e["sequence"] for e in body["events"]] == [0, 1, 2]
    assert [e["delta"] for e in body["events"]] == [1, 2, 3]
    assert body["lease_token"]
    assert body["expires_at"] > 0


def test_claim_skips_events_not_yet_applied_due_to_gap(client):
    # Sequences 1 and 2 arrive first and stay buffered (0 is missing).
    _post(client, "c-gap", "c-gap-e1", 1, 10)
    _post(client, "c-gap", "c-gap-e2", 2, 20)

    r = _claim(client, "c-gap", "worker", 10)
    assert r.status_code == 200
    body = r.json()
    # Nothing is applied yet: empty batch, but a lease still exists.
    assert body["events"] == []
    assert body["from_sequence"] == body["to_sequence"] == 0

    # Sequence 0 arrives; only it is applied (1 and 2 still pending the old gap?
    # actually all three become contiguous -> applied 0,1,2).
    _post(client, "c-gap", "c-gap-e0", 0, 5)

    # Expire the empty-batch lease, then re-claim.
    from app import consumers

    consumers._now_ms = lambda: body["expires_at"] + 1
    r = _claim(client, "c-gap", "worker", 10)
    assert [e["sequence"] for e in r.json()["events"]] == [0, 1, 2]


def test_reclaim_within_lease_returns_same_batch_and_token(client):
    _seed_applied_events(client, "c-same", 4)

    first = _claim(client, "c-same", "worker", 2).json()
    second = _claim(client, "c-same", "worker", 999).json()
    # Same batch, same token, same expiry, regardless of the new max_events.
    assert second == first


def test_ack_advances_cursor_and_next_claim_continues(client):
    _seed_applied_events(client, "c-ack", 5)

    batch = _claim(client, "c-ack", "worker", 2).json()
    r = _ack(client, "c-ack", "worker", batch["lease_token"])
    assert r.status_code == 200
    assert r.json()["cursor"] == 2

    next_batch = _claim(client, "c-ack", "worker", 10).json()
    assert next_batch["from_sequence"] == 2
    assert [e["sequence"] for e in next_batch["events"]] == [2, 3, 4]


def test_ack_with_wrong_token_returns_409_and_keeps_lease(client):
    _seed_applied_events(client, "c-wrong", 2)
    _claim(client, "c-wrong", "worker", 2)

    r = _ack(client, "c-wrong", "worker", "not-the-real-token")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_token_mismatch"

    # The valid lease is still usable and still covers the same batch.
    batch = _claim(client, "c-wrong", "worker", 2).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1]
    assert _ack(client, "c-wrong", "worker", batch["lease_token"]).status_code == 200


def test_ack_without_claim_returns_409(client):
    _seed_applied_events(client, "c-nolease", 1)
    r = _ack(client, "c-nolease", "fresh-worker", "anything")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_not_active"


def test_old_token_after_reclaim_is_rejected_with_409(client):
    _seed_applied_events(client, "c-old", 2)

    first = _claim(client, "c-old", "worker", 2).json()
    from app import consumers

    consumers._now_ms = lambda: first["expires_at"] + 1
    second = _claim(client, "c-old", "worker", 2).json()
    assert second["lease_token"] != first["lease_token"]

    # The old (expired) token must not advance the cursor.
    r = _ack(client, "c-old", "worker", first["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_token_mismatch"

    assert _ack(client, "c-old", "worker", second["lease_token"]).status_code == 200


def test_expired_lease_token_is_rejected_but_batch_reclaimable(client):
    _seed_applied_events(client, "c-exp", 2)
    batch = _claim(client, "c-exp", "worker", 2).json()

    from app import consumers

    consumers._now_ms = lambda: batch["expires_at"] + 1

    r = _ack(client, "c-exp", "worker", batch["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_expired"

    # Lease expired: the same batch is handed out again with a new token.
    again = _claim(client, "c-exp", "worker", 2).json()
    assert [e["sequence"] for e in again["events"]] == [0, 1]
    assert again["lease_token"] != batch["lease_token"]
    assert _ack(client, "c-exp", "worker", again["lease_token"]).json()["cursor"] == 2


def test_two_consumers_have_independent_cursors(client):
    _seed_applied_events(client, "c-two", 3)

    b1 = _claim(client, "c-two", "worker-a", 1).json()
    b2 = _claim(client, "c-two", "worker-b", 1).json()
    assert b1["events"][0]["sequence"] == 0
    assert b2["events"][0]["sequence"] == 0
    assert b1["lease_token"] != b2["lease_token"]

    assert _ack(client, "c-two", "worker-a", b1["lease_token"]).json()["cursor"] == 1
    assert _ack(client, "c-two", "worker-b", b2["lease_token"]).json()["cursor"] == 1


def test_claim_on_unknown_stream_returns_404(client):
    r = _claim(client, "no-such-stream", "worker", 10)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_ack_on_unknown_stream_returns_404(client):
    r = _ack(client, "no-such-stream", "worker", "token")
    assert r.status_code == 404


def test_claim_validation_rejects_bad_max_events(client):
    assert _claim(client, "c-val", "worker", 0).status_code == 422
    assert _claim(client, "c-val", "worker", -1).status_code == 422
    assert _claim(client, "c-val", "worker", True).status_code == 422
    assert client.post(
        "/streams/c-val/consumers/worker/claim", json={}
    ).status_code == 422
    assert client.post(
        "/streams/c-val/consumers/worker/ack", json={}
    ).status_code == 422


def test_concurrent_claims_never_produce_two_valid_leases(client):
    import threading

    _seed_applied_events(client, "c-conc", 10)
    tokens: list[str] = []

    def do_claim():
        r = _claim(client, "c-conc", "worker", 5)
        tokens.append(r.json()["lease_token"])

    threads = [threading.Thread(target=do_claim) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every concurrent claimant observed the exact same valid lease.
    assert len(tokens) == 20
    assert len(set(tokens)) == 1

    batch = _claim(client, "c-conc", "worker", 5).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1, 2, 3, 4]
    assert _ack(client, "c-conc", "worker", tokens[0]).json()["cursor"] == 5


def test_events_arriving_after_claim_appear_after_ack(client):
    _seed_applied_events(client, "c-late", 2)
    batch = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1]

    # New events land while the batch is leased; they do not change the lease.
    _seed_applied_events(client, "c-late", 2, start=2)
    same = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in same["events"]] == [0, 1]

    _ack(client, "c-late", "worker", batch["lease_token"])
    after = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in after["events"]] == [2, 3]


def test_ack_of_empty_batch_advances_nothing(client):
    # Stream exists but no applied events.
    _post(client, "c-empty", "c-empty-e1", 1, 1)  # buffered, gap at 0
    batch = _claim(client, "c-empty", "worker", 5).json()
    assert batch["events"] == []
    assert batch["from_sequence"] == batch["to_sequence"] == 0

    r = _ack(client, "c-empty", "worker", batch["lease_token"])
    assert r.status_code == 200
    assert r.json()["cursor"] == 0


# --------------------------------------------------------------------------- #
# Stream forks: POST /streams/{stream_id}/forks
# --------------------------------------------------------------------------- #


def _fork(client, source_id, fork_id, target_id, through):
    return client.post(
        f"/streams/{source_id}/forks",
        json={
            "fork_id": fork_id,
            "target_stream_id": target_id,
            "through_sequence": through,
        },
    )


def _seed_fork_source(client, stream_id="f-src"):
    # Applied prefix [0, 5): deltas 10, 20, 30, 40, 50 -> balance 150.
    for seq in range(5):
        r = _post(client, stream_id, f"{stream_id}-e{seq}", seq, (seq + 1) * 10)
        assert r.status_code in (200, 201)
    # Buffered (not applied) events beyond the gap must never be copied.
    _post(client, stream_id, f"{stream_id}-e6", 6, 600)
    _post(client, stream_id, f"{stream_id}-e7", 7, 700)


def test_fork_copies_applied_prefix_and_target_is_appendable(client):
    _seed_fork_source(client, "f-basic-src")

    r = _fork(client, "f-basic-src", "fork-1", "f-basic-dst", 3)
    assert r.status_code == 201
    body = r.json()
    assert body == {
        "source_stream_id": "f-basic-src",
        "fork_id": "fork-1",
        "target_stream_id": "f-basic-dst",
        "through_sequence": 3,
        "balance": 60,
        "next_sequence": 3,
        "replayed": False,
    }

    target = client.get("/streams/f-basic-dst").json()
    assert target == {
        "stream_id": "f-basic-dst",
        "balance": 60,
        "next_sequence": 3,
        "pending_sequences": [],
    }

    # The target continues appending at its own sequence 3; the source is untouched.
    r = _post(client, "f-basic-dst", "dst-e3", 3, 7)
    assert r.status_code == 201
    assert r.json()["stream"]["balance"] == 67
    assert r.json()["stream"]["next_sequence"] == 4

    src = client.get("/streams/f-basic-src").json()
    assert src["balance"] == 150
    assert src["next_sequence"] == 5
    assert src["pending_sequences"] == [6, 7]


def test_fork_at_zero_creates_empty_target(client):
    _seed_fork_source(client, "f-zero-src")

    r = _fork(client, "f-zero-src", "fork-0", "f-zero-dst", 0)
    assert r.status_code == 201
    body = r.json()
    assert body["balance"] == 0
    assert body["next_sequence"] == 0

    target = client.get("/streams/f-zero-dst").json()
    assert target["balance"] == 0
    assert target["next_sequence"] == 0
    assert target["pending_sequences"] == []


def test_fork_at_next_sequence_copies_whole_applied_prefix(client):
    _seed_fork_source(client, "f-full-src")

    r = _fork(client, "f-full-src", "fork-full", "f-full-dst", 5)
    assert r.status_code == 201
    body = r.json()
    assert body["balance"] == 150
    assert body["next_sequence"] == 5
    # Buffered events 6 and 7 are never copied.
    assert client.get("/streams/f-full-dst").json()["pending_sequences"] == []


def test_fork_unknown_source_returns_404(client):
    r = _fork(client, "f-missing", "fork-1", "f-missing-dst", 0)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"
    # Nothing half-created.
    assert client.get("/streams/f-missing-dst").status_code == 404


def test_fork_existing_target_returns_409(client):
    _seed_fork_source(client, "f-dest-exists-src")
    _post(client, "f-dest-exists-dst", "x", 0, 1)

    r = _fork(client, "f-dest-exists-src", "fork-a", "f-dest-exists-dst", 2)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "target_stream_exists"
    # The existing target is untouched; no fork record was written.
    assert client.get("/streams/f-dest-exists-dst").json()["next_sequence"] == 1


def test_fork_target_equal_to_source_returns_409(client):
    _seed_fork_source(client, "f-self-src")
    r = _fork(client, "f-self-src", "fork-self", "f-self-src", 2)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "target_stream_exists"


def test_fork_through_sequence_beyond_next_returns_409(client):
    _seed_fork_source(client, "f-range-src")

    r = _fork(client, "f-range-src", "fork-bad", "f-range-dst", 6)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "through_sequence_out_of_range"
    assert client.get("/streams/f-range-dst").status_code == 404


def test_fork_retry_same_parameters_is_idempotent(client):
    _seed_fork_source(client, "f-retry-src")

    first = _fork(client, "f-retry-src", "fork-r", "f-retry-dst", 4)
    assert first.status_code == 201
    assert first.json()["replayed"] is False

    # The target moves on meanwhile; the retry still reports the fork snapshot.
    _post(client, "f-retry-dst", "dst-e4", 4, 99)

    second = _fork(client, "f-retry-src", "fork-r", "f-retry-dst", 4)
    assert second.status_code == 200
    body = second.json()
    assert body["replayed"] is True
    assert body["through_sequence"] == 4
    assert body["next_sequence"] == 4
    assert body["balance"] == 100  # 10+20+30+40, not affected by the later +99

    # Exactly one copy of each event.
    claim = client.post(
        "/streams/f-retry-dst/consumers/w/claim", json={"max_events": 100}
    ).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1, 2, 3, 4]


def test_fork_retry_with_different_parameters_returns_409(client):
    _seed_fork_source(client, "f-conf-src")

    r = _fork(client, "f-conf-src", "fork-c", "f-conf-dst", 2)
    assert r.status_code == 201

    # Same fork_id, different through_sequence.
    r = _fork(client, "f-conf-src", "fork-c", "f-conf-dst", 3)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "fork_parameters_conflict"

    # Same fork_id, different target_stream_id.
    r = _fork(client, "f-conf-src", "fork-c", "f-conf-other-dst", 2)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "fork_parameters_conflict"
    # The conflicting alternative target was never created.
    assert client.get("/streams/f-conf-other-dst").status_code == 404


def test_same_target_from_two_forks_returns_409_on_second(client):
    _seed_fork_source(client, "f-shared-src")

    r = _fork(client, "f-shared-src", "fork-one", "f-shared-dst", 1)
    assert r.status_code == 201

    # Different fork_id but the same target stream name.
    r = _fork(client, "f-shared-src", "fork-two", "f-shared-dst", 2)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] in {
        "target_stream_exists",
        "integrity_conflict",
    }


def test_fork_validation_rejects_bad_payloads(client):
    _seed_fork_source(client, "f-val-src")

    base = {"fork_id": "f", "target_stream_id": "t", "through_sequence": 1}
    for patch in (
        {"fork_id": ""},
        {"target_stream_id": ""},
        {"through_sequence": -1},
        {"through_sequence": True},
    ):
        payload = {**base, **patch}
        assert client.post("/streams/f-val-src/forks", json=payload).status_code == 422
    # Missing required fields.
    assert client.post("/streams/f-val-src/forks", json={}).status_code == 422


def test_concurrent_forks_never_leave_half_built_state(client):
    import threading

    _seed_fork_source(client, "f-conc-src")
    statuses: list[int] = []
    barrier = threading.Barrier(20)

    def do_fork(i):
        barrier.wait()
        r = _fork(client, "f-conc-src", f"fork-{i}", f"f-conc-dst-{i}", 5)
        statuses.append(r.status_code)

    threads = [threading.Thread(target=do_fork, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # All 20 independent forks succeed exactly once.
    assert sorted(statuses) == [201] * 20
    for i in range(20):
        target = client.get(f"/streams/f-conc-dst-{i}").json()
        assert target["balance"] == 150
        assert target["next_sequence"] == 5

    # Concurrent retries of one fork_id all observe the same fork.
    retry_statuses: list[int] = []
    barrier2 = threading.Barrier(20)

    def retry():
        barrier2.wait()
        r = _fork(client, "f-conc-src", "fork-0", "f-conc-dst-0", 5)
        retry_statuses.append(r.status_code)

    threads = [threading.Thread(target=retry) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(retry_statuses) == [200] * 20


def test_concurrent_forks_sharing_target_serialise_to_one_success(client):
    import threading

    _seed_fork_source(client, "f-race-src")
    codes: list[str] = []
    barrier = threading.Barrier(20)

    def do_fork(i):
        barrier.wait()
        r = _fork(client, "f-race-src", f"fork-x-{i}", "f-race-dst", 5)
        if r.status_code == 201:
            codes.append("created")
        elif r.status_code == 409:
            codes.append(r.json()["detail"]["code"])
        else:
            codes.append(f"unexpected-{r.status_code}")

    threads = [threading.Thread(target=do_fork, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert codes.count("created") == 1
    assert len(codes) == 20
    # No half-built state: the target holds the full prefix exactly once.
    target = client.get("/streams/f-race-dst").json()
    assert target["balance"] == 150
    assert target["next_sequence"] == 5
