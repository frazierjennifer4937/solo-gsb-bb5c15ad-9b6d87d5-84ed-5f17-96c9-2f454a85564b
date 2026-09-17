# 乱序事件归并 API（Python · FastAPI · SQLite）

客户端按 `stream_id` 提交带有 `event_id`、`sequence`（从 0 开始递增）和整数 `delta`
的事件。服务端只把**从当前 `next_sequence` 起连续**的事件依次累加到余额；遇到缺号
时，后续事件先暂存。当缺失事件到达时，在**同一个数据库事务**内自动完成追赶
（catch-up）。

核心特性：

- **幂等重放**：相同 `event_id` 以相同内容重复提交返回 `200`，不会重复累加；
  内容不一致返回 `409 Conflict`。
- **序号唯一**：同一 `stream_id` 下每个 `sequence` 只能属于一个 `event_id`，
  重复序号（即使 `delta` 相同）返回 `409 Conflict`。
- **原子追赶**：缺失事件到达后，连续事件链的累加、`applied` 标记和
  `next_sequence`/`balance` 更新全部在同一事务内提交；冲突时整体回滚，已暂存
  事件不受影响。
- **并发安全**：SQLite WAL 模式 + `BEGIN IMMEDIATE`，并发提交串行化，每个序号
  恰好应用一次。
- **消费游标与租约**：消费者可领取（claim）从自身游标开始、已应用且连续的事件批，
  服务端发放带 `expires_at` 的 `lease_token`；租约未到期重复领取返回**同一批次、
  同一 token**，持 token 确认（ack）后游标原子推进；过期/旧 token 返回 `409`，
  租约到期后同批事件可被再次领取（at-least-once）。多个实例并发领取也只会存在
  一个有效租约。
- **流分叉（fork）**：把源流的已应用前缀 `[0, through_sequence)` 在**单个
  `BEGIN IMMEDIATE` 事务**内复制为一条可继续追加的新流；`fork_id` 是流内幂等键，
  参数相同的重试返回既有分叉（`200`），参数不同返回 `409`。
- 无外部账号、无外部依赖，`docker compose up --build` 一条命令启动。

## 一键启动

```bash
docker compose up --build
```

服务监听 **8000** 端口，数据保存在命名卷 `events-data`（容器内 `/data`）中，
容器重启不丢数据。

健康检查：

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>

> 也可不使用 Docker 本地运行：
> `pip install -r requirements.txt && uvicorn app.main:app --host 0.0.0.0 --port 8000`

## 接口

### 1. 提交事件

```
POST /streams/{stream_id}/events
Content-Type: application/json
```

请求体：

| 字段       | 类型   | 约束                  | 说明                       |
| ---------- | ------ | --------------------- | -------------------------- |
| `event_id` | string | 非空，流内唯一        | 客户端分配的幂等键         |
| `sequence` | int    | `>= 0`，流内唯一      | 从 0 开始的单调递增序号    |
| `delta`    | int    | 整数                  | 事件应用时累加到余额的值   |

响应：

- `201 Created`：事件首次写入；
- `200 OK`：幂等重放（`replayed: true`）；
- `409 Conflict`：同一 `event_id` 内容不一致（`replay_conflict`），或
  `sequence` 已被其他 `event_id` 占用（`sequence_conflict`）；
- `422 Unprocessable Entity`：请求体校验失败。

### 2. 查询流状态

```
GET /streams/{stream_id}
```

```json
{
  "stream_id": "acct-1",
  "balance": 250,
  "next_sequence": 2,
  "pending_sequences": [3, 5]
}
```

- `balance`：已连续应用事件的累加余额；
- `next_sequence`：下一个可应用的序号（即连续前缀的长度）；
- `pending_sequences`：已暂存、但因缺号尚未应用的序号（升序）。

流不存在时返回 `404`。

### 3. 消费者领取事件（claim）

```
POST /streams/{stream_id}/consumers/{consumer_id}/claim
Content-Type: application/json
```

请求体：

| 字段        | 类型 | 约束              | 说明                                   |
| ----------- | ---- | ----------------- | -------------------------------------- |
| `max_events` | int  | `1 <= max_events <= 10000` | 本批最多返回多少事件                   |

从该消费者**游标**开始，返回已应用（applied）且连续的事件，最多 `max_events`
条；同时生成一个带 `expires_at`（POSIX 毫秒）的租约 `lease_token`。

- 租约**未到期**时重复领取（`consumer_id` 相同）始终返回同一批次、同一 token、
  同一 `expires_at`，忽略新的 `max_events`；
- 没有可消费事件时返回空批次（`events: []`，`from_sequence == to_sequence`），
  但同样发放租约；
- 游标之后若有事件尚未应用（缺号），本批只包含连续已应用部分；
- 流不存在返回 `404`，参数非法返回 `422`。

```json
{
  "stream_id": "acct-1",
  "consumer_id": "worker-a",
  "lease_token": "dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi",
  "expires_at": 1789676192280,
  "from_sequence": 0,
  "to_sequence": 2,
  "events": [
    {"event_id": "e0", "sequence": 0, "delta": 100},
    {"event_id": "e1", "sequence": 1, "delta": 200}
  ]
}
```

### 4. 消费者确认批次（ack）

```
POST /streams/{stream_id}/consumers/{consumer_id}/ack
Content-Type: application/json
```

请求体：`{"lease_token": "<claim 返回的 token>"}`。

只有 token **匹配当前未过期租约**时，才在同一个事务内把游标原子推进到
`to_sequence` 并清除租约：

```json
{"status": "ok", "stream_id": "acct-1", "consumer_id": "worker-a", "cursor": 2}
```

- `409 Conflict` 的情形：
  - `lease_not_active`：该消费者当前没有有效租约（尚未 claim，或已 ack）；
  - `lease_token_mismatch`：token 与当前租约不符（旧租约的 token、伪造 token）；
  - `lease_expired`：token 匹配的租约已过期。
- 租约到期后 token 失效（ack 返回 `409`），同批事件可被再次领取，新领取会得到
  新 token；
- 流不存在返回 `404`，请求体缺少 `lease_token` 返回 `422`。

不同 `consumer_id` 的游标与租约彼此独立；游标和租约都持久化在 SQLite 中。

### 5. 分叉流（fork）

```
POST /streams/{stream_id}/forks
Content-Type: application/json
```

请求体：

| 字段               | 类型   | 约束                         | 说明                                   |
| ------------------ | ------ | ---------------------------- | -------------------------------------- |
| `fork_id`          | string | 非空，源流内唯一             | 客户端分配的幂等键                     |
| `target_stream_id` | string | 非空                         | 分叉产生的新流，必须尚不存在           |
| `through_sequence` | int    | `>= 0` 且 `<= 源流 next_sequence` | 独占上界，只复制序号 `< through_sequence` 的**已应用**事件 |

在**单个 `BEGIN IMMEDIATE` 事务**内写入分叉记录、目标流和复制的事件：
只复制源流中 `applied=1`、序号位于 `[0, through_sequence)` 的事件（缺号暂存的
事件不会被复制）。目标流以复制出的前缀为起点，可像普通流一样继续提交新事件
（从 `through_sequence` 开始）。

- `201 Created`：分叉首次创建；
- `200 OK`：同一源流 + 同一 `fork_id` 以**完全相同的参数**重试（`replayed: true`），
  返回既有分叉；即使目标流此后又追加了事件，响应中的 `balance`/`next_sequence`
  仍是分叉时刻的快照值；
- `409 Conflict`：
  - `fork_parameters_conflict`：同一 `fork_id` 已存在，但 `target_stream_id`
    或 `through_sequence` 不同；
  - `through_sequence_out_of_range`：上界超出 `0..源流 next_sequence`；
  - `target_stream_exists`：目标流已存在（`target_stream_id` 与源流相同也属于此情形）；
- `404 Not Found`：源流不存在（`stream_not_found`）；
- `422 Unprocessable Entity`：请求体校验失败。

任何校验失败都会在写入前回滚，并发请求不会产生只有目标流、没有事件之类的半成品；
多个请求同时抢占同一个目标流名时恰好一个成功，其余返回 `409`。

```json
{
  "source_stream_id": "acct-1",
  "fork_id": "fork-2026-01",
  "target_stream_id": "acct-1-snapshot",
  "through_sequence": 2,
  "balance": 300,
  "next_sequence": 2,
  "replayed": false
}
```

- `balance`：目标流的初始余额（复制前缀的 delta 之和）；
- `next_sequence`：目标流的初始下一个序号（等于 `through_sequence`）。

## 工作机制示例

```bash
# 序号 1 先到：缺 0，暂存，余额不变
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","sequence":1,"delta":200}'
# 201 -> balance=0, next_sequence=0, pending=[1]

# 序号 2 再到：仍然暂存
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e2","sequence":2,"delta":50}'
# 201 -> balance=0, next_sequence=0, pending=[1,2]

# 序号 0 到达：同一事务内自动追赶 0 -> 1 -> 2
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# 201 -> balance=350, next_sequence=3, pending=[]

# 相同 event_id 重放：幂等，返回 200，余额不变
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# 200 -> replayed=true, balance=350, next_sequence=3

# 同一 event_id 内容不一致：409
curl -i -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":999}'
# HTTP/1.1 409
# {"detail":{"code":"replay_conflict","message":"event_id already exists with different sequence/delta"}}

# 序号 0 被另一个 event_id 占用：409
curl -i -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"other","sequence":0,"delta":100}'
# HTTP/1.1 409, code=sequence_conflict

# 查询
curl -s localhost:8000/streams/acct-1
```

## 消费游标示例（claim / ack）

```bash
# 先准备三个已应用事件（接上文，acct-1 的 next_sequence=3）
# 消费者 worker-a 领取最多 2 个事件
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' \
  -d '{"max_events":2}'
# {
#   "stream_id":"acct-1","consumer_id":"worker-a",
#   "lease_token":"dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi",
#   "expires_at":1789676192280,"from_sequence":0,"to_sequence":2,
#   "events":[{"event_id":"e0","sequence":0,"delta":100},
#             {"event_id":"e1","sequence":1,"delta":200}]
# }

# 租约未到期重复领取：同一批次、同一 token、同一 expires_at
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' -d '{"max_events":99}'
# -> lease_token 不变，events 仍是 [e0, e1]

# 用错误/旧 token 确认：409
curl -i -X POST localhost:8000/streams/acct-1/consumers/worker-a/ack \
  -H 'Content-Type: application/json' -d '{"lease_token":"bogus"}'
# HTTP/1.1 409 -> {"detail":{"code":"lease_token_mismatch", ...}}

# 用有效 token 确认：游标原子推进到 2
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/ack \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi"}'
# {"status":"ok","stream_id":"acct-1","consumer_id":"worker-a","cursor":2}

# 再次领取：从游标 2 继续
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}'
# -> events=[e2]，from_sequence=2, to_sequence=3

# 租约到期后再用旧 token 确认：409 lease_expired；
# 重新 claim 可再次拿到同批事件（新 token）。
```

## 流分叉示例（fork）

```bash
# 源流 acct-1 已有 3 个已应用事件（balance=350, next_sequence=3），
# 另有缺号暂存事件不会被复制。
# 把 [0, 2) 的已应用事件复制为新流 acct-1-snapshot
curl -s -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"fork-2026-01","target_stream_id":"acct-1-snapshot","through_sequence":2}'
# 201 ->
# {"source_stream_id":"acct-1","fork_id":"fork-2026-01",
#  "target_stream_id":"acct-1-snapshot","through_sequence":2,
#  "balance":300,"next_sequence":2,"replayed":false}

# 查看目标流，然后像普通流一样从序号 2 继续追加
curl -s localhost:8000/streams/acct-1-snapshot
# {"stream_id":"acct-1-snapshot","balance":300,"next_sequence":2,"pending_sequences":[]}
curl -s -X POST localhost:8000/streams/acct-1-snapshot/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"snap-e2","sequence":2,"delta":5}'
# 201 -> balance=305, next_sequence=3

# 参数相同的重试：幂等，200 + replayed=true，返回分叉时刻快照
curl -s -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"fork-2026-01","target_stream_id":"acct-1-snapshot","through_sequence":2}'
# 200 -> replayed=true, balance=300, next_sequence=2（不受目标流后续追加影响）

# 同一 fork_id 但参数不同：409 fork_parameters_conflict
curl -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"fork-2026-01","target_stream_id":"acct-1-snapshot","through_sequence":3}'
# HTTP/1.1 409

# 上界超出源流 next_sequence：409 through_sequence_out_of_range
curl -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"fork-late","target_stream_id":"acct-1-late","through_sequence":99}'
# HTTP/1.1 409

# 目标流已存在：409 target_stream_exists；源流不存在：404
curl -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"fork-dup","target_stream_id":"acct-1-snapshot","through_sequence":1}'
# HTTP/1.1 409
```

## 数据模型

```text
streams(stream_id PK, balance, next_sequence)
events(stream_id, event_id, sequence, delta, applied,
       PK(stream_id, event_id), UNIQUE(stream_id, sequence))
consumers(stream_id, consumer_id, cursor_seq,
          lease_token, lease_expires_at, lease_from_seq, lease_to_seq,
          PK(stream_id, consumer_id))
forks(source_stream_id, fork_id, target_stream_id, through_sequence, created_at,
      PK(source_stream_id, fork_id), UNIQUE(target_stream_id))
```

- `UNIQUE(stream_id, event_id)` 保证重放幂等；
- `UNIQUE(stream_id, sequence)` 保证序号唯一；
- `applied=0` 表示暂存，`applied=1` 表示已计入余额；
- 每个 `(stream_id, consumer_id)` 一行：`cursor_seq` 为消费游标（下一个待领取
  的序号），租约四列要么整体为 NULL（无有效租约），要么整体非空。一行只持有一个
  租约，因此领取与确认都在 `BEGIN IMMEDIATE` 事务内串行执行——即使多个应用实例
  同时领取，数据库中也只会存在一个有效租约。

租约生命周期：

1. claim 时若存在未到期租约（`lease_expires_at > now`），直接返回该租约覆盖的
   原有批次（`lease_from_seq..lease_to_seq`）、原 token、原到期时间；
2. 否则从 `cursor_seq` 起取连续已应用事件（序号上界为流的 `next_sequence`），
   至多 `max_events` 条，生成新 token（`secrets.token_urlsafe`）和
   `expires_at = now + LEASE_DURATION_SECONDS`，写入租约四列并提交；
3. ack 在单个 `BEGIN IMMEDIATE` 事务内校验：存在流（否则 `404`）→ 存在租约
   （否则 `lease_not_active`）→ token 一致（否则 `lease_token_mismatch`）→
   未过期（否则 `lease_expired`）；全部通过后把游标推进到 `lease_to_seq` 并清空
   租约列，原子提交；
4. 租约到期不会自动删除：旧 token 无法 ack（`409`），下一次 claim 会发放新
   token，同一批事件可被重新消费。

提交事件的处理在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 若流不存在则创建（`balance=0, next_sequence=0`）；
2. 若 `event_id` 已存在：比对 `sequence`/`delta`，不一致即 `409`，一致则直接
   返回当前状态（幂等）；
3. 否则检查 `sequence` 是否已被占用，占用则 `409`，未占用则写入
   `applied=0` 的事件；
4. 从 `next_sequence` 开始循环取 `applied=0` 的连续事件累加 `delta`、标记
   `applied=1`，直到遇到缺号；
5. 更新 `balance` 与 `next_sequence`，提交事务。

分叉（fork）在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 读取源流（不存在即 `404`）；
2. 查 `(source_stream_id, fork_id)`：已存在则比对 `target_stream_id` 与
   `through_sequence`，任一不同即 `409 fork_parameters_conflict`，完全一致则
   返回既有分叉（`200`，幂等重试）；
3. 校验 `0 <= through_sequence <= 源流 next_sequence`，越界即
   `409 through_sequence_out_of_range`；
4. 检查目标流不存在（已存在即 `409 target_stream_exists`；`forks.target_stream_id`
   上还有 `UNIQUE` 约束兜底并发抢占）；
5. 计算前缀余额（已应用且 `sequence < through_sequence` 的 `delta` 之和），
   依次插入目标流、分叉记录，并用一条
   `INSERT ... SELECT ... WHERE applied = 1 AND sequence < through_sequence`
   复制事件（复制出的事件直接 `applied=1`），整体提交；任一步失败整体回滚，
   因此并发请求不会留下半成品。

## 配置

| 环境变量                | 默认值                | 说明                          |
| ----------------------- | --------------------- | ----------------------------- |
| `APP_PORT`              | `8000`                | 服务监听端口                  |
| `DATABASE_PATH`         | `/data/events.db`     | SQLite 文件路径               |
| `LEASE_DURATION_SECONDS` | `30`                 | claim 租约有效时长（秒，> 0） |

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

`tests/test_api.py` 覆盖：顺序累加、缺号暂存与自动追赶、幂等重放、两种 409、
404、422 参数校验、大批量追赶以及 20 线程并发；消费者游标部分覆盖：领取连续批次、
缺号时只返回已应用事件、租约内重复领取同批同 token、ack 推进游标、错误/旧/过期
token 的三种 409、租约到期后同批重新领取、多消费者独立游标、空批次以及 20 线程
并发只产生一个有效租约；分叉部分覆盖：前缀复制与暂存事件排除、上界 0 与
`next_sequence` 边界、目标流可继续追加、源流不受影响、404/三种 409、幂等重试
（含目标流已追加后的快照语义）、参数冲突不产生多余目标流、422 校验，以及
20 线程并发独立分叉全部成功、重试全部命中既有分叉、共享目标流名恰好一个成功。

仅用 Python 标准库验证核心归并算法（无需安装 FastAPI）：

```bash
python3 verify_core.py
```

## 项目结构

```
.
├── app/
│   ├── __init__.py
│   ├── main.py        # FastAPI 路由与请求/响应模型
│   ├── repo.py        # 归并/追赶算法（纯 sqlite3，事务边界清晰）
│   ├── consumers.py   # 消费游标与 claim/ack 租约
│   ├── db.py          # 连接管理、WAL、schema 初始化
│   └── config.py      # 环境变量配置
├── tests/test_api.py
├── verify_core.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```
