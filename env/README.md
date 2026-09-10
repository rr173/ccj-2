# 事件外发系统

这是一个接收事件、按事件类型分发给订阅地址，把事件按顺序推送到外部 Webhook，并对推送结果做**回执对账**的系统：

- **ingest-api**：登记接收地址（含订阅的事件类型）、接收事件（可约定最早外发时间 `not_before`）、**取消/改期尚未打出的事件**、**接入回执**、查询事件投递轨迹与整笔/逐地址对账情况、人工恢复隔离地址、把超时或失败回执导致未对上的副本重投（支持只重投某一笔事件中尚未认的那些副本）。
- **worker**：负责真正的 HTTP 投递、重试、熔断隔离、崩溃恢复。
- **reconciler**：独立的对账进程，周期性把超过约定时间仍未收到回执的副本标记为 `timed_out`（可查，不算认）。
- **PostgreSQL**：作为任务队列和事实来源，用行锁和每地址单调序号保证同一个接收地址严格 FIFO。

前三个服务使用同一个镜像但启动命令不同，可以独立水平扩缩：回执接入（在 ingest-api 里）和对账（reconciler）都不在外发 worker 进程内，两边互不影响。

## 关键语义

### 1. 按事件类型分发（发布/订阅）

- 登记地址时用 `event_types` 声明它关心哪些事件类型；不传或传 `null` 表示保持现状（新地址则为不订任何类型），传列表（含空列表）则整体替换订阅集合。
- 提交事件时带 `event_type`，不再需要指定地址。入库的那一刻按当前订阅快照**扇出**：每个订了该类型的地址各得到一份独立的投递副本（delivery），各排各的队。
- **没人订的类型**：事件照常接收并持久化，状态为 `unrouted`，可以通过轨迹接口查到它一条都没投出去——不会写成成功。之后有地址补订该类型，也只从**下一条**新事件开始接收，不会把已入库的旧事件补投给它。
- 同一地址的重复登记是幂等的；重复提交同一 `dedupe_key` 的事件返回原事件并带 `duplicate: true`，不会重复扇出。

### 2. 同一接收地址严格按进入顺序投递

扇出时，每个地址的副本在该地址行锁内分配 `(destination_id, destination_seq)` 单调递增序号。Worker 每次只选择某地址当前最小的待投递副本，并用 `FOR UPDATE SKIP LOCKED` 锁定该地址：

- 同一个地址同一时刻只会被一个 Worker 线程处理；同一事件的不同地址副本互不等待。
- Worker 会先检查地址队头：队头投递中、未到下次重试时间、**未到约定外发时间（`not_before`）**或地址隔离时，后续副本不会越过它。
- Worker 崩溃时，队头会在租约超时后重新变为待投递，然后继续按序处理。
- 不同接收地址之间互不阻塞，可以并行投递；一个地址被隔离不影响订了同一类型的其他地址。

### 3. 定时外发、取消与改期

提交事件时可以带 `not_before`（ISO 8601 时间，无时区按 UTC 理解），约定这条事件**最早什么时候才准往外打**：

- **没到点不发出**：扇出时 `not_before` 写到每份副本上，Worker 只领取 `not_before` 已过的副本。没到点的副本就排在它在该地址队列里原来的位置（序号在提交那一刻已分配）等到点，**不会提前打，也不会被后面提交的副本越过**；到点后按该地址原有顺序投递，也不会插到正在投的副本前面。
- **没到点可以不要**：`POST /v1/events/{event_id}/cancel`。只要还没有任何一份副本打出去（含正在投），取消就成功：所有未投副本置为终态 `cancelled`，**之后任何时候都不会再打出去**，事件状态变为 `cancelled`。重复取消是幂等的。只要已有一份副本投妥或正在投，取消返回 `409`——打出去的收不回来。
- **没打出去前可以改时间**：`POST /v1/events/{event_id}/reschedule`，body 为 `{"not_before": "..."}`（传 `null` 表示取消定时、轮到就投）。改期只动未投副本的时间门槛，**每份副本在该地址队列里的位置不变**。同样地，只要有一份副本已投妥或正在投，改期返回 `409`——**已经打出去的不能改时间**；已取消的事件也不能再改期。
- **对账从真正打出去之后才开始算**：`reconcile_deadline` 是在投妥那一刻才写入的（投妥时间 + `RECEIPT_TIMEOUT_SECONDS`）。提交时间、定时等待的时间都不计入倒计时；没到点、未投妥、已取消的副本 `reconcile_state` 一直是 `none`，reconciler 不会扫它们。
- **没人订的类型照收**：带 `not_before` 的无人订阅事件同样正常接收、持久化，状态 `unrouted`，不会被当成已发出；也可以对它取消或改期（只改事件记录，本来就没有副本）。

### 4. 重试与隔离

- 非 2xx 响应、连接失败、超时等都算投递失败。
- 使用指数退避并加入随机抖动：约为 `2s, 4s, 8s, ...`，最大 1 小时。
- 同一地址连续失败达到阈值（默认 5 次）后标记为 `isolated`。
- 隔离时间默认 15 分钟。隔离期间该地址的新副本继续入库排队，不影响其他地址。
- 隔离时间结束后，Worker 会自动将地址恢复为 `active`，并从尚未成功的最小序号副本继续投递。
- 也可以调用管理 API 立即人工恢复。

### 5. 去重与“接收方只处理一次”

事件必须带 `dedupe_key`，全局唯一：

- 重复提交不会生成新事件，接口返回原事件并带 `duplicate: true`。
- Worker 投递时会发送请求头：
  - `Idempotency-Key: <dedupe_key>`
  - `X-Event-Id: <event_id>`
  - `X-Event-Type: <event_type>`
  - `X-Delivery-Id: <delivery_id>`
- 投递采用至少一次语义：Worker 崩溃或网络超时时可能重复发送。接收方必须使用 `Idempotency-Key` 做幂等处理，从而做到业务效果只处理一次。

### 6. Worker 崩溃恢复（租约 + 心跳）

副本被领取后进入 `in_flight`，记录 `claim_token`、`claimed_at` 和 `lease_until`。投递期间，Worker 会用后台心跳线程按 `LEASE_HEARTBEAT_SECONDS`（默认 15 秒）把 `lease_until` 向后延长一个租约窗口（默认 60 秒）：

- **Worker 活着、只是接收方响应慢**：心跳持续续约，租约不过期，同一条副本不会被其他 Worker 重复领取或重复外发；该地址后续副本继续按 FIFO 被它挡在后面，直到这次调用结束。
- **Worker 进程真的挂掉或被 kill**：心跳随之停止，`lease_until` 在一个租约窗口后过期。其他 Worker 的 reaper 会把副本重新置为 `pending` 并从最小序号继续投递，不会丢任务。
- **极端情况下租约仍过期并被接管**（例如数据库长时间不可用导致心跳无法续约）：旧 Worker 那次 HTTP 调用结束后，不会再修改已被新 Worker 接管的副本，只在 `delivery_attempts` 中追加一条 `lost_lease=true` 的审计记录，因此这些“多打的一次”在投递轨迹里可见。

这样“慢响应”和“真宕机”被区分开：慢不会导致重复外发，宕机仍能从未成功处续投。

### 7. 回执对账

HTTP 投递拿到 2xx 只说明“打出去了”，不代表对方处理完了。对账语义：

- 副本投妥后进入 `awaiting` 对账状态，并写入约定对账时限 `reconcile_deadline`（**投妥时间** + `RECEIPT_TIMEOUT_SECONDS`，默认 300 秒）。对账倒计时从真正打出去那一刻才开始算：事件提交时间、`not_before` 定时等待的时间都不计入；还在排队等定时、等待重试或已取消的副本 `reconcile_state` 为 `none`，没有任何倒计时。
- 接收方处理完后回调 `POST /v1/receipts`，带上它拿到的 `destination_id`、`dedupe_key` 和处理结果 `result`（`success` / `failure`）。系统按 `(destination_id, dedupe_key)` 定位唯一副本：
  - 在时限内匹配上 → 副本记为 `acknowledged`（result=success）或 `receipt_failed`（result=failure），这才算**对方认了**；
  - 时限内没等到 → reconciler 把副本标记为 `timed_out`。**超时不是认**，可以通过对账查询接口全部查出来；
  - 过了时限才到的回执记为 `late`：写入回执日志可查询，但**不会**把已经超时的副本改回认了；
  - 同一条回执重复送来只认一次：副本已被对账过的后续回执记为 `duplicate`，幂等返回，不重复生效；
  - 查无此副本的回执记为 `orphan`，副本还在传输中就提前到的回执记为 `premature`，都留在回执日志里可查。
- 回执接入（ingest-api 的接口）和对账扫描（reconciler 进程）都不在外发 worker 进程里，可以各自独立扩缩。
- 回执结果（包括失败回执）不影响地址的失败计数与隔离状态——隔离只看传输层投递结果。

#### 整笔事件的对账状态

一条事件扇出给多个地址时，每个地址的副本独立对账；事件响应和轨迹里的 `reconcile_status` 再汇总整笔状态：

- `pending`：还没有任何一份副本对上成功回执（包括仍在传输、等待回执、已超时或收到失败回执）；
- `partially_acknowledged`：至少一份副本已经对上，但不是所有订阅地址都对上；轨迹中可逐个查看谁是 `acknowledged`、谁还在 `awaiting`、谁是 `timed_out` / `receipt_failed`；
- `acknowledged`：事件扇出的**每一份**副本都在时限内收到成功回执，这时整笔才算认完。
- `acknowledged_count / unacknowledged_count` 分别给出已认和未认份数；`delivery_count = 0` 的无人订阅事件仍是明确的 `unrouted`，不会被汇总成已认。

`status` 仍只表示传输层状态（`pending` / `delivered` / `unrouted`），不能把“全部收到 2xx”当成整笔已认；整笔是否认完只看 `reconcile_status`。

#### 超时/失败副本的重投

`timed_out` 或 `receipt_failed` 的副本可以丢回原地址重投：

- `POST /v1/deliveries/{delivery_id}/requeue`：重投单个副本；
- `POST /v1/events/{event_id}/requeue-unreconciled`：只把这一笔事件中已经终态未对上的副本（`timed_out` / `receipt_failed`）按原地址批量重投；
- `POST /v1/destinations/{destination_id}/requeue-unreconciled`：把该地址所有未对上的副本一次性重投。

事件级重投只选择未认副本：已经 `acknowledged` 的副本不会再打，仍在传输或仍处于本轮等待回执窗口的副本也不会被提前重打。重投**保留原来的 `destination_seq`**，每份副本回到自己原地址队列中原来的位置，按原顺序接着排；各地址仍互不等待。同时 Worker 的领取逻辑保证：只要某地址还有副本在投（`in_flight`），重投回来的副本就不会被领取——不会插到还在投的副本前面。重投成功后该副本重新进入 `awaiting`，等待新一轮回执。

#### 对账查询

- `GET /v1/reconciliations/summary`：各对账状态的副本数量；
- `GET /v1/reconciliations/deliveries?reconcile_state=timed_out&destination_id=...`：列出指定状态（默认 `timed_out`）的副本明细；
- `GET /v1/receipts?disposition=late&destination_id=...`：回执日志，可按处置结果（`applied`/`duplicate`/`late`/`orphan`/`premature`）过滤；
- `GET /v1/events/{event_id}/trace`：展示整笔 `reconcile_status`、已认/未认数量；每条副本附带 `reconcile_state`、对账时限、回执结果，以及该事件命中的全部回执记录，可直接查出哪些地址已认、哪些还没认。

## 快速启动

```bash
docker compose up --build
```

服务：

- API：`http://localhost:8000`
- OpenAPI：`http://localhost:8000/docs`
- PostgreSQL：仅 Compose 内部暴露给几个服务

独立扩缩示例：

```bash
# 扩 Worker；每个实例默认 4 个投递线程
WORKER_CONCURRENCY=8 docker compose up --build --scale worker=3

# 回执接入随 ingest-api 扩，对账扫描单独扩
docker compose up --build --scale reconciler=2
```

API 也可以独立增加副本，但 Compose 文件中默认将 API 的 8000 端口固定发布到宿主机。多副本时建议改为反向代理/负载均衡器访问服务发现名 `ingest-api:8000`，不要直接对多副本绑定同一个宿主机端口。

## API 示例

### 登记接收地址并声明订阅的事件类型

```bash
curl -s http://localhost:8000/v1/destinations \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/webhook","event_types":["paid","refunded"]}'
```

返回示例：

```json
{
  "id": "6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c",
  "url": "https://example.com/webhook",
  "status": "active",
  "failure_count": 0,
  "event_types": ["paid", "refunded"],
  "recoverable_at": null,
  "created_at": "2026-09-09T00:00:00Z"
}
```

重复登记同一 URL 是幂等的，返回同一个地址。再次登记时：

- 传 `event_types` 列表 → 整体替换订阅集合（传 `[]` 表示退订全部）；
- 不传 `event_types` → 保持现有订阅不变。

### 提交事件

```bash
curl -s http://localhost:8000/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "paid",
    "dedupe_key": "order-1001-paid",
    "payload": {
      "order_id": "1001",
      "event_type": "paid"
    }
  }'
```

约定最早外发时间（可选；没到点不会打出去，到点后按各地址原队列顺序投）：

```bash
curl -s http://localhost:8000/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "paid",
    "dedupe_key": "order-1002-paid",
    "not_before": "2026-09-10T08:00:00Z",
    "payload": {
      "order_id": "1002",
      "event_type": "paid"
    }
  }'
```

响应中包含：

- `id`：事件 ID
- `event_type` / `dedupe_key` / `payload`：事件本体
- `not_before`：约定的最早外发时间（未定时为 `null`）；`cancelled_at`：取消时间（未取消为 `null`）
- `status`：`unrouted`（没有地址订该类型）、`pending`（至少一份副本未投完）、`delivered`（全部副本已收到传输层 2xx，但不一定已对上回执）、`cancelled`（未打出前已取消，剩余副本永不再投）
- `reconcile_status`：`pending`（还没有副本认）、`partially_acknowledged`（只认了一部分）、`acknowledged`（所有订阅地址的副本都认了）
- `delivery_count` / `delivered_count`：扇出副本总数 / 已投妥数
- `acknowledged_count` / `unacknowledged_count`：已对上成功回执的份数 / 还没对上的份数
- `duplicate`：是否命中去重并返回已有事件

### 取消与改期（仅在任何副本打出之前可用）

```bash
# 没到点不想要了：所有未投副本置为终态 cancelled，永不再投；重复调用幂等
curl -s -X POST http://localhost:8000/v1/events/<event_id>/cancel

# 改时间：只动未投副本的时间门槛，队列位置不变；传 null 表示取消定时、轮到就投
curl -s -X POST http://localhost:8000/v1/events/<event_id>/reschedule \
  -H 'Content-Type: application/json' \
  -d '{"not_before": "2026-09-10T10:00:00Z"}'
```

只要已有一份副本投妥或正在投，这两个接口都返回 `409`——已经打出去的不能取消、也不能改时间；已取消的事件再改期同样返回 `409`。

### 查询某条事件的投递过程

```bash
curl -s http://localhost:8000/v1/events/<event_id>/trace
```

返回事件当前传输状态和整笔对账状态（`reconcile_status`，以及已认/未认数量）、每个地址的副本（`deliveries`：状态、序号、已尝试次数、下次尝试时间、最近错误、对账状态 `reconcile_state`、对账时限 `reconcile_deadline`、回执结果 `receipt_result`）、全部投递尝试（`attempts`：开始/结束时间、是否成功、HTTP 状态码、响应片段或错误信息；若某次调用是在租约丢失后返回的，会带 `lost_lease: true`）和该事件命中的回执（`receipts`）。部分地址已认时，整笔是 `partially_acknowledged`，不会写成已认完；逐个查看副本即可知道谁认了、谁还没认。没人订的事件在这里能看到 `status: "unrouted"` 且 `deliveries` 为空——是明确的“没送出去”，不是成功。

### 回执接入（接收方回调）

接收方处理完一条副本后，在约定的对账时限内回调：

```bash
curl -s http://localhost:8000/v1/receipts \
  -H 'Content-Type: application/json' \
  -d '{
    "destination_id": "<登记地址时返回的 id>",
    "dedupe_key": "order-1001-paid",
    "result": "success"
  }'
```

响应里的 `disposition` 说明这条回执被如何处置：

- `applied`：在时限内匹配到待对账副本，已按 `result` 记为 `acknowledged` / `receipt_failed`；
- `duplicate`：同一条回执重复送来，只认一次，幂等返回；
- `late`：过了对账时限才到，只记录、不把已超时的副本改成认了；
- `orphan`：按 `(destination_id, dedupe_key)` 查无此副本；
- `premature`：副本还在传输中（回执比投递结果先到了）。

无论哪种处置，回执都会落进回执日志，可用 `GET /v1/receipts` 查询。

### 对账查询与重投

```bash
# 各对账状态的副本数量
curl -s http://localhost:8000/v1/reconciliations/summary

# 约定时间内没对上的副本（默认 reconcile_state=timed_out）
curl -s 'http://localhost:8000/v1/reconciliations/deliveries?reconcile_state=timed_out'

# 迟到的回执
curl -s 'http://localhost:8000/v1/receipts?disposition=late'

# 重投单个超时副本（保留原 destination_seq，回到原地址队列原位置）
curl -s -X POST http://localhost:8000/v1/deliveries/<delivery_id>/requeue

# 只把这一笔事件里已超时或收到失败回执、还没认的副本按原地址/原顺序重投；
# 已 acknowledged 的副本不会重打
curl -s -X POST http://localhost:8000/v1/events/<event_id>/requeue-unreconciled

# 把某地址所有没对上的副本一次性重投
curl -s -X POST http://localhost:8000/v1/destinations/<destination_id>/requeue-unreconciled
```

### 立即恢复隔离地址

```bash
curl -s -X POST http://localhost:8000/v1/destinations/<destination_id>/recover
```

该操作：

1. 将地址从 `isolated` 改回 `active`；
2. 清空失败计数和自动恢复时间；
3. 将该地址所有未成功副本的下次尝试时间重置为当前时间；
4. Worker 随后从最小序号的未成功副本继续投递。

对已经是 `active` 的地址调用是幂等的，返回中 `recovered` 为 `false`。

## 投递给接收方的请求格式

Worker 发送：

```http
POST /webhook HTTP/1.1
Content-Type: application/json
Idempotency-Key: order-1001-paid
X-Event-Id: 6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c
X-Event-Type: paid
X-Delivery-Id: 9c2f0a4e-7b1d-4e55-9a3c-2f8d6c1a0b22

{
  "event_id": "6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c",
  "delivery_id": "9c2f0a4e-7b1d-4e55-9a3c-2f8d6c1a0b22",
  "event_type": "paid",
  "destination_id": "1c4f8a2b-....",
  "destination_seq": 12,
  "dedupe_key": "order-1001-paid",
  "payload": {
    "order_id": "1001",
    "event_type": "paid"
  }
}
```

接收方只有返回 2xx 才算**传输成功**。请在接收方用 `Idempotency-Key` 做幂等表或唯一约束。

传输成功不等于对方认了：接收方处理完业务后，还需要在约定时限（`RECEIPT_TIMEOUT_SECONDS`，默认 300 秒）内回调回执接口：

```http
POST /v1/receipts HTTP/1.1
Content-Type: application/json

{
  "destination_id": "1c4f8a2b-....",
  "dedupe_key": "order-1001-paid",
  "result": "success"
}
```

- `destination_id` 和 `dedupe_key` 直接取投递请求体里的同名字段（`dedupe_key` 也即 `Idempotency-Key` 头）；`result` 为 `success` 或 `failure`。
- 回执按 `(destination_id, dedupe_key)` 对账：时限内对上才算认；重复回执只认一次；超时后到的回执只记为迟到，不会把副本改回认了。
- 回执接口可以安全重试：接收方收不到回执响应时直接重发同一条回执即可，服务端幂等。

## 本地手工验证

可以启动一个会强制失败数次的接收方，观察重试、顺序和幂等：

```bash
python3 scripts/mock_receiver.py --port 9000 --fail-times 3
```

让它同时自动回执（处理成功后回调回执接口）：

```bash
python3 scripts/mock_receiver.py --port 9000 \
  --receipt-url http://localhost:8000/v1/receipts
# 观察迟到回执：把回执延迟到对账时限之后
# RECEIPT_TIMEOUT_SECONDS=20 python3 ... (服务端) + --receipt-delay 30 (接收方)
```

在 Linux + Docker Desktop/bridge 网络下，容器内通常可用 `http://172.17.0.1:9000/` 访问宿主机进程；不同环境请替换为实际可达地址。注册该地址（记得带上 `event_types`）后连续提交多条对应类型的事件，再用 `/v1/events/{id}/trace` 查看每份副本的每次尝试与对账状态；用 `/v1/reconciliations/summary`、`/v1/reconciliations/deliveries?reconcile_state=timed_out` 和 `/v1/receipts?disposition=late` 观察对账结果。

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `DATABASE_URL` | Compose 中已设置 | PostgreSQL SQLAlchemy/psycopg 连接串 |
| `DB_POOL_SIZE` | `10`，Worker 建议 `4` | SQLAlchemy 常驻连接数 |
| `DB_MAX_OVERFLOW` | `5`，Worker 建议 `2` | 连接池溢出连接数 |
| `WORKER_CONCURRENCY` | `4` | 单个 Worker 实例内的投递线程数 |
| `POLL_INTERVAL_SECONDS` | `0.5` | 无可投递任务时的轮询间隔 |
| `HTTP_TIMEOUT_SECONDS` | `10` | 单次 HTTP 请求超时 |
| `CLAIM_LEASE_SECONDS` | `60` | Worker 崩溃后，`in_flight` 副本多久可被其他 Worker 接管；存活 Worker 会用心跳续约 |
| `LEASE_HEARTBEAT_SECONDS` | `15` | 投递期间续约租约的心跳间隔（实际取该值与租约的 1/3 的较小值） |
| `RETRY_BACKOFF_BASE_SECONDS` | `2` | 初始退避时间 |
| `RETRY_BACKOFF_MAX_SECONDS` | `3600` | 最大退避时间 |
| `FAILURE_THRESHOLD` | `5` | 连续失败多少次后隔离地址 |
| `QUARANTINE_SECONDS` | `900` | 自动隔离时长 |
| `MAX_RESPONSE_BODY_BYTES` | `2048` | 轨迹表保存响应体片段的最大长度 |
| `RECEIPT_TIMEOUT_SECONDS` | `300` | 回执对账时限：副本投妥后等待回执的约定时间，超时记为 `timed_out`（Worker 侧配置） |
| `RECONCILE_SWEEP_INTERVAL_SECONDS` | `5` | reconciler 扫描超时副本的间隔 |
| `RECEIPT_DELIVERY_GRACE_SECONDS` | `2` | 回执比投递结果先到时，回执接口等待 Worker 落库投递结果的宽限（API 侧配置） |

## 数据表概览

- `events`：事件本体（类型、去重键、负载、最早外发时间 `not_before`、取消时间 `cancelled_at`），一条事件一行，与地址无关。
- `destination_subscriptions`：地址订阅的事件类型集合。
- `deliveries`：扇出后的每地址投递副本，含每地址顺序号、投递状态（`pending` / `in_flight` / `delivered` / `cancelled`）、下次尝试时间、最早外发时间 `not_before`、租约信息和对账状态（`reconcile_state`、对账时限、回执结果、重投次数）；Worker 只消费这张表。
- `delivery_attempts`：每次 HTTP 投递尝试的审计轨迹（关联事件与副本）。
- `receipts`：接收方回执日志，一条回执一行，含处置结果（`applied`/`duplicate`/`late`/`orphan`/`premature`）与匹配到的副本；重复、迟到、查无副本的回执都留在这里可查。
- `destinations`：接收地址、状态、连续失败次数、隔离可恢复时间、该地址下一个序号。

## 从一对一模型升级

表结构通过 `init_db` 幂等迁移：旧的按地址存事件的 `events` 表会自动改名为 `deliveries`（未投完的行原样保留，Worker 会接着投），并新建逻辑事件表 `events` 与订阅表 `destination_subscriptions`。老数据里的历史投递没有对应的逻辑事件，其 `event_id` 为 `NULL`，不影响继续投递。

回执对账的列（`reconcile_state` 等）和 `receipts` 表同样以幂等方式补齐。升级前已投妥的历史副本 `reconcile_state` 为 `none`，不会要求补回执；如果接收方对这些老副本补发回执，仍会按 `applied` 正常对账。

## 生产化建议

当前 Compose 配置适合开发和小规模部署。上生产前建议补充：

1. API 前增加负载均衡器和 HTTPS。
2. 为登记地址、提交事件和恢复接口增加认证/鉴权。
3. 使用托管 PostgreSQL，设置备份、连接数上限和慢查询监控。
4. 增加 Prometheus 指标：待投递数、投递延迟、失败率、隔离地址数、Worker 心跳、超时未对账副本数、迟到回执数。
5. 将 `CREATE TABLE IF NOT EXISTS` 替换为 Alembic 迁移，便于后续表结构变更。
