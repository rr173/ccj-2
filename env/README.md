# 事件外发系统

这是一个接收事件、按事件类型分发给订阅地址，并把事件按顺序推送到外部 Webhook 的两服务系统：

- **ingest-api**：只负责登记接收地址（含订阅的事件类型）、接收事件、查询事件投递轨迹和人工恢复隔离地址。
- **worker**：负责真正的 HTTP 投递、重试、熔断隔离、崩溃恢复。
- **PostgreSQL**：作为任务队列和事实来源，用行锁和每地址单调序号保证同一个接收地址严格 FIFO。

两个服务使用同一个镜像但启动命令不同，可以独立水平扩缩。

## 关键语义

### 1. 按事件类型分发（发布/订阅）

- 登记地址时用 `event_types` 声明它关心哪些事件类型；不传或传 `null` 表示保持现状（新地址则为不订任何类型），传列表（含空列表）则整体替换订阅集合。
- 提交事件时带 `event_type`，不再需要指定地址。入库的那一刻按当前订阅快照**扇出**：每个订了该类型的地址各得到一份独立的投递副本（delivery），各排各的队。
- **没人订的类型**：事件照常接收并持久化，状态为 `unrouted`，可以通过轨迹接口查到它一条都没投出去——不会写成成功。之后有地址补订该类型，也只从**下一条**新事件开始接收，不会把已入库的旧事件补投给它。
- 同一地址的重复登记是幂等的；重复提交同一 `dedupe_key` 的事件返回原事件并带 `duplicate: true`，不会重复扇出。

### 2. 同一接收地址严格按进入顺序投递

扇出时，每个地址的副本在该地址行锁内分配 `(destination_id, destination_seq)` 单调递增序号。Worker 每次只选择某地址当前最小的待投递副本，并用 `FOR UPDATE SKIP LOCKED` 锁定该地址：

- 同一个地址同一时刻只会被一个 Worker 线程处理；同一事件的不同地址副本互不等待。
- Worker 会先检查地址队头：队头投递中、未到下次重试时间或地址隔离时，后续副本不会越过它。
- Worker 崩溃时，队头会在租约超时后重新变为待投递，然后继续按序处理。
- 不同接收地址之间互不阻塞，可以并行投递；一个地址被隔离不影响订了同一类型的其他地址。

### 3. 重试与隔离

- 非 2xx 响应、连接失败、超时等都算投递失败。
- 使用指数退避并加入随机抖动：约为 `2s, 4s, 8s, ...`，最大 1 小时。
- 同一地址连续失败达到阈值（默认 5 次）后标记为 `isolated`。
- 隔离时间默认 15 分钟。隔离期间该地址的新副本继续入库排队，不影响其他地址。
- 隔离时间结束后，Worker 会自动将地址恢复为 `active`，并从尚未成功的最小序号副本继续投递。
- 也可以调用管理 API 立即人工恢复。

### 4. 去重与“接收方只处理一次”

事件必须带 `dedupe_key`，全局唯一：

- 重复提交不会生成新事件，接口返回原事件并带 `duplicate: true`。
- Worker 投递时会发送请求头：
  - `Idempotency-Key: <dedupe_key>`
  - `X-Event-Id: <event_id>`
  - `X-Event-Type: <event_type>`
  - `X-Delivery-Id: <delivery_id>`
- 投递采用至少一次语义：Worker 崩溃或网络超时时可能重复发送。接收方必须使用 `Idempotency-Key` 做幂等处理，从而做到业务效果只处理一次。

### 5. Worker 崩溃恢复（租约 + 心跳）

副本被领取后进入 `in_flight`，记录 `claim_token`、`claimed_at` 和 `lease_until`。投递期间，Worker 会用后台心跳线程按 `LEASE_HEARTBEAT_SECONDS`（默认 15 秒）把 `lease_until` 向后延长一个租约窗口（默认 60 秒）：

- **Worker 活着、只是接收方响应慢**：心跳持续续约，租约不过期，同一条副本不会被其他 Worker 重复领取或重复外发；该地址后续副本继续按 FIFO 被它挡在后面，直到这次调用结束。
- **Worker 进程真的挂掉或被 kill**：心跳随之停止，`lease_until` 在一个租约窗口后过期。其他 Worker 的 reaper 会把副本重新置为 `pending` 并从最小序号继续投递，不会丢任务。
- **极端情况下租约仍过期并被接管**（例如数据库长时间不可用导致心跳无法续约）：旧 Worker 那次 HTTP 调用结束后，不会再修改已被新 Worker 接管的副本，只在 `delivery_attempts` 中追加一条 `lost_lease=true` 的审计记录，因此这些“多打的一次”在投递轨迹里可见。

这样“慢响应”和“真宕机”被区分开：慢不会导致重复外发，宕机仍能从未成功处续投。

## 快速启动

```bash
docker compose up --build
```

服务：

- API：`http://localhost:8000`
- OpenAPI：`http://localhost:8000/docs`
- PostgreSQL：仅 Compose 内部暴露给两个服务

独立扩缩示例：

```bash
# 扩 Worker；每个实例默认 4 个投递线程
WORKER_CONCURRENCY=8 docker compose up --build --scale worker=3
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

响应中包含：

- `id`：事件 ID
- `event_type` / `dedupe_key` / `payload`：事件本体
- `status`：`unrouted`（没有地址订该类型）、`pending`（至少一份副本未投完）、`delivered`（全部副本已投妥）
- `delivery_count` / `delivered_count`：扇出副本总数 / 已投妥数
- `duplicate`：是否命中去重并返回已有事件

### 查询某条事件的投递过程

```bash
curl -s http://localhost:8000/v1/events/<event_id>/trace
```

返回事件当前状态、每个地址的副本（`deliveries`：状态、序号、已尝试次数、下次尝试时间、最近错误）和全部投递尝试（`attempts`：开始/结束时间、是否成功、HTTP 状态码、响应片段或错误信息；若某次调用是在租约丢失后返回的，会带 `lost_lease: true`）。没人订的事件在这里能看到 `status: "unrouted"` 且 `deliveries` 为空——是明确的“没送出去”，不是成功。

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

接收方只有返回 2xx 才算成功。请在接收方用 `Idempotency-Key` 做幂等表或唯一约束。

## 本地手工验证

可以启动一个会强制失败数次的接收方，观察重试、顺序和幂等：

```bash
python3 scripts/mock_receiver.py --port 9000 --fail-times 3
```

在 Linux + Docker Desktop/bridge 网络下，容器内通常可用 `http://172.17.0.1:9000/` 访问宿主机进程；不同环境请替换为实际可达地址。注册该地址（记得带上 `event_types`）后连续提交多条对应类型的事件，再用 `/v1/events/{id}/trace` 查看每份副本的每次尝试。

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

## 数据表概览

- `events`：事件本体（类型、去重键、负载），一条事件一行，与地址无关。
- `destination_subscriptions`：地址订阅的事件类型集合。
- `deliveries`：扇出后的每地址投递副本，含每地址顺序号、投递状态、下次尝试时间和租约信息；Worker 只消费这张表。
- `delivery_attempts`：每次 HTTP 投递尝试的审计轨迹（关联事件与副本）。
- `destinations`：接收地址、状态、连续失败次数、隔离可恢复时间、该地址下一个序号。

## 从一对一模型升级

表结构通过 `init_db` 幂等迁移：旧的按地址存事件的 `events` 表会自动改名为 `deliveries`（未投完的行原样保留，Worker 会接着投），并新建逻辑事件表 `events` 与订阅表 `destination_subscriptions`。老数据里的历史投递没有对应的逻辑事件，其 `event_id` 为 `NULL`，不影响继续投递。

## 生产化建议

当前 Compose 配置适合开发和小规模部署。上生产前建议补充：

1. API 前增加负载均衡器和 HTTPS。
2. 为登记地址、提交事件和恢复接口增加认证/鉴权。
3. 使用托管 PostgreSQL，设置备份、连接数上限和慢查询监控。
4. 增加 Prometheus 指标：待投递数、投递延迟、失败率、隔离地址数、Worker 心跳。
5. 将 `CREATE TABLE IF NOT EXISTS` 替换为 Alembic 迁移，便于后续表结构变更。
