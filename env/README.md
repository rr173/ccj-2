# 事件外发系统

这是一个接收事件并将事件按顺序推送到外部 Webhook 的两服务系统：

- **ingest-api**：只负责登记接收地址、接收事件、查询事件投递轨迹和人工恢复隔离地址。
- **worker**：负责真正的 HTTP 投递、重试、熔断隔离、崩溃恢复。
- **PostgreSQL**：作为任务队列和事实来源，用行锁和每地址单调序号保证同一个接收地址严格 FIFO。

两个服务使用同一个镜像但启动命令不同，可以独立水平扩缩。

## 关键语义

### 1. 同一接收地址严格按进入顺序投递

每个事件写入时，在目标地址行锁内分配 `(destination_id, destination_seq)` 单调递增序号。Worker 每次只选择某地址当前最小的待投递事件，并用 `FOR UPDATE SKIP LOCKED` 锁定该地址：

- 同一个地址同一时刻只会被一个 Worker 线程处理。
- Worker 会先检查地址队头：队头事件投递中、未到下次重试时间或地址隔离时，后续事件不会越过它。
- Worker 崩溃时，队头会在租约超时后重新变为待投递，然后继续按序处理。
- 不同接收地址之间互不阻塞，可以并行投递。

### 2. 重试与隔离

- 非 2xx 响应、连接失败、超时等都算投递失败。
- 使用指数退避并加入随机抖动：约为 `2s, 4s, 8s, ...`，最大 1 小时。
- 同一地址连续失败达到阈值（默认 5 次）后标记为 `isolated`。
- 隔离时间默认 15 分钟。隔离期间该地址的新事件继续入库排队，不影响其他地址。
- 隔离时间结束后，Worker 会自动将地址恢复为 `active`，并从尚未成功的最小序号事件继续投递。
- 也可以调用管理 API 立即人工恢复。

### 3. 去重与“接收方只处理一次”

事件必须带 `dedupe_key`。同一接收地址上 `(destination_id, dedupe_key)` 唯一：

- 重复提交不会生成新事件，接口返回原事件并带 `duplicate: true`。
- Worker 投递时会发送请求头：
  - `Idempotency-Key: <dedupe_key>`
  - `X-Event-Id: <event_id>`
- 投递采用至少一次语义：Worker 崩溃或网络超时时可能重复发送。接收方必须使用 `Idempotency-Key` 做幂等处理，从而做到业务效果只处理一次。

### 4. Worker 崩溃恢复

事件被领取后进入 `in_flight`，并记录 `claim_token` 和 `claimed_at`。如果 Worker 在投递过程中崩溃，其他 Worker 会在租约时间（默认 60 秒）后把该事件重新置为 `pending` 并继续投递。`delivery_attempts` 表保留每次尝试记录。

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

### 登记接收地址

```bash
curl -s http://localhost:8000/v1/destinations \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/webhook"}'
```

返回示例：

```json
{
  "id": "6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c",
  "url": "https://example.com/webhook",
  "status": "active",
  "failure_count": 0,
  "recoverable_at": null,
  "created_at": "2026-09-09T00:00:00Z"
}
```

重复登记同一 URL 是幂等的，会返回同一个地址。

### 提交事件

```bash
curl -s http://localhost:8000/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "destination_url": "https://example.com/webhook",
    "dedupe_key": "order-1001-paid",
    "payload": {
      "order_id": "1001",
      "event_type": "paid"
    }
  }'
```

响应中包含：

- `id`：事件 ID
- `destination_seq`：该接收地址上的顺序号
- `status`：`pending`（等待到点投递）、`in_flight`（Worker 正在投递）、`delivered`（已收到 2xx）
- `attempts`：已领取/尝试投递次数；重复提交不会增加该值
- `duplicate`：是否命中去重并返回已有事件

### 查询某条事件的投递过程

```bash
curl -s http://localhost:8000/v1/events/<event_id>/trace
```

返回事件当前状态和每次投递尝试：开始/结束时间、是否成功、HTTP 状态码、响应片段或错误信息。

### 立即恢复隔离地址

```bash
curl -s -X POST http://localhost:8000/v1/destinations/<destination_id>/recover
```

该操作：

1. 将地址从 `isolated` 改回 `active`；
2. 清空失败计数和自动恢复时间；
3. 将该地址所有未成功事件的下次尝试时间重置为当前时间；
4. Worker 随后从最小序号的未成功事件继续投递。

对已经是 `active` 的地址调用是幂等的，返回中 `recovered` 为 `false`。

## 投递给接收方的请求格式

Worker 发送：

```http
POST /webhook HTTP/1.1
Content-Type: application/json
Idempotency-Key: order-1001-paid
X-Event-Id: 6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c

{
  "event_id": "6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c",
  "dedupe_key": "order-1001-paid",
  "destination_seq": 12,
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

在 Linux + Docker Desktop/bridge 网络下，容器内通常可用 `http://172.17.0.1:9000/` 访问宿主机进程；不同环境请替换为实际可达地址。注册该地址后连续提交多条事件，再用 `/v1/events/{id}/trace` 查看每次尝试。

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `DATABASE_URL` | Compose 中已设置 | PostgreSQL SQLAlchemy/psycopg 连接串 |
| `DB_POOL_SIZE` | `10`，Worker 建议 `4` | SQLAlchemy 常驻连接数 |
| `DB_MAX_OVERFLOW` | `5`，Worker 建议 `2` | 连接池溢出连接数 |
| `WORKER_CONCURRENCY` | `4` | 单个 Worker 实例内的投递线程数 |
| `POLL_INTERVAL_SECONDS` | `0.5` | 无可投递任务时的轮询间隔 |
| `HTTP_TIMEOUT_SECONDS` | `10` | 单次 HTTP 请求超时 |
| `CLAIM_LEASE_SECONDS` | `60` | `in_flight` 事件多久后可被其他 Worker 接管 |
| `RETRY_BACKOFF_BASE_SECONDS` | `2` | 初始退避时间 |
| `RETRY_BACKOFF_MAX_SECONDS` | `3600` | 最大退避时间 |
| `FAILURE_THRESHOLD` | `5` | 连续失败多少次后隔离地址 |
| `QUARANTINE_SECONDS` | `900` | 自动隔离时长 |
| `MAX_RESPONSE_BODY_BYTES` | `2048` | 轨迹表保存响应体片段的最大长度 |

## 数据表概览

- `destinations`：接收地址、状态、连续失败次数、隔离可恢复时间、该地址下一个序号。
- `events`：事件内容、去重键、每地址顺序号、投递状态、下次尝试时间和租约信息。
- `delivery_attempts`：每次 HTTP 投递尝试的审计轨迹。

## 生产化建议

当前 Compose 配置适合开发和小规模部署。上生产前建议补充：

1. API 前增加负载均衡器和 HTTPS。
2. 为登记地址、提交事件和恢复接口增加认证/鉴权。
3. 使用托管 PostgreSQL，设置备份、连接数上限和慢查询监控。
4. 增加 Prometheus 指标：待投递数、投递延迟、失败率、隔离地址数、Worker 心跳。
5. 将 `CREATE TABLE IF NOT EXISTS` 替换为 Alembic 迁移，便于后续表结构变更。
