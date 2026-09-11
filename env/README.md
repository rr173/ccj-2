# 事件外发系统

这是一个接收事件、按事件类型分发给订阅地址，把事件按顺序推送到外部 Webhook，并对推送结果做**回执对账**的系统：

- **ingest-api**：登记**事件来源**（发放只属于它的签名密钥、可停用/可换钥）、登记接收地址（含订阅的事件类型；可把任一地址标成**只跟着看 `observe_only`**——照样收副本、按它自己的生命周期外发/重试/隔离/死信，但它的回执认不认、超时或进死信都不影响整笔算不算认完；**地址得先完成一次上线握手确认才会收到投递**，换接收位置要重新确认；可给任一地址标一段**现在不收**的时间——窗口内副本在原队列位置等、不算失败、对账不倒计时，订了同一类型的其他地址照打）、**按事件类型设定认完门槛**（当真副本认够份数整笔即认完且终态不可逆，影子副本不凑数；没定门槛的类型仍要求所有当真副本都认）、**验签 + 发送时间校验后**接收事件（可约定最早外发时间 `not_before`）、**取消/改期尚未打出的事件**、**给已收下的事件补一笔更正**（另补一笔新事件，只补给当初真正打到过的地址，排在各地址队尾，对账从真正打出才算，更正失败只算它自己的）、**接入回执与确认应答**、查询**入口准入记录（含每一条被拒事件，以及"收了但还没确认、一份没发"的事件）**、事件投递轨迹与整笔/逐地址对账情况、人工恢复隔离地址、把超时或失败回执导致未对上的副本重投（整笔重投在认够门槛后不再拉名单；没认够时只重投某一笔事件中尚未认的那些**当真**副本，只跟着看的副本不在名单内）。
- **worker**：负责真正的 HTTP 投递、重试、熔断隔离、崩溃恢复，以及向未确认地址发送上线握手请求（confirmer 线程）。
- **reconciler**：独立的对账进程，周期性把超过约定时间仍未收到回执的副本标记为 `timed_out`（可查，不算认）。
- **PostgreSQL**：作为任务队列和事实来源，用行锁和每地址单调序号保证同一个接收地址严格 FIFO。

前三个服务使用同一个镜像但启动命令不同，可以独立水平扩缩：回执接入（在 ingest-api 里）和对账（reconciler）都不在外发 worker 进程内，两边互不影响。

## 关键语义

### 0. 来源登记、密钥签名与准入（入口）

外部系统要往这里丢事件，得先登记来源并拿到一把只属于它的密钥：

- `POST /v1/sources`，body 为 `{"name": "<唯一名称>"}`。返回 `id` 和一次性展示的 `secret`——之后再查（`GET /v1/sources[/{id}]`）不会再返回密钥，请当场保存。
- 每次提交事件都必须带头：
  - `X-Source-Id`：登记时拿到的来源 id；
  - `X-Signed-At`：**发送时刻**的 Unix 秒级时间戳；
  - `X-Signature`：`hex(HMAC_SHA256(secret, "<X-Signed-At>." + 原始请求体))`（可选 `sha256=` 前缀）。签名覆盖的是未经解析的原始字节，用常量时间比较。
- 可以用 `python3 scripts/push_event.py --register <name>` 登记，用 `python3 scripts/push_event.py --source-id ... --secret ... ...` 签名发送。

**对不上、太旧、重复，都不能收成新事件，并且能查到被拒了：**

| 情况 | HTTP | 准入记录 disposition |
|---|---:|---|
| 没带/乱填来源 id、来源没登记 | 401 | `source_unknown` |
| 没带签名，或签名和该来源密钥对不上（报文被改也算） | 401 | `bad_signature` |
| `X-Signed-At` 不是数字时间戳 | 401 | `invalid_timestamp` |
| 发送时间旧于 `INGEST_MAX_AGE_SECONDS`（默认 300 秒，防重放） | 401 | `stale_timestamp` |
| 发送时间超前于服务时钟 `INGEST_MAX_FUTURE_SKEW_SECONDS`（默认 60 秒） | 401 | `future_timestamp` |
| 来源已停用 | 403 | `source_disabled` |
| 报文不是合法 JSON / 不符事件 schema | 422 | `invalid_body` |
| 同一 `dedupe_key` 再送一次（即便换一个来源送） | 200 | `duplicate`（返回原事件，`duplicate: true`，不新建事件、不二次扇出） |

- **每一次入口尝试都落 `ingestion_attempts` 表**，包括所有被拒的。用 `GET /v1/ingestion/attempts?rejected_only=true` 或按 `source_id` / `disposition` / `dedupe_key` 过滤即可查到"谁、什么时间、因为什么被拒"。被拒记录的 `event_id` 为空——它从未成为事件，轨迹/对账里绝不会把它写成"已收下/已发出"。
- 错误响应形如 `{"detail": {"error": "...", "disposition": "bad_signature"}}`，与日志中的处置一一对应。

**对上密钥且时间新鲜的，才按现在的类型和确认状态分发（见第 1 节）**。几个边界：

- **没人订的类型对上了也要照收**：事件正常持久化，状态 `unrouted`，准入记录为 `unrouted`；不会投出任何副本，也不会写成已发出。之后补订只对新事件生效。
- **有人订但地址还没完成上线确认也要照收**：事件正常持久化，但不生成任何投递副本，准入记录为 `pending_confirmation`；等该地址确认之后也只接新事件，已收下的旧事件不补投。
- **停用来源只关入口**：`POST /v1/sources/{id}/disable` 之后，新来的事件一律 `source_disabled` 拒收；**已经收下、已经排进各地址队列的副本不受影响，继续按原队列往外打，不会被从队列里拿掉**。`POST .../enable` 可恢复。
- **换密钥后只认新的**：`POST /v1/sources/{id}/rotate-key` 当场覆盖密钥并返回新 `secret`（旧密钥不保留）。换钥后拿旧密钥签的事件立即 `bad_signature` 被拒，不需要重启或等待。
- 事件记录带 `source_id`，事件响应和 trace 里可查它来自哪个来源。

### 1. 按事件类型分发（发布/订阅）与上线确认

- 登记地址时用 `event_types` 声明它关心哪些事件类型；不传或传 `null` 表示保持现状（新地址则为不订任何类型），传列表（含空列表）则整体替换订阅集合。
- **接收地址得先完成一次上线确认（握手），对上之后才准往它那里打。** 新登记（或换了接收位置）的地址状态是 `pending`：系统向该地址发一个带一次性 `challenge` 的确认请求，对方得在约定时限（`CONFIRM_TIMEOUT_SECONDS`，默认 300 秒）内把 challenge 原样对上，地址才变成 `confirmed`。对上有两种方式：
  - 在确认请求的 2xx 响应体里回 `{"echo": "<challenge>"}`；
  - 或由接收方回调 `POST /v1/destinations/{id}/confirm`（body `{"challenge": "<challenge>"}`，也可用 `X-Confirmation-Challenge` 头）。
- 提交事件时带 `event_type`，不再需要指定地址。入库的那一刻按当前订阅快照**扇出**：每个**订了该类型且已确认**的地址各得到一份独立的投递副本（delivery），各排各的队。确认闸门的边界：
  - **没对上之前，事件还是照收**：事件正常持久化，但**不会给该地址生成副本**——既不会在轨迹/对账里写成"已发给这个地址"，更不会真打过去。这种情况的准入记录是 `pending_confirmation`（有人订但还没确认），区别于压根没人订的 `unrouted`。
  - **对上之后只接新事件**：确认只对此后入库的事件生效；确认之前收下的旧事件不会被补投（本来就没有副本，无从补起）。
  - **确认错过点了或回错了**：回错 challenge 记为 `invalid`、不改变未确认状态；一轮时限内没对上，这一轮作废（记 `expired`）并自动开一个全新 challenge 的新一轮。无论哪种，旧事件都不会在之后确认成功时被补走，只接新一轮确认之后的新事件。也可调 `POST /v1/destinations/{id}/reissue-challenge` 主动换一个新 challenge。
  - 确认请求与事件投递是两种消息：确认请求带 `X-Message-Type: activation_challenge` 头，body 的 `type` 为 `activation_challenge`，接收方应据此区分，不要当成业务事件。
- **换接收位置要重新对确认**：`PATCH /v1/destinations/{id}`（body `{"url": "..."}`，可同时带 `event_types`）改 URL 后地址立即回到 `pending`、确认代号 `confirmation_generation` 加一、开新一轮握手；
  - 没对上之前不能再往原来那个位置打：worker 的领取闸门同时要求地址已确认且副本的代号与当前代号一致，旧代号副本不会被送到新 URL；
  - 已经排进队列、还没打出去的旧代号副本置为终态 `superseded`（永不再投、也不补投），事件状态相应地可显示为 `superseded`（所有副本都被取代、没有任何一份在打）；
  - **已经打出去的不用收回来**：换位置时正在投（`in_flight`）或已投妥的副本保持原样；正在投的那次若拿到 2xx 就算交给了旧位置（回执对账照常），若失败则直接置 `superseded`，不会重试、也不会计入新位置的失败/隔离计数。
- **没人订的类型对上了也要照收**：即使存在已确认地址，只要没有任何地址订阅该事件类型，事件照常接收、持久化，状态为 `unrouted`——不会因为地址确认过了就把它当成已发出。之后补订只对新事件生效。
- 同一地址的重复登记是幂等的（同一 URL 再次 `POST` 不会重新握手）；重复提交同一 `dedupe_key` 的事件返回原事件并带 `duplicate: true`，不会重复扇出。

### 1.1 只跟着看的地址（observe_only / "影子订阅"）

登记或改地址时可以带 `"observe_only": true`，把一个接收地址标成**只跟着看、不当真**（默认 `false`，即当真）。它和当真地址**一样走完整流程**，唯独不进整笔事件的"认不认账"：

- **订了的类型照样给它各排一份**：扇出时它也拿到自己的独立副本（`deliveries` 里带 `observe_only = true` 的快照标记），之后外发、指数退避重试、连续失败隔离、耗尽进死信处、回执对账（认了 / 失败回执 / 超时 / 迟到回执）、换位置时代号失效与 `superseded`，**全部按它自己的副本独立进行**，与当真地址没有任何区别。这个标记在扇出那一刻快照到副本上，之后再改开关只影响新副本，老副本保持出生时的样子。
- **它认不认、超时还是进死信，都改不了整笔算不算认完**：整笔的 `reconcile_status` 只由**当真副本**决定。影子副本回成功回执，只把它自己那份记成 `acknowledged`，整笔仍是 `pending` / `partially_acknowledged`；已经被当真副本认成 `acknowledged` 的整笔，也不会被影子副本的失败回执、超时或进死信带偏。传输层 `status` 同理：影子副本的 2xx 不能把整笔写成 `delivered`，它进死信也不会把整笔写成 `dead_lettered`。
- **整笔重丢未认副本时不算它**：`POST /v1/events/{event_id}/requeue-unreconciled` 的名单只含当真副本；影子副本即使超时/失败也不会被这条整笔接口重丢。影子副本**自己**仍然可以单独重投——`POST /v1/deliveries/{delivery_id}/requeue` 和按地址的 `POST /v1/destinations/{id}/requeue-unreconciled` 照常作用于它，预算和死信规则按它自己那份算。
- **把它停掉（隔离）或丢进死信，挡不住订了同一类型的当真地址**：隔离/死信本来就是按地址、按副本独立的；影子地址失败计数、隔离恢复、死信后越过它继续排后面的副本，全部只影响它自己。
- **查某一笔时分得清谁是当真、谁是跟着看**：事件响应与 trace 的 `event` 里，当真副本计数是 `delivery_count` / `delivered_count` / `acknowledged_count` / `unacknowledged_count` / `dead_lettered_count`；影子副本单列 `shadow_delivery_count` / `shadow_delivered_count` / `shadow_acknowledged_count` / `shadow_unacknowledged_count` / `shadow_pending_count` / `shadow_dead_lettered_count` 等。每份副本（trace 的 `deliveries`、对账明细、死信明细）都带 `observe_only` 布尔字段。**影子副本认了绝不写成整笔已认完**——当真份数为 0、只有影子副本的整笔，`reconcile_status` 永远停在 `pending`。
- **只有影子订的类型**：事件照收、照常投给这些影子地址（传输层可到 `delivered`），但整笔 `reconcile_status` 不会变 `acknowledged`；它也**不会**被当成 `unrouted`——准入记录仍是 `accepted`，因为确实有已确认订户收到了。
- **没标成跟着看的地址维持现状**：不带 `observe_only` 的地址就是当真地址，所有计数、整笔状态、整笔重投行为与以前完全一致。
- **没人订的类型还是照收**：当真和影子订户都没有时，事件仍是 `unrouted`、一份不投、也不会写成已发出；影子订户不计入"没人订"。

开关方式：登记时 `"observe_only": true/false`；之后用 `PATCH /v1/destinations/{id}` 带 `{"observe_only": false}` 改（不动 URL、不重新握手）；同一 URL 再次 `POST` 登记时，传了 `observe_only` 就整体改标，不传保持现状。地址详情的 `observe_only` 字段随时可查。

### 1.2 按事件类型设"认完门槛"（ack threshold）

每种事件类型可以单独定一个**认完门槛**：当真收到的成功回执**认够这个份数**，这一笔就算认完，不必等剩下的地址。

- `PUT /v1/event-types/{event_type}/ack-threshold`，body 为 `{"ack_threshold": 2}`（必须 ≥1）；`GET .../ack-threshold` 查单个类型；`GET /v1/event-types/ack-thresholds` 列全部；`DELETE .../ack-threshold` 或 PUT 传 `{"ack_threshold": null}` 取消门槛。
- **只数当真副本**：认够门槛只看**非 `observe_only`** 的副本对上成功回执的份数；跟着看的影子副本认不认都**不能拿来凑数**。
- **入库那一刻快照**：门槛（以及"没有门槛就要求所有当真副本"这个默认规则）在事件入库扇出时算成该事件自己的 `required_ack_count`：
  - 定了门槛：取 `min(门槛, 当时已确认的当真订户数)`——门槛比当真订户多不会让事件永远认不完；
  - 没定门槛：等于当时扇出的当真副本份数（老事件无快照，同样按"现存每一份当真副本都认"处理）；
  - 之后再改/删门槛只影响**新事件**，已经收下的事件按自己的快照走。
- **认够之后是终态**：`reconcile_status` 一旦变成 `acknowledged`（`acknowledged_quorum = true`），剩下没认的当真副本再超时、回失败回执、耗尽重倒进死信，都**不会**把整笔改回 `pending` / `partially_acknowledged`。判断只看"已认份数"，这个数只会增加、不会减少。
- **整笔重丢未认副本时**（`POST /v1/events/{event_id}/requeue-unreconciled`）：
  - **已经认够**：不再拉名单，直接返回 `requeued_count = 0`、`already_acknowledged = true`；没认的副本仍按各自生命周期走（可以单独按副本/按地址重投），只是整笔接口不再替它们重丢；
  - **还没认够**：只把**当真**副本里已终态未对上的（`timed_out` / `receipt_failed`）按原地址/原顺序重投，影子副本照旧不在名单内，仍在传输/等回执的也不会被提前重打。
- **没定门槛的类型**：维持原规则——订了该类型的**每一份当真副本**都对上成功回执才算认完。
- **没人订的类型**：照样收下、持久化，状态仍是 `unrouted`（只有影子订户时也不是 `unrouted`，但整笔永远停在 `pending`），不会因为存在门槛配置就被当成已发出或已认完；对这种事件调整笔重投返回空名单且 `already_acknowledged = false`。
- 事件响应 / trace 的 `event` 里带：`ack_threshold`（该类型**当前**配置，可能为 `null`）、`required_ack_count`（这笔自己的快照门槛，`unrouted`/只有影子订户时为 0）、`acknowledged_quorum`（是否已经认够）。`acknowledged_count` / `unacknowledged_count` 仍只统计当真副本，影子计数继续单列在 `shadow_*`。

### 1.3 给地址标一段不收的时间（暂停接收窗口）

每个接收地址都可以标一段**现在不收**的时间（`POST /v1/destinations/{id}/pause`），比如对方停机维护、要求冻结推送的时段：

- body：`{"paused_from": "...", "paused_until": "..."}`（ISO 8601，无时区按 UTC 理解）。`paused_from` 不传表示**从现在开始**；`paused_until` 不传表示**一直停到人工恢复**；两个字段一个都不传是 422。`paused_until` 必须晚于 `paused_from`，且不能整个窗口都落在过去（多半是时区写错了，直接 422 挑明）。重复调用整体替换旧窗口；`POST .../resume` 随时清掉窗口（幂等，本来没窗口时返回 `resumed: false`）。地址详情带 `paused_from` / `paused_until` / `paused`（此刻是否在窗口内，按数据库时钟算）。
- **没到点的就在原队伍里等**：窗口生效期间，该地址队列里的副本原地不动（序号、位置都不变），Worker 根本不领取这个地址；窗口一结束就按原来的先后顺序接着投，也**不会插到正在投的副本前面**（领取闸门照旧：地址还有副本在投，排队的就不会被领取）。窗口打开时已经在投的副本正常投完——打出去的收不回来。
- **对账从真正打出去之后才开始算**：等窗口的副本 `reconcile_state` 一直是 `none`，没有任何倒计时；`reconcile_deadline` 在投妥那一刻才写入（投妥时间 + `RECEIPT_TIMEOUT_SECONDS`）。提交时间、等窗口的时间都不计入——哪怕等窗口的时间早就超过了对账时限，投出去后照样有完整的回执窗口。
- **停收期间不算失败**：窗口内一次都不会真正打，没有任何尝试可记，`failure_count` 不变、**不会被当成连续失败去隔离**；窗口结束后第一次真打如果真失败，才按正常规则计数。
- **只影响这一个地址**：订了同一类型的其他地址照打不误；暂停期间新扇出的副本照常进它的队列，排到窗口结束。
- 窗口只管事件投递：上线确认探测（confirmer）不受窗口影响，未确认的地址可以在停收期间完成握手，窗口一结束就能收事件。

### 1.4 给已收下的事件补一笔更正（correction）

每一笔已经收下的事件，之后都可以再补一笔更正：`POST /v1/events/{event_id}/corrections`，body 为 `{"dedupe_key": "...", "payload": {...}}`。更正是**另补的一笔**，不是把原来那份收回来改：

- **原来打出去的那份不动**：更正本身是一笔新事件（响应与查询里带 `corrects_event_id` 指向原来那笔），有自己的去重键、自己的副本、自己的对账生命周期；原来那笔事件的副本、回执状态、整笔 `reconcile_status` 一概不被改写。
- **只补给当初真正打到过的地址**：扇出名单 = 提交更正那一刻，原来那笔里已经真正投妥（`delivered_at` 有值）的副本所在的地址。还在排队、已取消、被取代（`superseded`）或根本没扇出过的地址一律不补——哪怕它之后投妥了，也不回头补这一笔。一个地址都没打出去过时，更正直接被拒（见下）。
- **排在原队伍后面等**：每份更正副本拿该地址新的队尾序号（`destination_seq`），排在已排队的所有副本后面；领取闸门照旧——只要该地址还有副本在投（`in_flight`），更正副本就不会被领取，不会插到人家正在投的前面。
- **对账从真正打出去之后才开始算**：更正副本投妥那一刻才进入 `awaiting` 并写入 `reconcile_deadline`（投妥时间 + `RECEIPT_TIMEOUT_SECONDS`）；提交更正的时间、排队等待的时间都不计入倒计时。接收方按更正自己的 `dedupe_key` 回执（回 `(destination_id, 更正的 dedupe_key)` 即可）。
- **更正失败按它自己算**：更正副本外发失败（非 2xx / 连接失败 / 超时）后**不原地重试**，直接置终态 `failed`——不计入该地址的连续失败计数、**不会触发隔离**、也不会挡在队列里让后面还在排的副本等它（它已不在队头）。失败可在更正自己的轨迹里查到（副本状态 `failed`、投递尝试里留着那次失败）。原来那笔已经认了的副本不受任何影响。想再补一次，用新的 `dedupe_key` 重新提交一笔更正即可。
- **只跟着看的地址有自己那份就跟自己的**：影子地址当初那份如果真打出去了，更正也照样给它补一份，并继承原来那份的 `observe_only` 快照——它自己的更正副本自己外发、自己对账，但**不能拿来给更正这笔的认完门槛凑数**（更正事件的 `required_ack_count` 只数当真副本，快照规则与入库扇出相同：类型门槛与当真份数取小）。
- **没人订或根本没收下的不能更正**：原来那笔是 `unrouted`（没人订或都没确认，一份副本都没有）、有副本但一份都没打出去、或已取消的，`POST` 更正返回 `409`，什么都不创建；事件 id 不存在（包括入口就被拒、从未成为事件的）返回 `404`。`GET /v1/events/{event_id}/corrections` 随时能查：没有更正就是空列表，绝不会写成已经发出去。
- **同一笔更正重复送来只认一次**：更正的 `dedupe_key` 全局唯一（与事件同一个命名空间）。重复提交同一笔更正返回 `200` 和原更正并带 `duplicate: true`，不新建、不二次扇出；`dedupe_key` 已被别的事件/更正占用时返回 `409`。被拒（409）的更正不占用键——等内容真的打出去了，可以拿同一个键再提交。

更正本身是一笔普通事件：可以用 `/v1/events/{更正id}/trace` 查它自己的投递轨迹与整笔对账状态；它的副本照常过确认闸门、停收窗口、回执对账、超时/失败回执重投与死信规则（只有传输层失败不同——见上，直接 `failed` 不原地重试）。原来那笔的轨迹（`GET /v1/events/{event_id}/trace`）里新增 `corrections` 段，列出它名下的全部更正；投递给接收方的更正请求会带头 `X-Corrects-Event-Id` 和 body 字段 `corrects_event_id`，接收方可以据此把它和原来那笔关联起来。

### 2. 同一接收地址严格按进入顺序投递

扇出时，每个地址的副本在该地址行锁内分配 `(destination_id, destination_seq)` 单调递增序号。Worker 每次只选择某地址当前最小的待投递副本，并用 `FOR UPDATE SKIP LOCKED` 锁定该地址：

- 同一个地址同一时刻只会被一个 Worker 线程处理；同一事件的不同地址副本互不等待。
- Worker 会先检查地址队头：队头投递中、未到下次重试时间、**未到约定外发时间（`not_before`）**、**地址在停收窗口内（见 1.3 节）**或地址隔离时，后续副本不会越过它。
- Worker 崩溃时，队头会在租约超时后重新变为待投递，然后继续按序处理。
- 不同接收地址之间互不阻塞，可以并行投递；一个地址被隔离不影响订了同一类型的其他地址。

### 3. 定时外发、取消与改期

提交事件时可以带 `not_before`（ISO 8601 时间，无时区按 UTC 理解），约定这条事件**最早什么时候才准往外打**：

- **没到点不发出**：扇出时 `not_before` 写到每份副本上，Worker 只领取 `not_before` 已过的副本。没到点的副本就排在它在该地址队列里原来的位置（序号在提交那一刻已分配）等到点，**不会提前打，也不会被后面提交的副本越过**；到点后按该地址原有顺序投递，也不会插到正在投的副本前面。
- **没到点可以不要**：`POST /v1/events/{event_id}/cancel`。只要还没有任何一份副本打出去（含正在投），取消就成功：所有未投副本置为终态 `cancelled`，**之后任何时候都不会再打出去**，事件状态变为 `cancelled`。重复取消是幂等的。只要已有一份副本投妥或正在投，取消返回 `409`——打出去的收不回来。
- **没打出去前可以改时间**：`POST /v1/events/{event_id}/reschedule`，body 为 `{"not_before": "..."}`（传 `null` 表示取消定时、轮到就投）。改期只动未投副本的时间门槛，**每份副本在该地址队列里的位置不变**。同样地，只要有一份副本已投妥或正在投，改期返回 `409`——**已经打出去的不能改时间**；已取消的事件也不能再改期。
- **对账从真正打出去之后才开始算**：`reconcile_deadline` 是在投妥那一刻才写入的（投妥时间 + `RECEIPT_TIMEOUT_SECONDS`）。提交时间、定时等待的时间都不计入倒计时；没到点、未投妥、已取消的副本 `reconcile_state` 一直是 `none`，reconciler 不会扫它们。
- **没人订的类型照收**：带 `not_before` 的无人订阅事件同样正常接收、持久化，状态 `unrouted`，不会被当成已发出；也可以对它取消或改期（只改事件记录，本来就没有副本）。

### 4. 重试、隔离与死信处

- 非 2xx 响应、连接失败、超时等都算投递失败。
- 使用指数退避并加入随机抖动：约为 `2s, 4s, 8s, ...`，最大 1 小时。
- 同一地址连续失败达到阈值（默认 5 次）后标记为 `isolated`。
- 隔离时间默认 15 分钟。隔离期间该地址的新副本继续入库排队，不影响其他地址。
- 隔离时间结束后，Worker 会自动将地址恢复为 `active`，并从尚未成功的最小序号副本继续投递。
- 也可以调用管理 API 立即人工恢复。

**外发连续失败到点，自动停止重试、进死信处：**

- 每一份副本有自己的连续传输失败计数（一份成功拿到 2xx 即清零；人工从死信捞回也清零，重新计数）。同一份连续失败达到 `MAX_DELIVERY_ATTEMPTS`（默认 10 次）后，该副本停止重试，进入死信处：副本状态置为终态 `dead_lettered`，记录原因 `delivery_attempts_exhausted`、进入时间、连续失败次数和最后一次错误。
- **进了死信的副本不会再自己往外打**：Worker 只领取 `pending`/`in_flight` 的队头，永远不会碰 `dead_lettered` 行。
- **也不会挡住同一地址后面还在排的副本**：Worker 的队头只看可领取状态，死信副本会被越过，它后面（序号更大）的副本直接顶上。触发死信的那一次失败同时把地址的失败计数清零、把地址恢复为 `active`（死信副本本身已不在队列，不应让它把整地址的后续副本挡在隔离墙后面）；下一份副本充当探针，真还失败会按正常阈值重新隔离。其他地址不受任何影响。
- 连续失败计数是**按副本**的，与地址的隔离计数相互独立：地址级隔离负责"这个地址暂时别打"，副本级死信负责"这一份别再自动打"。

**对账反复对不上，同样进死信处（但已经认了的绝不进去）：**

- 对 `timed_out` / `receipt_failed` 的副本做重投（见第 7 节）会累加 `requeue_count`。最多重投 `MAX_REQUEUE_CYCLES`（默认 3）轮：
  - 某一轮重投后仍在约定时限内等不到回执 → reconciler 在把它标记为 `timed_out` 的同时直接送进死信处，原因 `receipt_timeout_exhausted`；
  - 某一轮重投后接收方回的是失败回执（且此时重投轮次已用完）→ 回执按 `receipt_failed` 记为这次对账结果（**不是认**），同时送进死信处，原因 `receipt_failure_exhausted`。
- **已经 `acknowledged` 的副本永远不会进死信**：只有"还在等回执却超时"和"对方明确回失败"这两条路会停车；成功回执一旦对上即终局。
- 重投轮次计数也在人工复活时清零，所以捞回来的一份重新获得完整的重投预算。

**死信可查、可人工捞回：**

- `GET /v1/dead-letters`：列出死信处里的每一份，可按 `destination_id` / `event_id` / `reason` / `dedupe_key` 过滤。每条都直接给出"哪一份"（delivery id、event、dedupe_key、序号、尝试次数、连续失败次数、最后错误）、"哪个地址"（destination id + URL）和"为什么进去、什么时候进去的"（`dead_letter_reason` / `dead_lettered_at`）。
- `GET /v1/dead-letters/summary`：按原因汇总数量。
- 事件轨迹（trace）与事件响应里同样能看到死信副本；当一笔事件的所有活动副本都进了死信、且没有还在排队/投递中的副本时，事件整体 `status` 显示为 `dead_lettered`（不会被误报成 `delivered`）。
- `POST /v1/dead-letters/{delivery_id}/revive`：**人工**把指定的一份从死信处丢回它原来那个地址。副本保留原来的 `destination_seq`，按原顺序回到该地址队列中原来的位置；领取闸门保证只要该地址还有副本正在投（`in_flight`），复活回来的副本就不会被领取——不会插到正在投的前面。复活仍然过确认闸门：地址当前必须 `confirmed`、且副本的确认代号与当前代号一致（换过位置的老副本不能捞到新 URL 去）。复活把连续传输失败计数和重投轮次计数清零、清空死信原因/时间，下一轮按新预算重新计数。
- 复活是把副本送出死信处的**唯一**途径；系统自身不会自动捞回。

**没人订的类型不会进死信：** 无人订阅（`unrouted`）的事件根本没有任何副本，不会投出、不会被当成已发出，也无副本可进死信。

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

- `pending`：还没有任何一份**当真**副本对上成功回执（包括仍在传输、等待回执、已超时、收到失败回执，或有当真副本在死信处等待人工处理；只跟着看的副本无论对上与否都不计入这里的判断）；
- `partially_acknowledged`：至少一份当真副本已经对上，但还没认够这笔自己的 `required_ack_count`；轨迹中可逐个查看谁是 `acknowledged`、谁还在 `awaiting`、谁是 `timed_out` / `receipt_failed`（每份副本带 `observe_only`，跟着看的那些单列 `shadow_*` 计数）；
- `acknowledged`：当真副本的成功回执**认够了门槛**。没有为该类型定门槛时，要求事件扇出的**每一份当真副本**都在时限内收到成功回执；定了门槛（见 1.2 节）时，只要求 `required_ack_count` 份（入库时按"门槛与当真订户数取小"快照）对上即可，没认的其余副本不必再等。**认够是终态**：之后其余当真副本的超时、失败回执、进死信都不能把整笔改回未认。只跟着看的副本认与不认都不改变这个结果：它没认不会把整笔拖回未认，它认了也不能替整笔凑数。
- `acknowledged_count / unacknowledged_count` 分别给出已认和未认的**当真**份数；`required_ack_count` 是这笔自己的认完门槛快照，`acknowledged_quorum` 表示当前是否已经认够；影子副本的对应数字在 `shadow_acknowledged_count / shadow_unacknowledged_count`（以及 `shadow_delivery_count` 等）里；`delivery_count = 0` 的无人订阅事件仍是明确的 `unrouted`，不会被汇总成已认；`delivery_count = 0` 但有影子副本的事件不是 `unrouted`（确实投给了影子订户），但整笔也永远不会变成 `acknowledged`。

`status` 仍只表示传输层状态（`pending` / `delivered` / `unrouted`），不能把“全部收到 2xx”当成整笔已认；整笔是否认完只看 `reconcile_status`。

#### 超时/失败副本的重投

`timed_out` 或 `receipt_failed` 的副本可以丢回原地址重投：

- `POST /v1/deliveries/{delivery_id}/requeue`：重投单个副本；
- `POST /v1/events/{event_id}/requeue-unreconciled`：只把这一笔事件中已经终态未对上的副本（`timed_out` / `receipt_failed`）按原地址批量重投；
- `POST /v1/destinations/{destination_id}/requeue-unreconciled`：把该地址所有未对上的副本一次性重投。

**重投不是无限的**：每份副本最多被重投 `MAX_REQUEUE_CYCLES`（默认 3）轮。重投后依旧超时（reconciler 标记 `timed_out` 时）或在最后一轮收到失败回执（`receipt_failed`），该副本直接进死信处（原因分别是 `receipt_timeout_exhausted` / `receipt_failure_exhausted`），不再自动重打；之后只能人工从死信处捞回（见第 4 节）。

事件级重投只选择未认的**当真**副本：已经 `acknowledged` 的副本不会再打（**已认的也永远不会被塞进死信**），仍在传输或仍处于本轮等待回执窗口的副本也不会被提前重打；**只跟着看（`observe_only`）的副本即使超时/失败也不在事件级重投名单内**——要重丢影子副本，请用按副本或按地址的重投接口，按它自己的预算走。**整笔已经认够门槛（`acknowledged_quorum = true`，无门槛类型即所有当真副本都认）后，这个接口不再拉名单**：直接返回 `requeued_count = 0`、`already_acknowledged = true`，还没认的那些副本是否超时/失败/进死信都不影响整笔，需要重丢只能逐副本或按地址操作。**重投还过确认闸门**：地址当前必须是 `confirmed`，且副本的确认代号与地址当前代号一致——地址还没对上确认、或该副本是换位置前的老副本时不能重投（老副本不会被打到新位置）。重投**保留原来的 `destination_seq`**，每份副本回到自己原地址队列中原来的位置，按原顺序接着排；各地址仍互不等待。同时 Worker 的领取逻辑保证：只要某地址还有副本在投（`in_flight`），重投回来的副本就不会被领取——不会插到还在投的副本前面。重投成功后该副本重新进入 `awaiting`，等待新一轮回执。

#### 对账查询

- `GET /v1/reconciliations/summary`：各对账状态的副本数量；
- `GET /v1/reconciliations/deliveries?reconcile_state=timed_out&destination_id=...`：列出指定状态（默认 `timed_out`）的副本明细；
- `GET /v1/dead-letters/summary`：死信处按原因汇总的数量（传输失败耗尽 / 回执超时耗尽 / 失败回执耗尽）；
- `GET /v1/dead-letters?destination_id=...&event_id=...&reason=...&dedupe_key=...`：死信处明细，每条都能直接查出是哪一份、哪个地址（含 URL）、因何原因在何时进入；
- `POST /v1/dead-letters/{delivery_id}/revive`：人工把指定一份丢回原地址原序号位置接着排（不插正在投的队、过确认闸门）；
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

### 登记事件来源并取得密钥（入口鉴权）

```bash
curl -s http://localhost:8000/v1/sources \
  -H 'Content-Type: application/json' \
  -d '{"name":"billing-system"}'
```

返回示例（`secret` **只在登记和换钥时返回这一次**，请立即保存）：

```json
{
  "id": "7a2c...",
  "name": "billing-system",
  "status": "active",
  "disabled_at": null,
  "key_rotated_at": "2026-09-10T06:00:00Z",
  "created_at": "2026-09-10T06:00:00Z",
  "secret": "A1b2...只显示这一次"
}
```

停用/恢复/换钥，以及查询入口准入记录：

```bash
curl -s -X POST http://localhost:8000/v1/sources/7a2c.../disable     # 只关入口
curl -s -X POST http://localhost:8000/v1/sources/7a2c.../enable      # 恢复
curl -s -X POST http://localhost:8000/v1/sources/7a2c.../rotate-key  # 旧密钥立即失效
curl -s 'http://localhost:8000/v1/ingestion/attempts?rejected_only=true'
```

提交事件时必须用来源密钥对 `<发送时间戳>.<原始报文>` 做 HMAC-SHA256，并带三个头。手工签名示例：

```bash
SECRET='A1b2...'; SOURCE='7a2c...'
BODY='{"event_type":"paid","dedupe_key":"order-1001-paid","payload":{"order_id":"1001"}}'
TS=$(date +%s)
SIG=$(printf '%s' "$TS.$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $2}')
curl -s http://localhost:8000/v1/events \
  -H 'Content-Type: application/json' \
  -H "X-Source-Id: $SOURCE" -H "X-Signed-At: $TS" -H "X-Signature: $SIG" \
  -d "$BODY"
```

或直接用辅助脚本（密钥也可用 `EVENT_SOURCE_SECRET` 环境变量传入）：

```bash
python3 scripts/push_event.py --register billing
python3 scripts/push_event.py --source-id 7a2c... --secret "$SECRET" \
  --event-type paid --dedupe-key order-1001-paid --payload '{"order_id":"1001"}'
```

对不上密钥、发送时间太旧/太超前、来源已停用、同一 `dedupe_key` 重放，都不会成为新事件；都会带相应 `disposition` 出现在 `/v1/ingestion/attempts` 中。

### 登记接收地址并声明订阅的事件类型

```bash
curl -s http://localhost:8000/v1/destinations \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/webhook","event_types":["paid","refunded"]}'
```

# 只想跟着看、不当真（照样收副本、按自己的节奏外发/重试/隔离/死信/对账，但不影响整笔算不算认完、不进整笔重投名单）：

```bash
curl -s http://localhost:8000/v1/destinations \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/audit-mirror","event_types":["paid"],"observe_only":true}'
# 之后想当真：PATCH /v1/destinations/<id>  body {"observe_only": false}（不重新握手，只影响新事件）
```

给某类事件定"认完门槛"（当真地址认够这么多份，整笔就算认完，不用等剩下的；只跟着看的地址不能凑数）：

```bash
# paid 类型：2 个当真地址对上成功回执就算整笔认完
curl -s -X PUT http://localhost:8000/v1/event-types/paid/ack-threshold \
  -H 'Content-Type: application/json' \
  -d '{"ack_threshold": 2}'

curl -s http://localhost:8000/v1/event-types/paid/ack-threshold     # 查单个类型
curl -s http://localhost:8000/v1/event-types/ack-thresholds         # 列全部
curl -s -X DELETE http://localhost:8000/v1/event-types/paid/ack-threshold  # 取消门槛（恢复"全部当真副本都认"）
# PUT 传 {"ack_threshold": null} 同样是取消；ack_threshold 必须 >= 1，否则 422
```

返回示例（新地址**还没对上确认**，不会收到任何事件；字段以 `confirmation_` 开头）：

```json
{
  "id": "6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c",
  "url": "https://example.com/webhook",
  "status": "active",
  "failure_count": 0,
  "event_types": ["paid", "refunded"],
  "recoverable_at": null,
  "created_at": "2026-09-09T00:00:00Z",
  "confirmation_state": "pending",
  "challenge_expires_at": "2026-09-09T00:05:00Z",
  "confirmed_at": null,
  "confirmation_generation": 1,
  "confirmation_round": 1,
  "next_probe_at": "2026-09-09T00:00:00Z",
  "observe_only": false,
  "paused_from": null,
  "paused_until": null,
  "paused": false
}
```

登记后 worker 的 confirmer 会向该 URL 发确认请求（带头 `X-Message-Type: activation_challenge`）：

```json
{
  "type": "activation_challenge",
  "destination_id": "6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c",
  "confirmation_round": 1,
  "challenge": "8Kx...一次性随机串"
}
```

接收方对上的两种方式：

```bash
# 1) 在确认请求的 2xx 响应体中原样回 echo（mock_receiver 默认就这样做）
#    -> {"type":"activation_response","echo":"8Kx...一次性随机串"}

# 2) 或由接收方主动回调（也可用 X-Confirmation-Challenge 头代替 body）
curl -s -X POST \
  http://localhost:8000/v1/destinations/6f0d6e1f-363e-42b6-9f08-d8b2e1b30d8c/confirm \
  -H 'Content-Type: application/json' \
  -d '{"challenge":"8Kx...一次性随机串"}'
```

对上后 `confirmation_state` 变 `confirmed`、`confirmed_at` 落时间，此后**新入库**的订阅事件才会扇出给它；回错 challenge 返回 400（`invalid`），这一轮超时没对上会自动换 challenge 开新一轮（对上迟到的旧 challenge 返回 410 `expired`，旧事件不补投）。握手过程可查：

```bash
curl -s http://localhost:8000/v1/destinations/<id>/confirmation-attempts  # 探测/应答/轮次过期记录
curl -s -X POST http://localhost:8000/v1/destinations/<id>/reissue-challenge  # 主动换新 challenge
```

重复登记同一 URL 是幂等的，返回同一个地址，**不会**重新握手；再次登记时：

- 传 `event_types` 列表 → 整体替换订阅集合（传 `[]` 表示退订全部）；
- 不传 `event_types` → 保持现有订阅不变。

换接收位置（URL）用 PATCH，会立即重新进入未确认并让代号加一：

```bash
curl -s -X PATCH http://localhost:8000/v1/destinations/<id> \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/new-webhook"}'
```

换位置后：没对上之前不会再往旧位置打；队列里还没打出的旧副本置为终态 `superseded`（不补投）；已经投妥或正在投的副本不收回。

### 提交事件

> 下面只列 body 字段。`POST /v1/events` 现在**必须**带来源签名头（`X-Source-Id` / `X-Signed-At` / `X-Signature`），完整签名请求见上方"登记事件来源并取得密钥"或 `scripts/push_event.py`；不签名会以 `source_unknown` / `bad_signature` 被拒。

```
body: {
    "event_type": "paid",
    "dedupe_key": "order-1001-paid",
    "payload": {
      "order_id": "1001",
      "event_type": "paid"
    }
  }
```

约定最早外发时间（可选；没到点不会打出去，到点后按各地址原队列顺序投）：

```
body（同样要配合签名头发送）: {
    "event_type": "paid",
    "dedupe_key": "order-1002-paid",
    "not_before": "2026-09-10T08:00:00Z",
    "payload": {
      "order_id": "1002",
      "event_type": "paid"
    }
  }
```

响应中包含：

- `id`：事件 ID
- `source_id`：把事件送进来的已登记来源（入口准入前的历史事件可能为 `null`）
- `event_type` / `dedupe_key` / `payload`：事件本体
- `not_before`：约定的最早外发时间（未定时为 `null`）；`cancelled_at`：取消时间（未取消为 `null`）
- `status`：`unrouted`（没有任何地址订该类型）、`pending`（至少一份活跃副本未投完）、`delivered`（全部活跃副本已收到传输层 2xx，但不一定已对上回执）、`cancelled`（未打出前已取消，剩余副本永不再投）、`superseded`（所有副本都因地址换位置而被取代，没有一份打出去，也不补投）、`dead_lettered`（所有活跃副本都已进死信处、且没有还在排队或投递中的副本；其中任一份被人工复活即回到 `pending`）
- `reconcile_status`：`pending`（还没有副本认）、`partially_acknowledged`（只认够一部分、尚未达到这笔的认完门槛）、`acknowledged`（当真副本认够了这笔的 `required_ack_count`——无门槛类型即所有订阅地址的副本都认了）
- `ack_threshold`：该事件类型**当前**配置的认完门槛（没定为 `null`）；`required_ack_count`：这笔在入库时快照下来的需要认的当真份数（定了门槛取门槛与当真订户数的较小值，没定等于扇出的当真份数）；`acknowledged_quorum`：是否已经认够（一旦为 true 不会再变回去）
- `delivery_count` / `delivered_count`：**当真**扇出副本总数 / 已投妥数
- `shadow_delivery_count` / `shadow_delivered_count`：只跟着看（`observe_only`）的副本总数 / 已投妥数；另有 `shadow_acknowledged_count` / `shadow_unacknowledged_count` / `shadow_pending_count` / `shadow_dead_lettered_count`。影子副本的任何结果都不改变下面的整笔状态
- `acknowledged_count` / `unacknowledged_count`：已对上成功回执的**当真**份数 / 还没对上的**当真**份数
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

### 给已收下的事件补一笔更正

```bash
# 另补一笔：只补给当初真正投妥过的地址，排在各地址队尾；原来那份不动
curl -s -X POST http://localhost:8000/v1/events/<event_id>/corrections \
  -H 'Content-Type: application/json' \
  -d '{"dedupe_key":"order-1001-paid-fix-1","payload":{"order_id":"1001","amount":12}}'

# 查这笔事件名下的全部更正（没有就是空列表，不会写成已经发出去）
curl -s http://localhost:8000/v1/events/<event_id>/corrections

# 更正本身是一笔事件（响应里 corrects_event_id 指向原来那笔），有自己的轨迹：
curl -s http://localhost:8000/v1/events/<correction_event_id>/trace
```

重复提交同一笔更正（同一个 `dedupe_key`）返回 `200` 和原更正并带 `duplicate: true`，只认一次；`dedupe_key` 被别的事件/更正占用返回 `409`。原来那笔没人订、一份都没打出去、已取消的返回 `409`（什么都不创建，也不占用这个键）；事件不存在返回 `404`。更正副本外发失败后不重试，置终态 `failed`，只算它自己——不隔离地址、不挡后面的副本、不改原来那笔已认的状态。

### 查询某条事件的投递过程

```bash
curl -s http://localhost:8000/v1/events/<event_id>/trace
```

返回事件当前传输状态和整笔对账状态（`reconcile_status`，以及已认/未认数量）、每个地址的副本（`deliveries`：状态、序号、已尝试次数、下次尝试时间、最近错误、对账状态 `reconcile_state`、对账时限 `reconcile_deadline`、回执结果 `receipt_result`、连续失败次数与死信原因/进入时间，以及 `observe_only` 是否只跟着看）、全部投递尝试（`attempts`：开始/结束时间、是否成功、HTTP 状态码、响应片段或错误信息；若某次调用是在租约丢失后返回的，会带 `lost_lease: true`）和该事件命中的回执（`receipts`）。部分地址已认时，整笔是 `partially_acknowledged`，不会写成已认完；逐个查看副本即可知道谁认了、谁还没认——其中 `observe_only: true` 的副本只是跟着看：它认了不会让整笔变 `acknowledged`，它没认/超时/进死信也不会把已经认完的整笔拖回去（影子计数单列在 `shadow_*` 字段）。没人订的事件在这里能看到 `status: "unrouted"` 且 `deliveries` 为空——是明确的“没送出去”，不是成功。所有活动副本都进了死信处、且没有还在排队/投递中的副本时，事件整体 `status` 为 `dead_lettered`。轨迹里另有 `corrections` 段，列出这笔事件名下的全部更正（每笔更正都是一个带 `corrects_event_id` 的普通事件响应，可再用它自己的 `/trace` 追查）；没有更正时为空列表——没人订或没打出去过的事件永远查不到更正，不会写成已经发出去。

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

# 查死信处：按原因汇总 / 列明细（哪份、哪个地址、为什么、什么时候进去的）
curl -s http://localhost:8000/v1/dead-letters/summary
curl -s 'http://localhost:8000/v1/dead-letters?destination_id=<destination_id>'

# 把某一份人工从死信处捞回原地址，保留原 destination_seq，按原顺序接着排
curl -s -X POST http://localhost:8000/v1/dead-letters/<delivery_id>/revive
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

### 给地址标一段不收的时间（暂停/恢复）

```bash
# 从现在开始停收到指定时刻（也可以两个时间都传，标一个将来的维护窗口）：
curl -s -X POST http://localhost:8000/v1/destinations/<destination_id>/pause \
  -H 'Content-Type: application/json' \
  -d '{"paused_until":"2026-09-11T02:00:00Z"}'

# 不限期停收（paused_from 不传 = 从现在开始；paused_until 不传 = 直到人工恢复）：
curl -s -X POST http://localhost:8000/v1/destinations/<destination_id>/pause \
  -H 'Content-Type: application/json' \
  -d '{"paused_from": null}'

# 提前恢复：清掉窗口，排队的副本立刻按原顺序接着投
curl -s -X POST http://localhost:8000/v1/destinations/<destination_id>/resume
```

窗口生效期间：该地址的副本在原队列位置等待、不插正在投的队；一次都不会真打（不算失败、不触发隔离）；副本的对账倒计时从窗口结束后真正投妥那一刻才开始；订了同一类型的其他地址不受影响。重复 `pause` 整体替换窗口；`resume` 幂等（无窗口时返回 `resumed: false`）。地址详情里的 `paused_from` / `paused_until` / `paused` 随时可查。

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

如果这是一笔**更正**（对某笔已收下事件另补的一笔），请求会额外带头 `X-Corrects-Event-Id: <原来那笔事件的 id>`，body 里同样带 `corrects_event_id` 字段；`Idempotency-Key` / `dedupe_key` 用的是更正自己的去重键。接收方可以据此把更正和原来那笔关联起来做业务修正；普通事件不带这个头和字段。

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

mock 接收方默认会在确认探测（`X-Message-Type: activation_challenge`）的 2xx 响应里原样回 echo，因此登记地址后它会被自动对上确认；加 `--no-auto-confirm` 可保持未确认状态（用确认接口手工对上，或观察轮次超时换新 challenge）。

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
| `FAILURE_THRESHOLD` | `5` | 同一地址连续传输失败多少次后隔离地址 |
| `QUARANTINE_SECONDS` | `900` | 自动隔离时长 |
| `MAX_DELIVERY_ATTEMPTS` | `10` | 同一份副本连续传输失败多少次后停止自动重试、进死信处（一份拿到 2xx 或人工复活后计数清零） |
| `MAX_REQUEUE_CYCLES` | `3` | 超时/失败回执的副本最多重投多少轮；之后仍超时或最后一轮收到失败回执即进死信处（worker / reconciler / ingest-api 三个服务必须取同一个值） |
| `MAX_RESPONSE_BODY_BYTES` | `2048` | 轨迹表保存响应体片段的最大长度 |
| `RECEIPT_TIMEOUT_SECONDS` | `300` | 回执对账时限：副本投妥后等待回执的约定时间，超时记为 `timed_out`（Worker 侧配置） |
| `RECONCILE_SWEEP_INTERVAL_SECONDS` | `5` | reconciler 扫描超时副本的间隔 |
| `RECEIPT_DELIVERY_GRACE_SECONDS` | `2` | 回执比投递结果先到时，回执接口等待 Worker 落库投递结果的宽限（API 侧配置） |
| `INGEST_MAX_AGE_SECONDS` | `300` | 入口签名时间戳最多允许比服务时钟旧多少秒，超出按 `stale_timestamp` 拒（防重放窗口） |
| `INGEST_MAX_FUTURE_SKEW_SECONDS` | `60` | 入口签名时间戳最多允许超前服务时钟多少秒，超出按 `future_timestamp` 拒 |
| `CONFIRM_TIMEOUT_SECONDS` | `300` | 一轮上线确认的时限：challenge 发出后多久内没对上就作废、开新一轮 |
| `CONFIRM_BACKOFF_BASE_SECONDS` | `2` | 确认探测失败后重试退避的起始间隔（指数退避） |
| `CONFIRM_BACKOFF_MAX_SECONDS` | `60` | 确认探测重试退避上限 |
| `CONFIRM_POLL_INTERVAL_SECONDS` | `1` | confirmer 线程没有待确认地址时的轮询间隔 |
| `CONFIRMATION_ENABLED` | `true` | worker 进程内是否运行发确认探测的 confirmer 线程；多 worker 副本时只保留一个为 true |

## 数据表概览

- `event_sources`：登记的外部事件来源、状态（`disabled_at` 为空即启用）、当前签名密钥与最近换钥时间。密钥只在登记/换钥的响应里明文出现一次。
- `ingestion_attempts`：入口准入日志，每次事件推送一行（含全部被拒的），带处置结果（`accepted` / `unrouted` / `pending_confirmation` / `duplicate` / `source_unknown` / `source_disabled` / `bad_signature` / `stale_timestamp` / `future_timestamp` / `invalid_timestamp` / `invalid_body`）、发送时间、拒因；被拒记录没有 `event_id`，不会在任何轨迹里显示成已收/已发。`pending_confirmation` 的事件有 `event_id`（确实收下了），但当时没有生成任何副本。
- `events`：事件本体（来源 `source_id`、类型、去重键、负载、最早外发时间 `not_before`、取消时间 `cancelled_at`，以及入库时快照的认完门槛 `required_ack_count`：无门槛类型等于扇出的当真份数，定了门槛取门槛与当真订户数的较小值，只有影子订户/无人订阅时为 0；老事件该列为 NULL，按"现存每一份当真副本都认"处理），一条事件一行，与地址无关。**更正也是一行事件**：`corrects_event_id` 指向被更正的那笔（普通事件该列为 NULL），它有自己的去重键和副本，原来那笔的行不会被改写。
- `event_type_ack_thresholds`：按事件类型配置的认完门槛（每个类型至多一行，`ack_threshold >= 1`）。改/删只影响之后入库的事件；事件自己的要求以 `events.required_ack_count` 的快照为准。
- `destination_subscriptions`：地址订阅的事件类型集合。
- `deliveries`：扇出后的每地址投递副本，含每地址顺序号、投递状态（`pending` / `in_flight` / `delivered` / `cancelled` / `superseded` / `dead_lettered` / `failed`——`failed` 只出现在更正副本上：外发失败一次即终态，不原地重试、不计地址连续失败、不挡后续副本）、下次尝试时间、最早外发时间 `not_before`、租约信息、对账状态（`reconcile_state`、对账时限、回执结果、重投次数）、**确认代号 `confirmation_generation`**、**只跟着看快照 `observe_only`（扇出时从地址复制；为 true 的副本照常外发/重试/隔离/死信/对账，但其回执结果从不改变整笔事件的 `reconcile_status`/传输状态，也不进事件级重投名单）** 和**死信信息（连续传输失败次数 `consecutive_failures`、死信原因 `dead_letter_reason`、进入时间 `dead_lettered_at`；原因取值为传输失败耗尽 `delivery_attempts_exhausted`、回执超时耗尽 `receipt_timeout_exhausted`、失败回执耗尽 `receipt_failure_exhausted`）**（换位置后老代号排队副本置 `superseded`，领取闸门也会挡住老代号副本）；Worker 只消费这张表且只领取 `pending`/`in_flight`，死信副本永不自动外发、也不挡后续副本。
- `delivery_attempts`：每次 HTTP 投递尝试的审计轨迹（关联事件与副本）。
- `confirmation_attempts`：上线握手轨迹，一行对应一次确认探测（`challenge`）、应答（`echo`，含回错的 `invalid`）或轮次过期（`expired`），带轮次号、HTTP 状态码、响应片段或错误信息。
- `receipts`：接收方回执日志，一条回执一行，含处置结果（`applied`/`duplicate`/`late`/`orphan`/`premature`）与匹配到的副本；重复、迟到、查无副本的回执都留在这里可查。
- `destinations`：接收地址、状态、连续失败次数、隔离可恢复时间、该地址下一个序号、是否只跟着看（`observe_only`）、停收窗口（`paused_from` / `paused_until`，皆空为无窗口；窗口内 Worker 不领取该地址，副本原地等、不计失败、不算对账），以及上线确认状态（`confirmation_state`、当前 challenge 与时限、确认时间、确认代号/轮次号、下次探测时间）。

## 从一对一模型升级

表结构通过 `init_db` 幂等迁移：旧的按地址存事件的 `events` 表会自动改名为 `deliveries`（未投完的行原样保留，Worker 会接着投），并新建逻辑事件表 `events` 与订阅表 `destination_subscriptions`。老数据里的历史投递没有对应的逻辑事件，其 `event_id` 为 `NULL`，不影响继续投递。

回执对账的列（`reconcile_state` 等）和 `receipts` 表同样以幂等方式补齐。升级前已投妥的历史副本 `reconcile_state` 为 `none`，不会要求补回执；如果接收方对这些老副本补发回执，仍会按 `applied` 正常对账。

上线握手相关的列（`confirmation_state`、`challenge_token`、`confirmation_generation` 等）和 `confirmation_attempts` 表也由 `init_db` 幂等补齐。**升级前已存在的接收地址一律视为 `confirmed`**（它们此前一直在收事件），队列里未投完的副本代号为第 1 代、与地址当前代号一致，会接着投；只有升级后新登记或换位置的地址才从 `pending` 开始。

## 生产化建议

当前 Compose 配置适合开发和小规模部署。上生产前建议补充：

1. API 前增加负载均衡器和 HTTPS。
2. 为登记地址、**来源登记/停用/换钥**、提交事件之外的管理接口增加管理员鉴权（事件入口本身已要求来源密钥签名）；生产环境建议对 `event_sources.secret` 做静态加密（KMS/信封加密），而不是明文落库，并将签名比较保持为常量时间。
3. 使用托管 PostgreSQL，设置备份、连接数上限和慢查询监控。
4. 增加 Prometheus 指标：待投递数、投递延迟、失败率、隔离地址数、Worker 心跳、超时未对账副本数、迟到回执数。
5. 将 `CREATE TABLE IF NOT EXISTS` 替换为 Alembic 迁移，便于后续表结构变更。
