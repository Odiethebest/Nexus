# MIGRATION.md — RabbitMQ → Redpanda (Kafka 协议) 重构看板

## Overview

**目标**:把 Nexus 的消息层从 RabbitMQ 一次性切换到 Redpanda(Kafka 协议兼容),并让整份代码严格对齐简历 4 条 bullet——每条都能通过跑命令 + 看 Prometheus/Grafana 指标现场复现。

### 已拍板的三个决定

1. **Kafka 发行版**:本地用 Redpanda(KRaft、单节点);Railway 用 Redpanda Cloud dev cluster;集成测试用 `testcontainers-go` 的 redpanda 模块。代码 / 文档 / commit message 里都如实称为 "Redpanda(Kafka 协议兼容)",不伪装成原生 Apache Kafka。
2. **Cache-aside**:主缓存路径是「按 `message_id` 单条读」——天然热点、重复读,命中率是真实负载特性。不用短 TTL 列表缓存去人为凑命中率。
3. **一次性切到 Kafka**,删掉所有 AMQP 代码,不保留 dual-mode。

### 相对原 11 步计划的四条修正

- **A. Cache-aside 读路径**:新增 `GET /notifications/{message_id}`(Redis key `cache:notif:{id}`,TTL 60s)作为主缓存路径。列表接口可选加短 TTL 缓存,但**不作为命中率主要来源**。loadtest 的读流量主要打单条接口。指标 `nexus_cache_hits_total{scope}` / `nexus_cache_misses_total{scope}`,scope ∈ {`by_id`, `list`},面试口径只讲 `by_id`。
- **B. Consumer lag 双指标**:同时暴露 offset lag(gauge)和事件年龄 histogram(秒)。前者用于 Kafka 视角的 backlog,后者对应简历 "lag < 1.5s"。
  - `nexus_consumer_lag_records{channel,priority}` — gauge
  - `nexus_event_e2e_lag_seconds` — histogram(`now − x-produced-at`)
- **C. 50K/s 不硬凑**:本机 demo 模式打到实际能达到的上限(预计 10–20K/s),k6 Cloud real 模式目标 50K/s。RUNBOOK 里如实记录本机实测值,并明确 50K 是 k6 Cloud 目标,任何文档都不把本机测不到的数写成"已达成"。
- **D. Partition 数写成可辩护的推导**:README 保留原文推导——"目标吞吐 / 单 partition 吞吐(经验值 5–10K msg/s)+ 约 30% 余量 → 12 partition 覆盖 50K/s"。不拍脑袋。

### 执行规则

- 严格按下面 11 步顺序,每步一个 commit,commit message 写清"做了什么 + 为什么"。
- 每完成一步,把对应 `- [ ]` 改成 `- [x]`,并在该步下补一行「实际产出 / 与计划的差异」,该修改和当步代码同一个 commit。
- 中途需要调整计划,先改 MIGRATION.md 说明原因,再动代码。
- 全部完成后,末尾「bullet → 状态」表要全部标成可复现,填上 RUNBOOK 的真实实测数字。
- 保留这些接口契约不变(前端 / gRPC 零改动):`Publisher.Publish(ctx, type, priority, payload)`、`Event` JSON 结构、`Replayer.Replay(ctx, target, max)`、`idempotency.Client.Check`、`store.Store`。
- 保留高/中/低优先级 lane 独立性(高优不被低优阻塞)、Redis SETNX 幂等、`(message_id, channel)` PG upsert、retry-with-backoff、DLQ、SIGTERM graceful shutdown。
- Producer:`acks=all` + 幂等 producer + 异步批量 Produce(**不要**每条 wait confirm)。record key = `message_id`。Headers:`x-msg-id` / `x-event-type` / `x-priority` / `x-produced-at`(纳秒)/ `x-retry-count`。
- 不引入 CGO 依赖;所有库版本写进 go.mod;每步保持可编译可运行。

---

## Checklist

### Step 1 — 引入 franz-go,占位新包(暂不删旧)

- [x] 完成

**实际产出 / 与计划的差异**
- 新增 `internal/kbroker/`:`topics.go`(topic 命名 + `NormalizeDLQTopic` 兼容老 AMQP 形式 `nexus.<ch>.dlq.<p>` → `nexus.dlq.<ch>.<p>`)、`event.go`(`Event` 结构 + 5 个 header 常量)、`config.go`(env 解析,SASL/TLS 可选)、`admin.go`(`EnsureTopics` 幂等建 topic)、`topics_test.go`(拼接 + group 唯一性 + 兼容映射测试)。
- `go get` 顺带把 go.mod 从 1.22 提到 1.25(franz-go v1.21.5 最低要求);顺带升级 klauspost/compress、golang.org/x/{crypto,net,sync,sys,text},无 CGO。
- 无 breaking change:AMQP 路径完全未动;`go build ./...` 通过,`go test ./internal/kbroker/...` 通过。

**改动**
- `go.mod` / `go.sum`:`go get github.com/twmb/franz-go`、`github.com/twmb/franz-go/pkg/kadm`、`github.com/twmb/franz-go/pkg/kgo`。
- 新增 `internal/kbroker/`(与旧 `internal/broker/` 并存):
  - `client.go`:`Client` 包装 `*kgo.Client`,构造从 env 读 `KAFKA_BROKERS`。
  - `topics.go`:`TopicName(channel, priority)` = `nexus.<channel>.<priority>`,`DLQTopic(channel, priority)` = `nexus.dlq.<channel>.<priority>`;`AllTopics()` 用于启动时 auto-create。
  - `admin.go`:用 `kadm.Client` 在启动时确保 topic 存在(partition 数从 env `KAFKA_TOPIC_PARTITIONS` 读,默认 12;`KAFKA_REPLICATION_FACTOR` 默认 1)。
- 新增 env(`.env.example` 补齐):`KAFKA_BROKERS`、`KAFKA_TOPIC_PARTITIONS`、`KAFKA_REPLICATION_FACTOR`、`KAFKA_CLIENT_ID`。

**验收**
- `go build ./...` 通过。
- 有单元测试证明 `TopicName` / `DLQTopic` 拼接正确。
- 不改动任何现有 AMQP 代码路径(生产者/消费者仍走 RabbitMQ)。

**关联 bullet**:#1(为后续 Kafka pipeline 打底)。

---

### Step 2 — Kafka 版 Publisher(异步 + acks=all + 幂等)

- [x] 完成

**实际产出 / 与计划的差异**
- 新增 `internal/kbroker/publisher.go`:签名 `Publish(ctx, eventType, priority, payload) (msgID, error)` 与旧 AMQP 完全一致。franz-go client 选项:`AllISRAcks` + `StickyKeyPartitioner`(record key=`message_id`)+ `MaxBufferedRecords(200_000)` + `ProducerBatchMaxBytes(1 MiB)`。幂等 producer 是 franz-go 默认。
- 每次 `Publish` **fan-out 到全部 3 个 channel 的 lane topic**——精确复现 AMQP 的 `event.*.<priority>` binding 语义(以前 email/inapp/webhook 三个 queue 都绑定 `event.*.<priority>`,所以每个事件都进三个队列)。这是相对原计划的一处澄清:计划里没写 fan-out 语义,实现里必须体现。
- Headers 5 个(`x-msg-id / x-event-type / x-priority / x-produced-at 纳秒 / x-retry-count=0`)。async `client.Produce` 回调收集 error → `Publish` 直到全部 ack 才返回,保留 HTTP 同步返回 msg_id 的调用契约。
- `internal/metrics/metrics.go`:新增 `nexus_stage_ingest_duration_seconds{channel,priority}` histogram 和 `nexus_events_published_total{channel,priority}` counter;把旧 `nexus_publish_duration_seconds` / `nexus_worker_process_duration_seconds` 标 Deprecated。
- `cmd/producer/main.go`:加 `USE_KAFKA` env flag(默认 false,继续走 AMQP)。true 时:调 `kbroker.EnsureTopics` 建 topic → 起 `kbroker.NewPublisher` → 用它替换 amqp `broker.Publisher`;replay handler 暂时返回 503(Step 5 才实现 Kafka replayer)。graceful shutdown 时会 flush + close kafka client。
- `internal/grpcserver/server.go`:抽出 `Publisher` interface,`NewEventServer` / `Listen` 改吃 interface,AMQP 和 Kafka 两种 publisher 都能塞进去。
- `.env.example` 加 `USE_KAFKA` 说明(实际值先放默认 false 到 Step 3 打通)。
- `go build ./... && go test ./...` 全绿(集成测试有 `//go:build integration` tag,Step 7 处理)。

**改动**
- 新增 `internal/kbroker/publisher.go`,同签名 `Publish(ctx, eventType, priority, payload) (msgID string, err error)`。
- `kgo.Client` 选项:`kgo.RequiredAcks(kgo.AllISRAcks())`、`kgo.ProducerBatchMaxBytes` 调大、`kgo.MaxBufferedRecords`、`kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil))`、幂等 producer(默认开)。
- 每条 record:key = `message_id`(UUIDv4);value = 现有 `broker.Event` JSON 原样序列化(**不改结构**);headers 填 `x-msg-id`、`x-event-type`、`x-priority`、`x-produced-at`(纳秒 int64 十进制)、`x-retry-count=0`。
- 使用异步 `client.Produce(ctx, rec, cb)`,回调里做 promise / error 汇总;HTTP handler 里通过 `chan error` 拿到结果并返回 msg_id(保留前端契约:同步返回 msg_id)。
- 新指标 `nexus_stage_ingest_duration_seconds`(histogram)。**移除旧的 `nexus_publish_duration_seconds`** 名义混淆的写法,或保留但打标签说明。
- `cmd/producer/main.go` 加一个 feature flag `USE_KAFKA=true|false`,`true` 时用 `kbroker.Publisher`,`false` 时用旧 AMQP。默认先给 `false`。

**验收**
- 本地起 Redpanda(docker run 一发即可)后,把 `USE_KAFKA=true` 打开,`POST /events` 能成功返回 msg_id,能用 `rpk topic consume nexus.email.high` 看到消息(key=msg_id,headers 齐全)。
- `nexus_stage_ingest_duration_seconds_count` 有数据。

**关联 bullet**:#1、#4。

---

### Step 3 — Worker 消费重写(3 lane × consumer group)

- [x] 完成

**实际产出 / 与计划的差异**
- 新增 `internal/kworker/` 包:
  - `runner.go`:通用 `Runner`,消费一个 (channel, priority) lane。用 franz-go `PollFetches` + 一个 `sem chan struct{}` 做 bounded 并发,每条 record 一个 goroutine。`DisableAutoCommit` + `BlockRebalanceOnPoll` + 手动 `CommitRecords`——**处理成功才 commit,即 at-least-once**。`AllowRebalance()` 在每轮 poll 结束后放开。
  - `processors.go`:`EmailProcessor` / `InAppProcessor` / `WebhookProcessor` 三个 `Processor` 实现。返回 `Outcome`(Delivered / Skipped / TransientError / PermanentError),让 runner 统一处理 retry vs DLQ。Webhook 的 500/429 → transient;其他 4xx → permanent(不重试)。
  - `republisher.go`:`KafkaRepublisher` 独立 producer client,负责 retry re-produce 和 DLQ produce。`cloneWithRetry` **保留 `x-produced-at` 原值**——retries 不会重置 e2e lag 时钟(这是简历 "lag<1.5s" 口径的正确性关键)。
  - `republisher_test.go`:验证 clone 保留 produced-at、更新 retry count、pool split `[pool, pool/2, pool/4]` 对齐旧 AMQP QoS。
- `internal/metrics/metrics.go` 新增 5 个指标:
  - `nexus_stage_processing_duration_seconds{channel}` — 三阶段中的 processing;
  - `nexus_stage_delivery_duration_seconds{channel}` — 三阶段中的 delivery;
  - `nexus_event_e2e_lag_seconds{channel}` — histogram,由 runner 用 `now - x-produced-at` 更新;简历 "lag<1.5s" 就看这个。
  - `nexus_consumer_lag_records{channel,priority}` + `nexus_dlq_messages_total{channel,priority}` — 空 gauge,由 Step 4 填数据。
- `cmd/worker/main.go`:`USE_KAFKA=true` 时,起 9 个 lane runner(3 channels × 3 priorities),每个独立 consumer group id(`nexus.<channel>.<priority>`);跟原计划的"独立 group"选择一致。SIGTERM → `signal.NotifyContext` → 各 runner `Run` 退出前 `Wait` in-flight + `CommitUncommittedOffsets` → republisher `Flush + Close` → 主 goroutine 退出。等价 graceful shutdown。
- Retry 次数从原来 AMQP `x-death` 计数器改成消息 header `x-retry-count`;`MaxRetries=3`,指数退避 2s/4s/8s(与旧 webhook worker 一致)。
- 差异:计划里写"每 channel 一个 group 订阅 3 topic",实测更清晰的方案是"每 lane 独立 group"(我们之前也选了这条),已按此实现。
- `go build ./... && go test ./...` 全绿。

**改动**
- 新增 `internal/kworker/{email,inapp,webhook}.go`(或就地改 `internal/worker/`,但保留旧文件到 Step 7 一次性删);每个 worker 结构:
  - 三个独立 `*kgo.Client`,分别订阅 `nexus.<channel>.high` / `.normal` / `.low`(三个 consumer group id:`nexus.<channel>.high` 等,或者用同一个 group + 独立 client 区分——用独立 group 更清晰)。**决定用独立 group**:高优 lane 挂了不影响其他 lane 的 committed offset。
  - 各 lane 的 goroutine 池大小 `[pool, pool/2, pool/4]` 对应现有 `EMAIL_WORKER_POOL` 等 env。
  - 消息处理:
    1. 从 headers 取 `x-msg-id` → `idempotency.Check`;
    2. 派发(SMTP / hub broadcast / HTTP POST);
    3. 成功 → `store.SaveNotification` + `client.CommitRecords`;
    4. 瞬时失败 + `x-retry-count ≤ 3` → 递增 header re-produce 到同 topic + commit offset;
    5. `> 3` 或持久失败 → produce 到 DLQ topic + commit offset。
  - 埋点:`nexus_stage_processing_duration_seconds{channel}`、`nexus_stage_delivery_duration_seconds{channel}`、`nexus_event_e2e_lag_seconds`(`now - x-produced-at`)。
- Graceful shutdown:SIGTERM → 三个 client `PauseFetchTopics` → 等 in-flight → `CommitUncommittedOffsets` → `Close`。
- `cmd/worker/main.go` 在 `USE_KAFKA=true` 时启动 Kafka worker,`false` 时启动旧 worker(临时并存)。

**验收**
- `USE_KAFKA=true` 下 producer + worker 全链路跑通:发一条 `payment.completed` (high) → worker 派发 → PG 有记录 → WS 前端收到。
- SIGTERM 时 lag 曲线不会突然跳高(offset 有正常提交)。
- 故意让 webhook 目标返回 500,能看到 3 次重试后进 DLQ topic。

**关联 bullet**:#1、#3、#4。

---

### Step 4 — 真实 consumer lag / DLQ 指标 + 修复 metrics summary

- [x] 完成

**实际产出 / 与计划的差异**
- 新增 `internal/kbroker/lag.go`:`LagReader` 用 `kadm.Client` 每 3s 采样:
  - 每 lane 的 (end offset − committed offset) → `nexus_consumer_lag_records{channel,priority}` gauge;
  - 每 DLQ topic 的 end offset → `nexus_dlq_messages_total{channel,priority}` gauge。
  - 独立 kgo client 避免和 publisher 的关闭顺序耦合。`kerr.GroupIDNotFound` 静默(新集群 group 还没提交过 offset 时正常)。
- `cmd/producer/main.go`:USE_KAFKA=true 时起后台 goroutine 跑 `LagReader.Run`。用 `rootCtx` 控制生命周期,SIGTERM 时 `rootCancel()` 结束采样。差异:计划里没细化 lag reader 跑在哪个服务;这里放在 producer 是因为 summary handler 也在 producer,读本地 Prometheus registry 就能拿数据,不用跨服务 scrape。
- `internal/metrics/summary.go` 重写为:
  - 同时 gather 本地 registry(producer 侧计数)+ remote worker scrape,`mergeMetricFamilies` 本地覆盖同名。
  - `PublishRatePerSec` 改用 `nexus_events_published_total`(修复原来"publish 用 processed 数硬凑"的名义混淆);新增 `ProcessedRatePerSec` 字段独立呈现。
  - `QueueDepth` 从 `nexus_consumer_lag_records` gauge 读取(替换硬编码 0);缺失 lane 会 backfill 0 保持字段稳定。
  - `DLQCount` 从 `nexus_dlq_messages_total` 汇总(替换硬编码 0)。
  - 新增 `E2ELagP99Seconds`:从 `nexus_event_e2e_lag_seconds` histogram 取 p99。**这是简历 "lag < 1.5s" 直接对应的字段**。
- `internal/metrics/summary_test.go`:测 mergeMetricFamilies 冲突时 local 胜出 + histP99Sec 插值。
- `go build ./... && go test ./...` 全绿。

**改动**
- 新增 `internal/kbroker/lag.go`:后台 goroutine(3s 周期),用 `kadm.Client` 拉:
  - 每个 lane topic 的 end offset − 该 lane consumer group 的 committed offset → `nexus_consumer_lag_records{channel,priority}`;
  - 每个 DLQ topic 的 end offset(近似 DLQ 消息数)→ `nexus_dlq_messages_total{channel,priority}` gauge。
- 已在 Step 3 埋 `nexus_event_e2e_lag_seconds`(histogram)。
- 修 `internal/metrics/summary.go`:
  - `QueueDepth` 用 lag gauge 汇总(替换硬编码 0);
  - `DLQCount` 用 DLQ gauge 汇总(替换硬编码 0);
  - 拆开 publish rate 和 processed rate:新增 `nexus_events_published_total` 计数器由 Step 2 publisher 更新,`publish_rate_per_sec` 从这个算;`processed_rate_per_sec` 从原有的 `nexus_messages_processed_total` 算。summary JSON 同时暴露两者。
- 前端 `types/index.ts` + `useMetrics` 如涉及新增字段同步跟进(保持字段可选以免破坏兼容)。

**验收**
- `curl /api/metrics/summary` 里 `queue_depth` 和 `dlq_count` 是真实数字(不再是 0)。
- Prometheus 里能看到 `nexus_consumer_lag_records` 与 `nexus_event_e2e_lag_seconds_bucket`。
- 压测时能看到 lag 上升 / 处理完后回落。

**关联 bullet**:#1(lag<1.5s)、#4(三阶段追踪)。

---

### Step 5 — Replay 适配(DLQ topic → 主 topic 重发)

- [ ] 完成

**改动**
- 重写 `internal/replay/replay.go`:
  - `Replayer.Replay(ctx, target, max)` 签名不变;`target` 可传新式 `nexus.dlq.email.high` 或老式 `nexus.email.dlq.high`,handler 层做归一化。
  - 用一次性 `kgo.Client`(`kgo.ConsumeTopics(dlqTopic)` + `kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())` + `kgo.DisableAutoCommit()`),poll 到 max 条为止。
  - 每条从 headers 恢复 `x-event-type` / `x-priority`,通过 `kbroker.Publisher` 重发到 `nexus.<channel>.<priority>`(把 `x-retry-count` 重置为 0)。
  - 处理完手动 `CommitOffsets` 到 poll 到的位置(避免重复 replay)。
- `POST /dlq/replay` handler 保持 body `{queue, max}` 语义,内部把 `queue` 当 DLQ topic 名处理。

**验收**
- 故意造几条 DLQ 消息 → `POST /dlq/replay {"queue":"nexus.dlq.webhook.normal","max":10}` → 消息重新出现在 `nexus.webhook.normal` → worker 成功处理 → DLQ gauge 下降。
- 兼容老前端调用形式 `nexus.webhook.dlq.normal`(如前端在用)。

**关联 bullet**:#3(DLQ 处理)。

---

### Step 6 — Cache-aside 读路径(by_id 主 + list 短 TTL 辅)

- [ ] 完成

**改动**
- 新增 handler `GET /notifications/{message_id}`(cmd/producer/main.go 注册路由):
  - 先 `GET cache:notif:{id}`(命中 → `nexus_cache_hits_total{scope="by_id"}` + 返回);
  - miss → PG 查(所有 channel 的 rows 打包返回,或返回按 channel 分组的对象);回填 Redis TTL 60s。miss 计数 `nexus_cache_misses_total{scope="by_id"}`。
- `GET /notifications` 列表:key `cache:notif:list:v1`,TTL 2s,埋 `scope="list"` 指标。这是工程实现,不作为主命中率。
- 新指标定义在 `internal/metrics/metrics.go`。
- 前端 `types/index.ts` / `useNotifications` 如需新增单条读能力,补一个 `useNotification(id)` hook(可选,不阻塞主流程)。
- Loadtest client 增加读流量:每 N 条 publish 顺带打一次 `GET /notifications/{msg_id}`(N 可配,默认让 read/write ≈ 2:1,能压出 95% 命中率——因为 msg_id 集合小、TTL 60s、复读多)。

**验收**
- 手动打 100 次同一 id → 至少 99 次命中(Prometheus `rate(nexus_cache_hits_total{scope="by_id"}[1m]) / (rate(hits) + rate(misses))` ≥ 0.95)。
- Loadtest 中 `by_id` 命中率能稳定在 ≥ 95%(RUNBOOK 记录实测)。

**关联 bullet**:#2。

---

### Step 7 — 删除旧 AMQP 代码 + 依赖

- [ ] 完成

**改动**
- 删 `internal/broker/{connection,priority,publisher}.go`(publisher.go 里的 `Event` 类型迁到 `internal/kbroker/event.go`,保持 JSON 字段完全一致)。
- 删 `internal/worker/{email,inapp,webhook}.go` 旧 AMQP 版本(如 Step 3 是并存实现,现改为唯一实现)。
- 删 `USE_KAFKA` feature flag(Kafka 是唯一路径)。
- `go.mod`:移除 `github.com/rabbitmq/amqp091-go`、`testcontainers/modules/rabbitmq`。
- 集成测试 `internal/integration/pipeline_test.go` 迁到 `testcontainers-go/modules/redpanda`。
- `cmd/producer/main.go` / `cmd/worker/main.go` 清掉旧路径,只留 Kafka 启动逻辑。

**验收**
- `grep -r amqp . --include='*.go'` 无结果。
- `go build ./... && go test ./...` 全绿。
- 集成测试跑通(Redpanda testcontainer 起来)。

**关联 bullet**:#1(全链路只走 Kafka)、#3。

---

### Step 8 — Docker Compose 换 Redpanda

- [ ] 完成

**改动**
- `deploy/docker-compose.yml`:
  - 移除 `rabbitmq` service;
  - 加 `redpanda`(镜像 `docker.redpanda.com/redpandadata/redpanda:latest`,单节点 KRaft,`--smp 1 --memory 1G --overprovisioned --node-id 0 --check=false --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092`);
  - producer / worker 环境变量 `KAFKA_BROKERS=redpanda:9092`;
  - Prometheus scrape 目标不变(worker :9091, producer :8080);可选加 redpanda `/metrics`。
- 更新 `deploy/prometheus.yml` 若需要。
- 新增 Grafana 面板 JSON(`deploy/grafana/dashboards/nexus-kafka.json`):Consumer Lag(records + seconds)、DLQ Count、Cache Hit Rate、三阶段 P50/P95/P99 面板。

**验收**
- `docker compose -f deploy/docker-compose.yml up -d` 全部起来无 restart loop。
- `rpk cluster info -X brokers=localhost:19092` 能连上。
- Grafana 3000 面板能开,面板上四张图有数。

**关联 bullet**:#1、#2、#3、#4(整体可运行)。

---

### Step 9 — Railway + Redpanda Cloud 部署配置

- [ ] 完成

**改动**
- `deploy/railway.toml` / `deploy/railway.worker.toml`:加环境变量占位 `KAFKA_BROKERS`、`KAFKA_SASL_USER`、`KAFKA_SASL_PASS`、`KAFKA_TLS=true`;Publisher / kbroker 支持 SASL_SSL(franz-go 用 `kgo.SASL(scram.Auth{...}.AsSha256Mechanism())` + `kgo.DialTLSConfig(...)`)。
- 零停机滚动:Railway 默认 rolling deploy;consumer group 自动 rebalance。README + RUNBOOK 写清验证步骤(新实例 up → 老实例 SIGTERM → 老实例 drain → 老实例退出;期间 `nexus_event_e2e_lag_seconds` p99 不应飙升超过若干 s)。
- README 里说明如何在 Redpanda Cloud 建 dev cluster、拿 broker URL / SASL 凭据、灌到 Railway env。

**验收**
- 本地能用 `KAFKA_BROKERS=<cloud-host>:9092 KAFKA_SASL_*=... KAFKA_TLS=true go run ./cmd/producer` 连上 Redpanda Cloud dev cluster 发一条消息。
- Railway config 语法正确(`railway up --detach` 能提交,不实际部署也可)。

**关联 bullet**:#4(Railway 零停机)。

---

### Step 10 — Loadtest 更新

- [ ] 完成

**改动**
- `internal/loadtest/client.go`:
  - 用 franz-go 直连 Kafka 或走 HTTP publish 都可;为了测端到端 pipeline,继续走 `POST /events`(测的是完整链路,不是 Kafka 裸吞吐)。
  - 加读流量:发 `POST` 后按比例调用 `GET /notifications/{msg_id}`,比例 env `LOADTEST_READ_RATIO`(默认 2.0 = 每写 1 次读 2 次,把 by_id 命中率推上去)。
  - demo 模式 pacing 目标:本机推到极限(先不设死上限,以 5s 滑窗测出的实际 RPS 为准);real 模式 target 50K/s 保留(需要 k6 Cloud)。
- `internal/loadtest/demo.go`:去除任何暗中限流,记录实测 P99 / 端到端延迟。
- `internal/loadtest/service.go` 汇总 metric 输出加上:实测 RPS、P99、平均 e2e lag、by_id 命中率。

**验收**
- 本地跑一遍 demo(约 55s),得到实际 RPS / P99 / lag / 命中率数值,填入 RUNBOOK 表格。
- Grafana 面板能看到压测过程中的曲线。

**关联 bullet**:#1、#2。

---

### Step 11 — 文档(README、RUNBOOK、CLAUDE.md)

- [ ] 完成

**改动**
- `README.md`:
  - 架构章节换成 Redpanda(Kafka 协议)+ mermaid 图(producer → topics × lanes → worker groups → PG/Redis;WS/Hub 保持;DLQ topics);
  - 加"partition 数怎么定"章节,保留推导原文(修正 D);
  - 本地跑法(docker compose、`rpk topic list`、灌数据、观察 lag);
  - Railway + Redpanda Cloud 部署说明;
  - 明确 Redpanda 是 Kafka 协议兼容,选它的原因(单二进制、KRaft、启动快)。
- 新增 `RUNBOOK.md`:每条简历 bullet 一行,列出「代码位置 → 复现命令 → 要看的 Prometheus 指标 / Grafana 面板」。表格里填 Step 10 跑出的**真实数字**,不留占位符。
- `CLAUDE.md` §2 / §3 / §5 / §7 同步:AMQP → Kafka;新增 `/notifications/{id}` 接口;env 表更新;deployment 表更新。

**验收**
- README 里的 mermaid 图渲染正确;命令能复制粘贴跑。
- RUNBOOK 里每条 bullet 的复现命令我亲手跑过一遍;数字对得上。
- CLAUDE.md 无 AMQP 残留。

**关联 bullet**:#1、#2、#3、#4(可复现)。

---

## 简历 Bullet → 状态

| # | Bullet | 关联步骤 | 状态 | 实测数字 / 面板 |
|---|---|---|---|---|
| 1 | Go + Kafka message-driven pipeline;50K/s 目标(k6 Cloud);p99<50ms;lag<1.5s;partition tuning + consumer group scaling | Step 1–4, 8, 10, 11 | ⏳ 未开始 | 本机 RPS: — / p99: — / e2e lag p99: — s |
| 2 | Redis cache-aside + PostgreSQL 持久化;负载下 by_id 命中率 95% | Step 6, 10, 11 | ⏳ 未开始 | 命中率 by_id: — |
| 3 | 幂等消费 + retry-with-backoff + DLQ + graceful shutdown = at-least-once | Step 3, 5, 7 | ⏳ 未开始 | DLQ 演示: — / rebalance drain: — |
| 4 | 端到端三阶段延迟追踪(ingest / processing / delivery)+ Railway 零停机滚动 | Step 2, 3, 4, 9, 11 | ⏳ 未开始 | Grafana 面板: — |

> 状态图例:⏳ 未开始 / 🚧 进行中 / ✅ 可复现
