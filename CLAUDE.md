# CLAUDE.md — Nexus Project

> 本文档是 Nexus 项目的全局规范文件，供 AI 辅助开发和人工协作时统一参照。
> 修改任何子系统设计前，请先更新此文档。

---

## 1. 项目概述

**Nexus** 是一个生产级消息驱动通知系统，展示高并发后端架构、多通道分发、实时监控与现代前端工程能力。

**核心定位（Portfolio）**：面向国内大厂（ByteDance、Alibaba、Baidu）Agent/后端开发岗位，重点展示：
- 消息队列 + 优先队列设计
- Redis 幂等去重机制
- 死信队列 + 手动重放
- WebSocket 实时广播
- Prometheus 全链路埋点
- 全栈部署能力（Railway）

**技术栈总览**

| 层次 | 技术 |
|---|---|
| 后端语言 | Go 1.22 |
| 消息队列 | RabbitMQ (AMQP 0.9.1) |
| 数据库 | PostgreSQL 16 |
| 缓存 / 幂等 | Redis |
| 实时通信 | WebSocket (Gorilla) |
| API | HTTP REST + gRPC |
| 监控 | Prometheus（`/api/metrics/summary` 待实现） |
| 前端 | Next.js 15 (App Router) + TypeScript |
| UI 组件库 | shadcn/ui + Tailwind CSS |
| 部署 | Railway（producer + worker 已部署，web service 待创建） |

---

## 2. 后端架构规范

### 2.1 服务拆分

**Producer** (`cmd/producer`，port `8080` / gRPC `50051`)
- 职责：事件接收、HTTP/gRPC API、WebSocket Hub 管理、压测编排
- 不做任何消息消费逻辑

**Worker** (`cmd/worker`，metrics port `9091`)
- 职责：从 RabbitMQ 消费，通过三种渠道分发，写入 PostgreSQL
- 三类 Worker：EmailWorker (pool=10)、InAppWorker (pool=5)、WebhookWorker (pool=8)
- 不对外暴露 HTTP（仅 Prometheus 抓取 `:9091/metrics`）

### 2.2 消息流

```
POST /events
    │
    ▼
Publisher → Exchange "nexus.events" (topic)
            routing key: event.{type}.{priority}
            │
    ┌───────┼───────┐
    ▼       ▼       ▼
 Email   InApp  Webhook
 队列    队列    队列
(×3 优先级: high / normal / low)
    │
    ▼
Worker 处理流程：
  1. Redis 幂等检查 (key=msg:{id}, TTL=24h)
  2. 实际分发 (SMTP / WebSocket 广播 / HTTP POST)
  3. Upsert → PostgreSQL notifications 表
  4. Ack（成功）或 Nack→DLQ（失败）
```

### 2.3 关键设计决策（面试可讲）

| 设计 | 原因 |
|---|---|
| 三级优先队列 | 高优先级消息（支付、告警）不被低优先级任务阻塞 |
| Redis 幂等去重 | 防止 Worker 重启后重复消费造成重复通知 |
| DLQ + 手动重放 | 失败消息不丢失，支持运维介入后选择性重试 |
| WebSocket 非阻塞广播 | 慢客户端直接丢弃，不影响其他订阅者延迟 |
| Prometheus 全路径埋点 | 发布延迟、处理耗时、队列深度均可观测 |
| 压测 Demo 模式 | 不依赖 k6 云配额也能在 Demo 中展示真实吞吐曲线 |

### 2.4 REST API 完整列表

| Method | Path | 描述 |
|---|---|---|
| POST | `/events` | 发布新事件 |
| GET | `/notifications` | 查询通知列表（固定 limit=50，**暂无过滤/分页**） |
| POST | `/notifications/clear` | 清空所有通知记录 |
| GET | `/ws` | WebSocket 升级端点 |
| GET | `/api/metrics/summary` | 聚合指标摘要（**待实现**，见 §2.5） |
| POST | `/ops/loadtest/start` | 触发压测（真实 / Demo 模式） |
| GET | `/ops/loadtest/{run_id}` | 按 run_id 查询压测结果 |
| GET | `/ops/loadtest/latest` | 查询最新一次压测结果 |
| POST | `/dlq/replay` | 手动重放 DLQ 消息 |

### 2.5 `/api/metrics/summary` 响应规范

```json
{
  "publish_rate_per_sec": 142.3,
  "processing_latency_p99_ms": 38.1,
  "queue_depth": {
    "email_high": 0,
    "email_normal": 12,
    "email_low": 45,
    "inapp_high": 0,
    "inapp_normal": 8,
    "inapp_low": 20,
    "webhook_high": 2,
    "webhook_normal": 5,
    "webhook_low": 11
  },
  "delivery_success_rate": 0.986,
  "dlq_count": 3,
  "active_ws_connections": 7,
  "uptime_seconds": 84732
}
```

> 该端点每 5 秒由前端 `useMetrics` hook 轮询，不走 WebSocket。

### 2.6 WebSocket 消息格式

服务端广播的是原始 `broker.Event` JSON（InAppWorker 直接转发，不做字段映射）：

```json
{
  "message_id": "uuid",
  "type": "payment.completed",
  "priority": "high",
  "payload": { ... },
  "timestamp": "2026-04-05T10:00:00Z"
}
```

> **注意**：字段名是 `type`（非 `event_type`）、`timestamp`（非 `created_at`），且不含 `channel` 和 `status`。前端 `types/index.ts` 的 `WsEvent` 类型需与此对齐。

---

## 3. 数据库规范

### 3.1 Schema

```sql
CREATE TABLE notifications (
  message_id  TEXT        NOT NULL,
  channel     TEXT        NOT NULL,  -- 'email' | 'inapp' | 'webhook'
  event_type  TEXT        NOT NULL,
  status      TEXT        NOT NULL,  -- 'delivered' | 'failed' | 'duplicate' | 'dlq'
  payload     JSONB,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (message_id, channel)
);

-- 实际只有一个索引
CREATE INDEX notifications_created_at_idx ON notifications(created_at DESC);
```

> **待办**：`priority` 字段尚未加入 schema，后续前端需要按优先级展示时需补 migration。status / channel 索引待加。

### 3.2 查询规范

- 列表查询 hardcode `limit=50`，无过滤参数，无分页（前端直接消费全量）
- 时间范围查询使用 `created_at DESC` 索引
- 不做软删除，历史数据保留用于 portfolio 演示

---

## 4. 前端架构规范

### 4.1 技术选型

| 项目 | 选择 | 原因 |
|---|---|---|
| 框架 | Next.js 15 (App Router) | SSR 能力 + 现代 React 范式 |
| 语言 | TypeScript（严格模式） | 类型安全，portfolio 展示工程规范 |
| 样式 | Tailwind CSS | 与 shadcn/ui 原生配合 |
| 组件库 | shadcn/ui | 质感好，可定制，不黑盒 |
| 状态管理 | React hooks（无 Redux）| 项目规模不需要全局 store |
| 数据请求 | 原生 fetch + SWR | 轻量，适合轮询场景 |
| 图表 | Recharts | 与 React 生态一致 |

### 4.2 页面规范

#### `/` — Dashboard 总览
- 展示：实时吞吐率、P99 延迟、DLQ 数量、WebSocket 在线连接数、各队列深度热力图
- 数据源：`useMetrics` hook（5s 轮询 `/api/metrics/summary`）
- 不做 SSR，纯客户端渲染（数据实时性优先）

#### `/notifications` — 通知列表
- 支持筛选：channel（email/inapp/webhook）、status（delivered/failed/duplicate/dlq）、priority
- 支持分页（每页 50 条）
- 数据源：`useNotifications` hook，调用 `GET /notifications`

#### `/live` — 实时消息流
- WebSocket 连接到 `GET /ws`
- 新消息从顶部弹入，保留最新 100 条
- 支持按 channel / priority 过滤（客户端过滤）
- 数据源：`useWebSocket` hook

#### `/loadtest` — 压测控制台
- 启动 / 停止按钮（Demo 模式，~55s）
- 实时展示：吞吐量折线图、成功率、队列积压趋势
- 压测进行中禁止重复触发
- 数据源：`useLoadTest` hook（轮询 `GET /ops/loadtest/latest`，或按 run_id 查 `GET /ops/loadtest/{run_id}`）

#### `/dlq` — 死信队列
- 展示各队列 DLQ 数量（来自 metrics summary）
- Replay 按钮调用 `POST /dlq/replay`
- 操作后刷新 notifications 列表
- 重放结果用 Toast 提示

#### `/publish` — 手动发布（调试工具）
- 表单：event_type（下拉）、priority（单选）、payload（JSON 编辑器）
- 提交调用 `POST /events`
- 返回 message_id，可直接跳转 `/live` 观察

### 4.3 组件规范

- **shadcn/ui 组件**：只通过 `npx shadcn add` 安装，禁止手动修改 `components/ui/` 下的文件
- **业务组件**：放 `components/{page}/`，每个文件单一职责
- **Props 类型**：所有组件必须显式定义 props interface，不用 `any`
- **命名**：组件 PascalCase，hooks `useXxx`，工具函数 camelCase

### 4.4 数据请求规范

所有后端请求通过 `lib/api.ts` 统一封装：

```typescript
// 所有请求走这一层，统一处理 baseURL 和错误
const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'
```

Next.js API Routes 仅用于：
1. 代理 Prometheus `:9091` metrics（避免跨域 + 聚合逻辑）
2. 未来需要服务端鉴权的场景

### 4.5 类型定义规范（`types/index.ts`）

```typescript
export type Channel  = 'email' | 'inapp' | 'webhook'
export type Priority = 'high' | 'normal' | 'low'
export type Status   = 'delivered' | 'failed' | 'duplicate' | 'dlq'

export interface Notification {
  message_id: string
  channel:    Channel
  event_type: string
  // priority 字段 DB 暂无此列，待加 migration 后再启用
  status:     Status
  payload:    Record<string, unknown>
  created_at: string
}

export interface WsEvent {
  message_id: string
  type:       string      // 注意：不是 event_type
  priority:   Priority
  payload:    Record<string, unknown>
  timestamp:  string      // 注意：不是 created_at
}

export interface MetricsSummary {
  publish_rate_per_sec:      number
  processing_latency_p99_ms: number
  queue_depth:               Record<string, number>
  delivery_success_rate:     number
  dlq_count:                 number
  active_ws_connections:     number
  uptime_seconds:            number
}
```

---

## 5. 环境变量

### Go 后端（producer / worker）

| 变量 | 示例 | 说明 |
|---|---|---|
| `AMQP_URL` | `amqp://user:pass@...` | Railway 自动注入 |
| `POSTGRES_DSN` | `postgres://...` | Railway PostgreSQL |
| `REDIS_URL` | `redis://...` | Railway Redis |
| `SMTP_HOST` | `smtp.gmail.com` | 邮件发送 |
| `SMTP_PORT` | `587` | |
| `SMTP_USER` | `...` | |
| `SMTP_PASS` | `...` | |
| `K6_API_TOKEN` | `...` | 真实压测模式（可不配，用 Demo） |

### Next.js 前端

| 变量 | 示例 | 说明 |
|---|---|---|
| `NEXT_PUBLIC_API_URL` | `https://producer.railway.app` | Go producer 地址 |
| `NEXT_PUBLIC_WS_URL` | `wss://producer.railway.app/ws` | WebSocket 地址 |
| `METRICS_INTERNAL_URL` | `http://worker.internal:9091` | 仅服务端 API Route 使用 |

---

## 6. 开发命令

### 后端
```bash
# 启动 producer
go run ./cmd/producer

# 启动 worker
go run ./cmd/worker

# 构建两个二进制
go build -o bin/producer ./cmd/producer
go build -o bin/worker ./cmd/worker
```

### 前端
```bash
cd web

# 安装依赖
npm install

# 开发模式
npm run dev

# 构建
npm run build

# 添加 shadcn 组件
npx shadcn add button card table badge
```

### 本地基础设施
```bash
# 启动本地基础设施（docker-compose.yml 在 deploy/ 目录下）
docker compose -f deploy/docker-compose.yml up -d
```

---

## 7. 部署规范（Railway）

Railway 实际部署结构：

| Service | 配置文件 | 构建方式 |
|---|---|---|
| `nexus-producer` | `deploy/railway.toml` | Dockerfile |
| `nexus-worker` | `deploy/railway.worker.toml` | Dockerfile |
| `nexus-web` | 待创建 | Dockerfile 或 nixpacks (Node)，root=`web/` |

**注意**：根目录 `nixpacks.toml` 只构建 producer 二进制，**不参与 Railway 实际部署**（Railway 走 Dockerfile）。`nixpacks.toml` 可视为本地构建备用，勿依赖其部署 worker 或前端。

**注意事项**：
- `nixpacks.toml` 中构建顺序：先 `go mod download`，再 `go build`（已修复过的历史问题）
- `embed.FS` SPA fallback **未实现**，producer 不托管静态文件
- 前端 `NEXT_PUBLIC_*` 变量在 Railway build 时注入（不是运行时），必须在 deploy 前配好

---

## 8. AI 辅助开发指引

使用 Claude Code 或其他 AI 工具时的约定：

- **修改后端 API**：同步更新本文档第 2.4 节
- **新增前端页面**：在本文档第 4.2 节补充页面说明
- **新增环境变量**：同步更新第 5 节
- **不要修改** `components/ui/` 下的 shadcn 自动生成文件
- **类型优先**：新数据结构先在 `types/index.ts` 定义，再写实现
- **禁止** 在组件内直接 fetch，所有请求走 `lib/api.ts` 或对应 hook