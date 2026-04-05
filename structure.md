# Nexus — Monorepo 文件结构

```
nexus/
│
├── cmd/
│   ├── producer/               # HTTP + gRPC + WebSocket 服务入口 (port 8080 / 50051)
│   │   └── main.go
│   └── worker/                 # RabbitMQ Consumer 服务入口 (metrics port 9091)
│       └── main.go
│
├── internal/
│   ├── broker/                 # RabbitMQ 连接、Exchange/Queue 声明、Publisher
│   ├── store/                  # PostgreSQL CRUD (notifications 表)
│   ├── hub/                    # WebSocket Hub，非阻塞广播
│   ├── idempotency/            # Redis 幂等去重 (24h TTL)
│   ├── grpcserver/             # gRPC server 实现
│   ├── replay/                 # DLQ 重放逻辑
│   ├── loadtest/               # 压测双模式 (k6 云 / Demo 合成数据)
│   ├── metrics/                # Prometheus 指标注册（不含 HTTP handler，summary 端点待实现）
│   ├── mailer/                 # SMTP 发送
│   ├── worker/
│   │   ├── email.go            # EmailWorker (pool=10)
│   │   ├── inapp.go            # InAppWorker (pool=5)
│   │   └── webhook.go          # WebhookWorker (pool=8)
│   └── envutil/                # 环境变量读取
│
├── web/                        # ★ Next.js 前端 (App Router)
│   │
│   ├── app/
│   │   ├── layout.tsx          # 根布局，Sidebar + TopBar
│   │   ├── page.tsx            # / → Dashboard 总览
│   │   ├── notifications/
│   │   │   └── page.tsx        # 通知列表（筛选 / 分页）
│   │   ├── live/
│   │   │   └── page.tsx        # WebSocket 实时消息流
│   │   ├── loadtest/
│   │   │   └── page.tsx        # 压测控制台 + 实时进度
│   │   ├── dlq/
│   │   │   └── page.tsx        # 死信队列管理 + Replay
│   │   ├── publish/
│   │   │   └── page.tsx        # 手动发布事件（调试工具）
│   │   └── api/
│   │       └── metrics/
│   │           └── route.ts    # Next.js API Route，代理 :9091 Prometheus 端点
│   │
│   ├── components/
│   │   ├── ui/                 # shadcn/ui 自动生成组件（不手动编辑）
│   │   ├── layout/
│   │   │   ├── Sidebar.tsx
│   │   │   └── TopBar.tsx
│   │   ├── dashboard/
│   │   │   ├── MetricCard.tsx  # 单指标卡片（吞吐量、延迟、队列深度等）
│   │   │   ├── ThroughputChart.tsx
│   │   │   └── SystemHealth.tsx
│   │   ├── notifications/
│   │   │   ├── NotificationTable.tsx
│   │   │   └── FilterBar.tsx
│   │   ├── live/
│   │   │   └── EventFeed.tsx   # WebSocket 消息实时弹入列表
│   │   ├── loadtest/
│   │   │   ├── LoadTestControl.tsx
│   │   │   └── LoadTestProgress.tsx
│   │   └── dlq/
│   │       ├── DLQTable.tsx
│   │       └── ReplayButton.tsx
│   │
│   ├── hooks/
│   │   ├── useWebSocket.ts     # WebSocket 连接管理，自动重连
│   │   ├── useNotifications.ts # 通知列表数据 + 筛选状态
│   │   ├── useMetrics.ts       # 定期 poll /api/metrics/summary
│   │   └── useLoadTest.ts      # 压测触发 + 进度轮询
│   │
│   ├── lib/
│   │   ├── api.ts              # 所有 fetch 调用的统一封装（baseURL、错误处理）
│   │   ├── websocket.ts        # WebSocket 单例 + 事件类型解析
│   │   └── utils.ts            # cn() 等通用工具
│   │
│   ├── types/
│   │   └── index.ts            # Notification、Event、MetricsSummary 等 TS 类型定义
│   │
│   ├── public/
│   ├── components.json         # shadcn/ui 配置
│   ├── next.config.ts          # rewrites: /api/backend/* → Go producer
│   ├── tailwind.config.ts
│   ├── tsconfig.json
│   └── package.json
│
├── docs/                       # ★ 待创建
│   ├── ARCHITECTURE.md         # 系统架构图 + 消息流说明
│   ├── API.md                  # 完整 REST / gRPC / WebSocket API 文档
│   └── DEPLOYMENT.md           # Railway 部署步骤 + 环境变量清单
│
├── go.mod
├── go.sum
├── nixpacks.toml               # 本地构建备用（仅 producer，不参与 Railway 实际部署）
├── deploy/
│   ├── railway.toml            # Railway producer service（Dockerfile 构建）
│   ├── railway.worker.toml     # Railway worker service（Dockerfile 构建）
│   └── docker-compose.yml      # 本地基础设施（RabbitMQ + PostgreSQL + Redis）
└── CLAUDE.md                   # ★ 项目全局规范文档（给 AI 和协作者读）
```

## Railway 部署结构

两个独立配置文件，均走 Dockerfile 构建：

| Service | 配置文件 | 启动命令（容器内路径） |
|---|---|---|
| `nexus-producer` | `deploy/railway.toml` | `/app/producer` |
| `nexus-worker` | `deploy/railway.worker.toml` | `/app/worker` |
| `nexus-web` | 待创建 | `next start`（root=`web/`） |

## next.config.ts Rewrite 规则（待实现）

> 以下为前端创建后的目标配置，当前 `web/` 目录尚不存在。

```
/api/backend/**  →  http://producer.internal:8080/**
```

Prometheus metrics 通过 `web/app/api/metrics/route.ts` 代理内部 `:9091`，不直接暴露给浏览器端。