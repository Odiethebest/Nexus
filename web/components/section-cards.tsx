"use client"

import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardAction,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import type { MetricsSummary } from "@/types"

interface SectionCardsProps {
  metrics: MetricsSummary | null
  loading: boolean
  latencyLabel?: string
}

function fmt1(v: number): string {
  return String(Number(v.toFixed(1)))
}

export function SectionCards({ metrics, loading, latencyLabel = "P99 Latency" }: SectionCardsProps) {
  return (
    <div className="grid grid-cols-2 gap-4 px-4 *:data-[slot=card]:bg-gradient-to-t *:data-[slot=card]:from-primary/5 *:data-[slot=card]:to-card *:data-[slot=card]:shadow-xs lg:px-6 xl:grid-cols-4 dark:*:data-[slot=card]:bg-card">
      <Card className="@container/card">
        <CardHeader>
          <CardDescription>Publish Rate</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
            {loading ? <Skeleton className="h-9 w-24" /> : metrics ? `${fmt1(metrics.publish_rate_per_sec)} msg/s` : "—"}
          </CardTitle>
          <CardAction>
            <Badge variant="outline">msg/s</Badge>
          </CardAction>
        </CardHeader>
      </Card>

      <Card className="@container/card">
        <CardHeader>
          <CardDescription>{latencyLabel}</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
            {loading ? <Skeleton className="h-9 w-24" /> : metrics ? `${fmt1(metrics.processing_latency_p99_ms)} ms` : "—"}
          </CardTitle>
          <CardAction>
            <Badge variant="outline">ms</Badge>
          </CardAction>
        </CardHeader>
      </Card>

      <Card className="@container/card">
        <CardHeader>
          <CardDescription>DLQ Backlog</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
            {loading ? <Skeleton className="h-9 w-24" /> : metrics ? `${metrics.dlq_count} msgs` : "—"}
          </CardTitle>
          <CardAction>
            {loading || metrics === null ? (
              <Badge variant="outline">msgs</Badge>
            ) : metrics.dlq_count > 0 ? (
              <Badge className="bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200" variant="outline">
                attention
              </Badge>
            ) : (
              <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200" variant="outline">
                nominal
              </Badge>
            )}
          </CardAction>
        </CardHeader>
      </Card>

      <Card className="@container/card">
        <CardHeader>
          <CardDescription>WS Connections</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
            {loading ? <Skeleton className="h-9 w-24" /> : metrics ? `${metrics.active_ws_connections} clients` : "—"}
          </CardTitle>
          <CardAction>
            <Badge variant="outline">clients</Badge>
          </CardAction>
        </CardHeader>
      </Card>
    </div>
  )
}
