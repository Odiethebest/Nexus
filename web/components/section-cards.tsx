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
import type { Channel, MetricsSummary } from "@/types"

interface SectionCardsProps {
  metrics: MetricsSummary | null
  loading: boolean
}

/**
 * The e2e-lag SLO the pipeline is designed around: p99 of
 * (now − x-produced-at) at consumer pick-up. Matches the 1.5s threshold line
 * on the "End-to-end lag p99" Grafana panel.
 */
const E2E_LAG_SLO_SECONDS = 1.5

/** Non-breaking space, so an empty sub-line still reserves its row height. */
const BLANK_LINE = " "

/**
 * Coerce a possibly-absent number. The API always sends these fields, but a
 * browser holding a cached bundle against a different producer build should
 * render an em dash rather than throw on `undefined.toFixed`.
 */
function num(v: number | undefined | null): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null
}

function fmt1(v: number): string {
  return String(Number(v.toFixed(1)))
}

/** Sub-second lag reads better in ms; above that, seconds. */
function fmtLag(seconds: number): string {
  return seconds < 1 ? `${Math.round(seconds * 1000)} ms` : `${fmt1(seconds)} s`
}

function Value({
  loading,
  value,
  render,
}: {
  loading: boolean
  value: number | null
  render: (v: number) => string
}) {
  if (loading) return <Skeleton className="h-9 w-24" />
  if (value === null) return <>&mdash;</>
  return <>{render(value)}</>
}

export function SectionCards({ metrics, loading }: SectionCardsProps) {
  const publishRate   = num(metrics?.publish_rate_per_sec)
  const processedRate = num(metrics?.processed_rate_per_sec)
  const e2eLag        = num(metrics?.e2e_lag_p99_seconds)
  const processingP99 = num(metrics?.processing_latency_p99_ms)
  const dlqCount      = num(metrics?.dlq_count)
  const wsCount       = num(metrics?.active_ws_connections)

  // Backpressure is per-lane, not aggregate. publish_rate_per_sec counts
  // events while processed_rate_per_sec counts records, so comparing those
  // two directly is a unit error: the aggregate sits at roughly 3x publish
  // even when one lane is badly stalled. Each channel's rate does track the
  // event rate 1:1, so the slowest lane is the signal that means something.
  const laneRates = metrics?.processed_rate_per_sec_by_channel ?? {}
  const laneEntries = (Object.entries(laneRates) as [Channel, number][])
    .filter(([, v]) => typeof v === "number" && Number.isFinite(v))
  const slowestLane = laneEntries.length > 0
    ? laneEntries.reduce((a, b) => (b[1] < a[1] ? b : a))
    : null

  // Only meaningful once there is enough throughput for the ratio to mean
  // anything.
  const backpressure =
    publishRate !== null && slowestLane !== null &&
    publishRate > 5 && slowestLane[1] < publishRate * 0.8

  const lagWithinSLO = e2eLag !== null && e2eLag <= E2E_LAG_SLO_SECONDS

  const publishSubLine = () => {
    if (loading || processedRate === null) return BLANK_LINE
    if (backpressure && slowestLane) {
      return `${slowestLane[0]} lane behind at ${fmt1(slowestLane[1])} rec/s`
    }
    return `${fmt1(processedRate)} rec/s processed across all lanes`
  }

  return (
    <div className="grid grid-cols-2 gap-4 px-4 *:data-[slot=card]:bg-gradient-to-t *:data-[slot=card]:from-primary/5 *:data-[slot=card]:to-card *:data-[slot=card]:shadow-xs lg:px-6 xl:grid-cols-4 dark:*:data-[slot=card]:bg-card">
      <Card className="@container/card">
        <CardHeader>
          <CardDescription>Publish Rate</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
            <Value loading={loading} value={publishRate} render={v => `${fmt1(v)} events/s`} />
          </CardTitle>
          <p className="text-xs text-muted-foreground">{publishSubLine()}</p>
          <CardAction>
            {backpressure ? (
              <Badge className="bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200" variant="outline">
                backpressure
              </Badge>
            ) : (
              <Badge variant="outline">events/s</Badge>
            )}
          </CardAction>
        </CardHeader>
      </Card>

      <Card className="@container/card">
        <CardHeader>
          <CardDescription>E2E Lag p99</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
            <Value loading={loading} value={e2eLag} render={fmtLag} />
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            {loading || processingP99 === null
              ? BLANK_LINE
              : `${fmt1(processingP99)} ms worker processing p99`}
          </p>
          <CardAction>
            {loading || e2eLag === null ? (
              <Badge variant="outline">p99</Badge>
            ) : lagWithinSLO ? (
              <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200" variant="outline">
                &lt; {E2E_LAG_SLO_SECONDS}s
              </Badge>
            ) : (
              <Badge className="bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200" variant="outline">
                over SLO
              </Badge>
            )}
          </CardAction>
        </CardHeader>
      </Card>

      <Card className="@container/card">
        <CardHeader>
          <CardDescription>Dead-Lettered</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
            <Value loading={loading} value={dlqCount} render={v => `${v} msgs`} />
          </CardTitle>
          {/* Sampled from DLQ topic end offsets, so this is a cumulative
              total rather than a pending-work backlog: replay does not
              lower it. */}
          <p className="text-xs text-muted-foreground">cumulative; replay does not reduce it</p>
          <CardAction>
            {loading || dlqCount === null ? (
              <Badge variant="outline">msgs</Badge>
            ) : dlqCount > 0 ? (
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
            <Value loading={loading} value={wsCount} render={v => `${v} clients`} />
          </CardTitle>
          <p className="text-xs text-muted-foreground">live event-feed subscribers</p>
          <CardAction>
            <Badge variant="outline">clients</Badge>
          </CardAction>
        </CardHeader>
      </Card>
    </div>
  )
}
