"use client"

import * as React from "react"
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts"

import { useIsMobile } from "@/hooks/use-mobile"
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  ToggleGroup,
  ToggleGroupItem,
} from "@/components/ui/toggle-group"
import type { Channel, MetricsSample } from "@/types"

const EMAIL_COLOR   = "#4ade80"
const INAPP_COLOR   = "#60a5fa"
const WEBHOOK_COLOR = "#f472b6"

const chartConfig = {
  email:   { label: "Email",   color: EMAIL_COLOR },
  inapp:   { label: "In-App",  color: INAPP_COLOR },
  webhook: { label: "Webhook", color: WEBHOOK_COLOR },
} satisfies ChartConfig

const LEGEND: { key: Channel; label: string; color: string }[] = [
  { key: "email",   label: "Email",   color: EMAIL_COLOR },
  { key: "inapp",   label: "In-App",  color: INAPP_COLOR },
  { key: "webhook", label: "Webhook", color: WEBHOOK_COLOR },
]

type RangeKey = "1m" | "5m" | "15m"

const RANGE_MS: Record<RangeKey, number> = {
  "1m":  60_000,
  "5m":  5 * 60_000,
  "15m": 15 * 60_000,
}

interface ChartPoint {
  t:       number
  label:   string
  email:   number
  inapp:   number
  webhook: number
}

function clockLabel(ms: number): string {
  return new Date(ms).toLocaleTimeString([], {
    hour:   "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  })
}

export function ChartAreaInteractive({ history }: { history: MetricsSample[] }) {
  const isMobile = useIsMobile()
  const [timeRange, setTimeRange] = React.useState<RangeKey>("1m")

  React.useEffect(() => {
    if (isMobile) setTimeRange("1m")
  }, [isMobile])

  // Select by elapsed time, not by sample count: a throttled background tab
  // polls irregularly, so "the last 12 samples" is not "the last minute".
  const chartData: ChartPoint[] = React.useMemo(() => {
    if (history.length === 0) return []
    const newest = history[history.length - 1].received_at
    const cutoff = newest - RANGE_MS[timeRange]

    return history
      .filter(s => s.received_at >= cutoff)
      .map(s => {
        const byChannel = s.processed_rate_per_sec_by_channel ?? {}
        return {
          t:       s.received_at,
          label:   clockLabel(s.received_at),
          email:   byChannel.email   ?? 0,
          inapp:   byChannel.inapp   ?? 0,
          webhook: byChannel.webhook ?? 0,
        }
      })
  }, [history, timeRange])

  // Distinguish "no data yet" from "range is longer than the history we have".
  const spanMs = history.length > 1
    ? history[history.length - 1].received_at - history[0].received_at
    : 0
  const rangeExceedsHistory =
    chartData.length >= 2 && spanMs < RANGE_MS[timeRange] * 0.9

  return (
    <Card className="@container/card">
      <CardHeader>
        <CardTitle>Delivery Throughput by Channel</CardTitle>
        <CardDescription>
          <span className="hidden @[540px]/card:block">
            Records completed per second in each channel worker — stacked
          </span>
          <span className="@[540px]/card:hidden">records/s by channel</span>
        </CardDescription>
        <CardAction>
          <ToggleGroup
            type="single"
            value={timeRange}
            onValueChange={v => v && setTimeRange(v as RangeKey)}
            variant="outline"
            className="hidden *:data-[slot=toggle-group-item]:px-4! @[767px]/card:flex"
          >
            <ToggleGroupItem value="15m">15 min</ToggleGroupItem>
            <ToggleGroupItem value="5m">5 min</ToggleGroupItem>
            <ToggleGroupItem value="1m">1 min</ToggleGroupItem>
          </ToggleGroup>
          <Select value={timeRange} onValueChange={v => setTimeRange(v as RangeKey)}>
            <SelectTrigger
              className="flex w-32 **:data-[slot=select-value]:block **:data-[slot=select-value]:truncate @[767px]/card:hidden"
              size="sm"
              aria-label="Select time range"
            >
              <SelectValue placeholder="1 min" />
            </SelectTrigger>
            <SelectContent className="rounded-xl">
              <SelectItem value="15m" className="rounded-lg">15 min</SelectItem>
              <SelectItem value="5m" className="rounded-lg">5 min</SelectItem>
              <SelectItem value="1m" className="rounded-lg">1 min</SelectItem>
            </SelectContent>
          </Select>
        </CardAction>
      </CardHeader>

      {/* Legend */}
      <div className="flex items-center gap-6 px-6 pb-2">
        {LEGEND.map(({ key, label, color }) => (
          <div key={key} className="flex items-center gap-1.5">
            <div className="h-2.5 w-2.5 rounded-sm" style={{ background: color }} />
            <span className="text-xs text-muted-foreground">{label}</span>
          </div>
        ))}
        {rangeExceedsHistory && (
          <span className="ml-auto text-xs text-muted-foreground">
            showing {Math.round(spanMs / 1000)}s of history
          </span>
        )}
      </div>

      <CardContent className="px-2 pt-4 sm:px-6 sm:pt-6">
        <div className="relative">
          {chartData.length < 2 && (
            <div className="absolute inset-0 z-10 flex items-center justify-center pointer-events-none">
              <p className="text-sm text-muted-foreground">
                Waiting for data — metrics update every 5s
              </p>
            </div>
          )}
          <ChartContainer
            config={chartConfig}
            className="aspect-auto h-[250px] w-full"
          >
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="fillEmail" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor={EMAIL_COLOR}   stopOpacity={0.8} />
                  <stop offset="95%" stopColor={EMAIL_COLOR}   stopOpacity={0.1} />
                </linearGradient>
                <linearGradient id="fillInapp" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor={INAPP_COLOR}   stopOpacity={0.8} />
                  <stop offset="95%" stopColor={INAPP_COLOR}   stopOpacity={0.1} />
                </linearGradient>
                <linearGradient id="fillWebhook" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor={WEBHOOK_COLOR} stopOpacity={0.8} />
                  <stop offset="95%" stopColor={WEBHOOK_COLOR} stopOpacity={0.1} />
                </linearGradient>
              </defs>
              <CartesianGrid vertical={false} />
              <XAxis
                dataKey="label"
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                minTickGap={32}
              />
              <YAxis
                tickLine={false}
                axisLine={false}
                width={40}
                tick={{ fontSize: 11 }}
                allowDecimals={false}
                label={{
                  value: "rec/s",
                  angle: -90,
                  position: "insideLeft",
                  style: { fontSize: 11, fill: "currentColor", opacity: 0.6 },
                }}
              />
              <ChartTooltip cursor={false} content={<ChartTooltipContent indicator="dot" />} />
              <Area dataKey="email"   type="natural" fill="url(#fillEmail)"   stroke={EMAIL_COLOR}   stackId="a" />
              <Area dataKey="inapp"   type="natural" fill="url(#fillInapp)"   stroke={INAPP_COLOR}   stackId="a" />
              <Area dataKey="webhook" type="natural" fill="url(#fillWebhook)" stroke={WEBHOOK_COLOR} stackId="a" />
            </AreaChart>
          </ChartContainer>
        </div>
      </CardContent>
    </Card>
  )
}
