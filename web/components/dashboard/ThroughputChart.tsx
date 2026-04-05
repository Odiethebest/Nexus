'use client'

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import type { MetricsSummary } from '@/types'

interface ThroughputChartProps {
  history: MetricsSummary[]
}

interface ChartPoint {
  t:       string
  email:   number
  inapp:   number
  webhook: number
}

function toChartData(history: MetricsSummary[]): ChartPoint[] {
  return history.map((m, i) => ({
    t:       String(i),
    email:   (m.queue_depth['email_high'] ?? 0) + (m.queue_depth['email_normal'] ?? 0) + (m.queue_depth['email_low'] ?? 0),
    inapp:   (m.queue_depth['inapp_high'] ?? 0) + (m.queue_depth['inapp_normal'] ?? 0) + (m.queue_depth['inapp_low'] ?? 0),
    webhook: (m.queue_depth['webhook_high'] ?? 0) + (m.queue_depth['webhook_normal'] ?? 0) + (m.queue_depth['webhook_low'] ?? 0),
  }))
}

export function ThroughputChart({ history }: ThroughputChartProps) {
  const data = toChartData(history)

  return (
    <Card className="bg-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          Queue Depth by Channel
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="gEmail" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#6366f1" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="gInapp" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#22d3ee" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#22d3ee" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="gWebhook" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#a78bfa" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#a78bfa" stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis dataKey="t" hide />
            <YAxis width={30} tick={{ fontSize: 10, fill: '#6b7280' }} />
            <Tooltip
              contentStyle={{ background: '#1c1c1c', border: '1px solid #333', borderRadius: 6, fontSize: 12 }}
              labelFormatter={() => ''}
            />
            <Area type="monotone" dataKey="email"   stroke="#6366f1" fill="url(#gEmail)"   strokeWidth={1.5} name="Email" />
            <Area type="monotone" dataKey="inapp"   stroke="#22d3ee" fill="url(#gInapp)"   strokeWidth={1.5} name="InApp" />
            <Area type="monotone" dataKey="webhook" stroke="#a78bfa" fill="url(#gWebhook)" strokeWidth={1.5} name="Webhook" />
          </AreaChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
