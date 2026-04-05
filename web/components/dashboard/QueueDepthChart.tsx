'use client'

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import type { MetricsSummary } from '@/types'

interface QueueDepthChartProps {
  latest: MetricsSummary | null
}

const QUEUES = [
  { key: 'email_high',    label: 'Email High',    color: '#ef4444' },
  { key: 'email_normal',  label: 'Email Normal',  color: '#eab308' },
  { key: 'email_low',     label: 'Email Low',     color: '#6b7280' },
  { key: 'inapp_high',    label: 'InApp High',    color: '#ef4444' },
  { key: 'inapp_normal',  label: 'InApp Normal',  color: '#eab308' },
  { key: 'inapp_low',     label: 'InApp Low',     color: '#6b7280' },
  { key: 'webhook_high',  label: 'Webhook High',  color: '#ef4444' },
  { key: 'webhook_normal',label: 'Webhook Normal',color: '#eab308' },
  { key: 'webhook_low',   label: 'Webhook Low',   color: '#6b7280' },
]

export function QueueDepthChart({ latest }: QueueDepthChartProps) {
  const data = QUEUES.map(q => ({
    label: q.label,
    value: latest?.queue_depth[q.key] ?? 0,
    color: q.color,
  }))

  return (
    <Card className="bg-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          Queue Depth (All Lanes)
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={data} layout="vertical" margin={{ top: 0, right: 8, bottom: 0, left: 0 }}>
            <XAxis type="number" tick={{ fontSize: 10, fill: '#6b7280' }} width={30} />
            <YAxis type="category" dataKey="label" width={90} tick={{ fontSize: 10, fill: '#6b7280' }} />
            <Tooltip
              contentStyle={{ background: '#1c1c1c', border: '1px solid #333', borderRadius: 6, fontSize: 12 }}
              cursor={{ fill: 'rgba(255,255,255,0.04)' }}
            />
            <Bar dataKey="value" radius={[0, 3, 3, 0]}>
              {data.map((entry, i) => (
                <Cell key={i} fill={entry.color} fillOpacity={0.8} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
