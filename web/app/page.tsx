'use client'

import { TopBar } from '@/components/layout/TopBar'
import { MetricCard } from '@/components/dashboard/MetricCard'
import { ThroughputChart } from '@/components/dashboard/ThroughputChart'
import { QueueDepthChart } from '@/components/dashboard/QueueDepthChart'
import { useMetrics } from '@/hooks/useMetrics'
import { useNotifications } from '@/hooks/useNotifications'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import type { Notification } from '@/types'

function relativeTime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime()
  const s = Math.floor(diff / 1000)
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  return `${Math.floor(m / 60)}h ago`
}

const statusColor: Record<string, string> = {
  delivered: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
  failed:    'bg-red-500/15 text-red-400 border-red-500/30',
  duplicate: 'bg-yellow-500/15 text-yellow-400 border-yellow-500/30',
  dlq:       'bg-orange-500/15 text-orange-400 border-orange-500/30',
}

const channelColor: Record<string, string> = {
  email:   'bg-blue-500/15 text-blue-400 border-blue-500/30',
  inapp:   'bg-purple-500/15 text-purple-400 border-purple-500/30',
  webhook: 'bg-cyan-500/15 text-cyan-400 border-cyan-500/30',
}

function NotificationRow({ n }: { n: Notification }) {
  return (
    <TableRow className="border-border hover:bg-accent/20">
      <TableCell className="font-mono text-xs text-muted-foreground">
        {n.message_id.slice(0, 8)}
      </TableCell>
      <TableCell>
        <Badge variant="outline" className={channelColor[n.channel] ?? ''}>
          {n.channel}
        </Badge>
      </TableCell>
      <TableCell className="text-xs text-foreground">{n.event_type}</TableCell>
      <TableCell>
        <Badge variant="outline" className={statusColor[n.status] ?? ''}>
          {n.status}
        </Badge>
      </TableCell>
      <TableCell className="text-xs text-muted-foreground">
        {relativeTime(n.created_at)}
      </TableCell>
    </TableRow>
  )
}

export default function DashboardPage() {
  const { latest, history, loading: metricsLoading } = useMetrics()
  const { notifications, loading: notifLoading } = useNotifications()

  const dlqVariant = latest && latest.dlq_count > 0 ? 'bad' : 'good'

  return (
    <div className="flex flex-col flex-1">
      <TopBar title="Dashboard" />
      <main className="flex-1 p-6 space-y-6">

        {/* MetricCards */}
        <div className="grid grid-cols-4 gap-4">
          <MetricCard
            title="Publish Rate"
            value={latest?.publish_rate_per_sec ?? null}
            unit="msg/s"
            variant="good"
            loading={metricsLoading}
          />
          <MetricCard
            title="P99 Latency"
            value={latest?.processing_latency_p99_ms ?? null}
            unit="ms"
            variant={latest && latest.processing_latency_p99_ms > 500 ? 'bad' : 'neutral'}
            loading={metricsLoading}
          />
          <MetricCard
            title="DLQ Backlog"
            value={latest?.dlq_count ?? null}
            unit="msgs"
            variant={dlqVariant}
            loading={metricsLoading}
          />
          <MetricCard
            title="WS Connections"
            value={latest?.active_ws_connections ?? null}
            unit="clients"
            variant="neutral"
            loading={metricsLoading}
          />
        </div>

        {/* Charts */}
        <div className="grid grid-cols-5 gap-4">
          <div className="col-span-3">
            <ThroughputChart history={history} />
          </div>
          <div className="col-span-2">
            <QueueDepthChart latest={latest} />
          </div>
        </div>

        {/* Notifications table */}
        <div className="rounded-lg border border-border bg-card">
          <div className="flex items-center justify-between px-4 py-3 border-b border-border">
            <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Recent Notifications
            </span>
            <span className="text-xs text-muted-foreground">latest 50</span>
          </div>
          <Table>
            <TableHeader>
              <TableRow className="border-border hover:bg-transparent">
                <TableHead className="text-xs text-muted-foreground">ID</TableHead>
                <TableHead className="text-xs text-muted-foreground">Channel</TableHead>
                <TableHead className="text-xs text-muted-foreground">Event Type</TableHead>
                <TableHead className="text-xs text-muted-foreground">Status</TableHead>
                <TableHead className="text-xs text-muted-foreground">Time</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {notifLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <TableRow key={i} className="border-border">
                    <TableCell colSpan={5}>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                  </TableRow>
                ))
              ) : notifications.length === 0 ? (
                <TableRow className="border-border">
                  <TableCell colSpan={5} className="text-center text-xs text-muted-foreground py-8">
                    No notifications yet
                  </TableCell>
                </TableRow>
              ) : (
                notifications.map(n => (
                  <NotificationRow key={`${n.message_id}-${n.channel}`} n={n} />
                ))
              )}
            </TableBody>
          </Table>
        </div>

      </main>
    </div>
  )
}
