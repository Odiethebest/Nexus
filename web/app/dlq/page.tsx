"use client"

import React, { useEffect } from "react"
import { toast } from "sonner"
import { AlertCircleIcon } from "lucide-react"
import { AppSidebar } from "@/components/app-sidebar"
import { SiteHeader } from "@/components/site-header"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { replayDLQ } from "@/lib/api"
import { useMetrics } from "@/hooks/useMetrics"

const CHANNELS = ["email", "inapp", "webhook"] as const
const PRIORITIES = ["high", "normal", "low"] as const

/**
 * The nine DLQ topics. `topic` is the Kafka-native name
 * (`nexus.dlq.<channel>.<priority>`) that `POST /dlq/replay` expects;
 * `depthKey` matches the `<channel>_<priority>` keys in the summary's
 * `dlq_depth` map.
 */
const DLQ_QUEUES = CHANNELS.flatMap(channel =>
  PRIORITIES.map(priority => ({
    topic:    `nexus.dlq.${channel}.${priority}`,
    depthKey: `${channel}_${priority}`,
    label:    `${channel} / ${priority}`,
  }))
)

export default function DLQPage() {
  const { latest, loading, error, refresh } = useMetrics()

  useEffect(() => { document.title = "DLQ — Nexus" }, [])

  const dlqTotal = latest?.dlq_count ?? 0
  // Real per-lane dead-letter counts. This table used to read `queue_depth`,
  // which is the *primary* lane backlog — it showed healthy main-lane numbers
  // under a "DLQ" heading.
  const dlqDepth = latest?.dlq_depth ?? {}

  const handleReplayAll = async () => {
    try {
      let total = 0
      for (const q of DLQ_QUEUES) {
        try {
          const r = await replayDLQ(q.topic, 100)
          total += r?.replayed ?? 0
        } catch {/* a lane with nothing to replay is not an error */}
      }
      toast.success(`Replayed ${total} message${total !== 1 ? "s" : ""} from DLQ`)
      setTimeout(() => refresh(), 3000)
    } catch (e) {
      toast.error(String(e))
    }
  }

  return (
    <SidebarProvider
      style={
        {
          "--sidebar-width": "calc(var(--spacing) * 72)",
          "--header-height": "calc(var(--spacing) * 12)",
        } as React.CSSProperties
      }
    >
      <AppSidebar variant="inset" />
      <SidebarInset>
        <SiteHeader title="Dead-Letter Queue" />
        <div className="flex flex-1 flex-col gap-4 p-4 md:p-6">
          <p className="text-sm text-muted-foreground">
            Messages that failed processing after maximum retries
          </p>

          {error && (
            <Alert variant="destructive">
              <AlertCircleIcon className="h-4 w-4" />
              <AlertTitle>Failed to load data</AlertTitle>
              <AlertDescription className="flex items-center justify-between">
                <span>Could not connect to the Nexus backend at localhost:8080. Make sure the producer service is running.</span>
                <Button size="sm" variant="outline" className="ml-4 shrink-0" onClick={refresh}>Retry</Button>
              </AlertDescription>
            </Alert>
          )}

          {/* Summary cards */}
          <div className="grid grid-cols-1 gap-4 @xl/main:grid-cols-2">
            <Card>
              <CardHeader>
                <CardDescription>Total DLQ Messages</CardDescription>
                <CardTitle className={`text-3xl font-semibold tabular-nums ${dlqTotal > 0 ? "text-red-600" : ""}`}>
                  {loading ? "—" : dlqTotal}
                </CardTitle>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader>
                <CardDescription>DLQ Status</CardDescription>
                <CardTitle className="flex items-center gap-2">
                  {loading ? "—" : dlqTotal > 0 ? (
                    <Badge className="bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200" variant="outline">
                      Attention required
                    </Badge>
                  ) : (
                    <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200" variant="outline">
                      Nominal
                    </Badge>
                  )}
                </CardTitle>
              </CardHeader>
            </Card>
          </div>

          {/* Replay panel */}
          <Card>
            <CardHeader>
              <CardTitle>Replay Dead-Letter Queue</CardTitle>
              <CardDescription>
                Replay all messages currently in the dead-letter queues back into the main processing pipeline.
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              <div className="rounded-lg border border-yellow-200 bg-yellow-50 dark:bg-yellow-950 dark:border-yellow-800 p-3 text-sm text-yellow-800 dark:text-yellow-300">
                ⚠ Replayed messages will be re-processed. Ensure the underlying issue has been resolved before replaying.
              </div>
              <AlertDialog>
                <AlertDialogTrigger asChild>
                  <Button variant="destructive" className="w-fit">Replay All</Button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>Replay DLQ messages?</AlertDialogTitle>
                    <AlertDialogDescription>
                      This will replay {dlqTotal} DLQ message{dlqTotal !== 1 ? "s" : ""} back into the processing pipeline. Make sure the root cause has been resolved first.
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogCancel>Cancel</AlertDialogCancel>
                    <AlertDialogAction onClick={handleReplayAll}>Replay All</AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
            </CardContent>
          </Card>

          {/* Queue breakdown table */}
          <Card>
            <CardHeader>
              <CardTitle>Dead-Letter Breakdown by Lane</CardTitle>
              <CardDescription>
                Records dead-lettered per <code>nexus.dlq.&lt;channel&gt;.&lt;priority&gt;</code> topic.
                Sampled from topic end offsets, so these are cumulative totals —
                a successful replay re-processes the records but does not lower the count.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Lane</TableHead>
                    <TableHead className="text-right">Dead-lettered</TableHead>
                    <TableHead className="text-right">Action</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {DLQ_QUEUES.map(q => {
                    const depth = dlqDepth[q.depthKey] ?? 0
                    return (
                      <TableRow key={q.topic}>
                        <TableCell className="font-mono text-xs">{q.label}</TableCell>
                        <TableCell className="text-right">
                          <Badge variant={depth > 0 ? "destructive" : "outline"}>{depth}</Badge>
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            size="sm"
                            variant="ghost"
                            className="h-6 text-xs"
                            disabled={depth === 0}
                            onClick={async () => {
                              try {
                                const r = await replayDLQ(q.topic)
                                toast.success(`Replayed ${r?.replayed ?? 0} messages from ${q.label}`)
                                setTimeout(() => refresh(), 3000)
                              } catch (e) {
                                toast.error(String(e))
                              }
                            }}
                          >
                            Replay
                          </Button>
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
