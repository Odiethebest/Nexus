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

// DLQ queue names as declared in internal/worker/*
const DLQ_QUEUES = [
  { name: "nexus.email.dlq.high",    label: "email / high" },
  { name: "nexus.email.dlq.normal",  label: "email / normal" },
  { name: "nexus.email.dlq.low",     label: "email / low" },
  { name: "nexus.inapp.dlq.high",    label: "inapp / high" },
  { name: "nexus.inapp.dlq.normal",  label: "inapp / normal" },
  { name: "nexus.inapp.dlq.low",     label: "inapp / low" },
  { name: "nexus.webhook.dlq.high",  label: "webhook / high" },
  { name: "nexus.webhook.dlq.normal",label: "webhook / normal" },
  { name: "nexus.webhook.dlq.low",   label: "webhook / low" },
]

export default function DLQPage() {
  const { latest, loading, error, refresh } = useMetrics()

  useEffect(() => { document.title = "DLQ — Nexus" }, [])

  const dlqTotal = latest?.dlq_count ?? 0
  // TODO: backend should expose dlq_depth separately — queue_depth currently shows main queue depths
  const queueDepth = latest?.queue_depth ?? {}

  const handleReplayAll = async () => {
    try {
      let total = 0
      for (const q of DLQ_QUEUES) {
        try {
          const r = await replayDLQ(q.name, 100)
          total += r?.replayed ?? 0
        } catch {}
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
              <CardTitle>Queue Depth Breakdown</CardTitle>
              <CardDescription>
                Main queue depths — {/* TODO: backend should expose dlq_depth separately */}
                DLQ depths not yet tracked individually by backend.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Queue</TableHead>
                    <TableHead className="text-right">Depth</TableHead>
                    <TableHead className="text-right">Action</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {DLQ_QUEUES.map(q => {
                    const depthKey = q.name.replace("nexus.", "").replace(".dlq", "").replace(/\./g, "_")
                    const depth = queueDepth[depthKey] ?? 0
                    return (
                      <TableRow key={q.name}>
                        <TableCell className="font-mono text-xs">{q.label}</TableCell>
                        <TableCell className="text-right">
                          <Badge variant={depth > 0 ? "destructive" : "outline"}>{depth}</Badge>
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            size="sm"
                            variant="ghost"
                            className="h-6 text-xs"
                            onClick={async () => {
                              try {
                                const r = await replayDLQ(q.name)
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
