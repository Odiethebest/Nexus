"use client"

import React, { useEffect, useState } from "react"
import { CheckCircleIcon, PlayIcon } from "lucide-react"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts"
import { AppSidebar } from "@/components/app-sidebar"
import { SectionCards } from "@/components/section-cards"
import { SiteHeader } from "@/components/site-header"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { useLoadTest } from "@/hooks/useLoadTest"
import { useMetrics } from "@/hooks/useMetrics"

/**
 * Splitting the timer in two means the counter never needs resetting: when
 * the run stops, RunningTimer unmounts and takes its state with it, and the
 * next run mounts a fresh one at zero. The previous single-component version
 * reset by calling setState directly in an effect body, which triggers a
 * cascading render.
 */
function ElapsedTimer({ running }: { running: boolean }) {
  if (!running) return null
  return <RunningTimer />
}

function RunningTimer() {
  const [elapsed, setElapsed] = useState(0)

  useEffect(() => {
    // Derive from a start timestamp rather than incrementing. A counter
    // drifts, and stalls entirely when the tab is backgrounded and the
    // browser throttles setInterval — the displayed time would then
    // under-report the real run length.
    const startedAt = Date.now()
    const id = setInterval(
      () => setElapsed(Math.floor((Date.now() - startedAt) / 1000)),
      1000,
    )
    return () => clearInterval(id)
  }, [])

  return <span className="text-sm text-muted-foreground">{elapsed}s elapsed</span>
}

export default function LoadTestPage() {
  const { status, result, chartData, error, start, reset } = useLoadTest()
  const { latest, loading: metricsLoading } = useMetrics()

  useEffect(() => { document.title = "Load Test — Nexus" }, [])

  const running   = status === 'running'
  const completed = status === 'completed'

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
        <SiteHeader title="Load Test" />
        <div className="flex flex-1 flex-col gap-4 py-4 md:gap-6 md:py-6">
          <div className="px-4 lg:px-6">
            <p className="text-sm text-muted-foreground mb-4">
              Run a demo load test to see the system under pressure
            </p>

            {/* Control panel */}
            <Card className="mb-4">
              <CardHeader>
                <CardTitle>Demo Mode</CardTitle>
                <CardDescription>
                  Simulates ~55 seconds of sustained traffic across all channels and priority levels.
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-col gap-4">
                <div className="flex items-center gap-4">
                  <Button
                    size="lg"
                    disabled={running}
                    onClick={start}
                    className="gap-2"
                  >
                    <PlayIcon className="h-4 w-4" />
                    Start Demo
                  </Button>
                  <ElapsedTimer running={running} />
                  {completed && (
                    <div className="flex items-center gap-2 text-green-600">
                      <CheckCircleIcon className="h-5 w-5" />
                      <span className="text-sm font-medium">Test completed</span>
                    </div>
                  )}
                  {error && (
                    <span className="text-sm text-red-500">{error}</span>
                  )}
                </div>
                {running && (
                  <div className="h-2 w-full rounded-full bg-muted overflow-hidden">
                    <div className="h-full bg-primary rounded-full animate-pulse w-full" />
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Live pipeline metrics from /api/metrics/summary. These are the
              real running system, independent of the demo run's synthetic
              series below — the cards previously relabelled this p99 as
              "P95 Latency" to match the demo chart, which mislabelled it. */}
          <SectionCards metrics={latest} loading={metricsLoading} />

          {/* Demo run throughput chart */}
          {(running || completed) && chartData.length > 0 && (
            <div className="px-4 lg:px-6">
              <Card>
                <CardHeader>
                  <CardTitle>Throughput</CardTitle>
                  <CardDescription>
                    RPS and P95 latency over the demo run
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={260}>
                    <LineChart data={chartData} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                      <XAxis
                        dataKey="t"
                        tickFormatter={(v, i) => `${i * 2}s`}
                        tick={{ fontSize: 11 }}
                      />
                      <YAxis yAxisId="rps" tick={{ fontSize: 11 }} />
                      <YAxis yAxisId="p95" orientation="right" tick={{ fontSize: 11 }} />
                      <Tooltip
                        formatter={(value, name) =>
                          name === "RPS"
                            ? [`${value} req/s`, "RPS"]
                            : [`${value} ms`, "P95"]
                        }
                        labelFormatter={(_, payload) => {
                          if (!payload?.length) return ""
                          const idx = chartData.findIndex(d => d.t === payload[0]?.payload?.t)
                          return `${idx * 2}s`
                        }}
                      />
                      <Legend />
                      <Line
                        yAxisId="rps"
                        type="monotone"
                        dataKey="rps"
                        stroke="#4ade80"
                        strokeWidth={2}
                        dot={false}
                        name="RPS"
                      />
                      <Line
                        yAxisId="p95"
                        type="monotone"
                        dataKey="p95"
                        stroke="#60a5fa"
                        strokeWidth={2}
                        dot={false}
                        name="P95 (ms)"
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            </div>
          )}

          {/* Results panel */}
          {completed && result && (
            <div className="px-4 lg:px-6">
              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle>Test Results</CardTitle>
                    <Button size="sm" variant="outline" onClick={reset}>Run Again</Button>
                  </div>
                </CardHeader>
                <CardContent>
                  <pre className="text-xs font-mono bg-muted rounded p-4 overflow-auto max-h-64">
                    {JSON.stringify(result, null, 2)}
                  </pre>
                </CardContent>
              </Card>
            </div>
          )}
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
