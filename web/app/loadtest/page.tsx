"use client"

import React, { useEffect, useState } from "react"
import { CheckCircleIcon, PlayIcon } from "lucide-react"
import { AppSidebar } from "@/components/app-sidebar"
import { ChartAreaInteractive } from "@/components/chart-area-interactive"
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

function ElapsedTimer({ running }: { running: boolean }) {
  const [elapsed, setElapsed] = useState(0)

  useEffect(() => {
    if (!running) { setElapsed(0); return }
    const id = setInterval(() => setElapsed(s => s + 1), 1000)
    return () => clearInterval(id)
  }, [running])

  if (!running) return null
  return <span className="text-sm text-muted-foreground">{elapsed}s elapsed</span>
}

export default function LoadTestPage() {
  const { status, result, error, start, reset } = useLoadTest()
  const { latest, history, loading: metricsLoading } = useMetrics()

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

          {/* Live metrics */}
          <SectionCards metrics={latest} loading={metricsLoading} />

          {/* Throughput chart */}
          <div className="px-4 lg:px-6">
            <ChartAreaInteractive history={history} />
          </div>

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
