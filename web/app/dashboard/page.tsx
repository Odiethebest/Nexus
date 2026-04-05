"use client"

import React, { useEffect } from "react"
import { AlertCircleIcon } from "lucide-react"

import { AppSidebar } from "@/components/app-sidebar"
import { ChartAreaInteractive } from "@/components/chart-area-interactive"
import { DataTable } from "@/components/data-table"
import { SectionCards } from "@/components/section-cards"
import { SiteHeader } from "@/components/site-header"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import { useMetrics } from "@/hooks/useMetrics"
import { useNotifications } from "@/hooks/useNotifications"

export default function Page() {
  const { latest, history, loading: metricsLoading, error: metricsError, refresh: refreshMetrics } = useMetrics()
  const { notifications, loading: notifLoading } = useNotifications()

  useEffect(() => { document.title = "Dashboard — Nexus" }, [])

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
        <SiteHeader />
        <div className="flex flex-1 flex-col">
          <div className="@container/main flex flex-1 flex-col gap-2">
            <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
              {metricsError && (
                <div className="px-4 lg:px-6">
                  <Alert variant="destructive">
                    <AlertCircleIcon className="h-4 w-4" />
                    <AlertTitle>Failed to load data</AlertTitle>
                    <AlertDescription className="flex items-center justify-between">
                      <span>Could not connect to the Nexus backend at localhost:8080. Make sure the producer service is running.</span>
                      <Button size="sm" variant="outline" className="ml-4 shrink-0" onClick={refreshMetrics}>Retry</Button>
                    </AlertDescription>
                  </Alert>
                </div>
              )}
              <SectionCards metrics={latest} loading={metricsLoading} />
              <div className="px-4 lg:px-6">
                <ChartAreaInteractive history={history} />
              </div>
              <DataTable data={notifications} loading={notifLoading} />
            </div>
          </div>
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
