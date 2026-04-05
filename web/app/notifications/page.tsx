"use client"

import React, { useEffect, useState } from "react"
import { toast } from "sonner"
import { AlertCircleIcon } from "lucide-react"
import { AppSidebar } from "@/components/app-sidebar"
import { DataTable } from "@/components/data-table"
import { FilterBar } from "@/components/notifications/FilterBar"
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
import { Button } from "@/components/ui/button"
import { clearNotifications } from "@/lib/api"
import { useNotifications } from "@/hooks/useNotifications"
import type { Channel, Status } from "@/types"

type ChannelFilter = "all" | Channel
type StatusFilter  = "all" | Status

export default function NotificationsPage() {
  const { notifications, loading, error, refresh } = useNotifications()
  const [channel, setChannel] = useState<ChannelFilter>("all")
  const [status,  setStatus]  = useState<StatusFilter>("all")
  const [search,  setSearch]  = useState("")

  useEffect(() => { document.title = "Notifications — Nexus" }, [])

  const filtered = (notifications ?? []).filter(n => {
    if (channel !== "all" && n.channel !== channel) return false
    if (status  !== "all" && n.status  !== status)  return false
    if (search && !n.event_type.toLowerCase().includes(search.toLowerCase())) return false
    return true
  })

  const hasActiveFilter = channel !== "all" || status !== "all" || search !== ""
  const clearFilters = () => { setChannel("all"); setStatus("all"); setSearch("") }

  const handleClearAll = async () => {
    try {
      await clearNotifications()
      refresh()
      toast.success("All notifications cleared")
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
        <div className="flex items-center justify-between border-b px-4 lg:px-6 py-2">
          <SiteHeader title="Notifications" />
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="destructive" size="sm">Clear All</Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Clear all notifications?</AlertDialogTitle>
                <AlertDialogDescription>
                  This will permanently delete all notification records. This action cannot be undone.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction onClick={handleClearAll}>Clear All</AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
        <div className="flex flex-1 flex-col gap-4 py-4 md:gap-6 md:py-6">
          {error && (
            <div className="px-4 lg:px-6">
              <Alert variant="destructive">
                <AlertCircleIcon className="h-4 w-4" />
                <AlertTitle>Failed to load data</AlertTitle>
                <AlertDescription className="flex items-center justify-between">
                  <span>Could not connect to the Nexus backend at localhost:8080. Make sure the producer service is running.</span>
                  <Button size="sm" variant="outline" className="ml-4 shrink-0" onClick={refresh}>Retry</Button>
                </AlertDescription>
              </Alert>
            </div>
          )}
          <FilterBar
            channel={channel}
            status={status}
            search={search}
            count={filtered.length}
            onChannel={setChannel}
            onStatus={setStatus}
            onSearch={setSearch}
            onClear={clearFilters}
            hasActiveFilter={hasActiveFilter}
          />
          <DataTable data={filtered} loading={loading} />
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
