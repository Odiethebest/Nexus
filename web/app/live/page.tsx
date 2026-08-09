"use client"

import React, { useEffect, useState } from "react"
import { ActivityIcon } from "lucide-react"
import { AppSidebar } from "@/components/app-sidebar"
import { EventCard } from "@/components/live/EventCard"
import { SiteHeader } from "@/components/site-header"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { useWebSocket } from "@/hooks/useWebSocket"
import type { Priority } from "@/types"

type ChannelFilter = "all" | "email" | "inapp" | "webhook"
type PriorityFilter = "all" | Priority

const CHANNEL_OPTS: ChannelFilter[] = ["all", "email", "inapp", "webhook"]
const PRIORITY_OPTS: PriorityFilter[] = ["all", "high", "normal", "low"]

export default function LivePage() {
  const { events, connected, loading, clear } = useWebSocket()
  const [channelFilter, setChannelFilter]   = useState<ChannelFilter>("all")
  const [priorityFilter, setPriorityFilter] = useState<PriorityFilter>("all")

  useEffect(() => {
    document.title = "Live Feed — Nexus"
  }, [])

  const filtered = (events ?? [])
    .filter(e => channelFilter === "all" || e.channel === channelFilter)
    .filter(e => priorityFilter === "all" || e.priority === priorityFilter)

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
        <SiteHeader title="Live Feed" />

        {/* Connection status bar */}
        <div className={`flex items-center gap-2 border-b px-4 py-2 text-sm ${connected ? "bg-green-50 dark:bg-green-950" : "bg-yellow-50 dark:bg-yellow-950"}`}>
          <span className={`h-2 w-2 rounded-full ${connected ? "bg-green-500" : "bg-yellow-500 animate-pulse"}`} />
          <span className={connected ? "text-green-700 dark:text-green-400" : "text-yellow-700 dark:text-yellow-400"}>
            {connected ? "Connected · receiving events" : "Reconnecting..."}
          </span>
        </div>

        <div className="flex flex-1 flex-col gap-4 p-4 md:p-6">
          {/* Filter controls */}
          <div className="flex flex-wrap items-center gap-2">
            <div className="flex items-center gap-1">
              {CHANNEL_OPTS.map(c => (
                <Button
                  key={c}
                  size="sm"
                  variant={channelFilter === c ? "default" : "outline"}
                  onClick={() => setChannelFilter(c)}
                  className="capitalize h-7 text-xs"
                >
                  {c}
                </Button>
              ))}
            </div>
            <div className="flex items-center gap-1">
              {PRIORITY_OPTS.map(p => (
                <Button
                  key={p}
                  size="sm"
                  variant={priorityFilter === p ? "default" : "outline"}
                  onClick={() => setPriorityFilter(p)}
                  className="capitalize h-7 text-xs"
                >
                  {p}
                </Button>
              ))}
            </div>
            <div className="ml-auto flex items-center gap-2">
              <Badge variant="outline">{filtered.length} events</Badge>
              <Button size="sm" variant="ghost" className="h-7 text-xs" onClick={clear}>
                Clear
              </Button>
            </div>
          </div>

          {/* Event feed */}
          {loading ? (
            <div className="flex flex-col gap-2">
              {[0, 1, 2].map(i => <Skeleton key={i} className="h-24 w-full rounded-lg" />)}
            </div>
          ) : filtered.length === 0 ? (
            <div className="flex flex-col items-center justify-center gap-3 py-16 text-center">
              <ActivityIcon className="size-10 text-muted-foreground" />
              <p className="font-medium">Waiting for events</p>
              <p className="text-sm text-muted-foreground">
                Open another tab and publish an event to see it appear here in real time
              </p>
              <Button size="sm" variant="outline" onClick={() => window.location.href = "/publish"}>
                → Go to Publish
              </Button>
            </div>
          ) : (
            <div className="flex flex-col gap-2">
              {filtered.map(event => (
                <EventCard key={`${event.message_id}-${event.timestamp}`} event={event} />
              ))}
            </div>
          )}
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
