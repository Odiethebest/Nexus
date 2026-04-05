"use client"

import React, { useState, useEffect } from "react"
import { formatDistanceToNow } from "date-fns"
import { Badge } from "@/components/ui/badge"
import type { WsEvent } from "@/types"

const channelClass: Record<string, string> = {
  email:   "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
  inapp:   "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
  webhook: "bg-pink-100 text-pink-800 dark:bg-pink-900 dark:text-pink-200",
}

const priorityClass: Record<string, string> = {
  high:   "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
  normal: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
  low:    "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300",
}

function RelativeTime({ timestamp }: { timestamp: string }) {
  const [label, setLabel] = useState("")

  useEffect(() => {
    const update = () =>
      setLabel(formatDistanceToNow(new Date(timestamp), { addSuffix: true }))
    update()
    const id = setInterval(update, 1000)
    return () => clearInterval(id)
  }, [timestamp])

  return <span className="text-xs text-muted-foreground">{label}</span>
}

export function EventCard({ event }: { event: WsEvent }) {
  const [expanded, setExpanded] = useState(false)
  const payloadStr = JSON.stringify(event.payload, null, 2)
  // All live events arrive via InAppWorker — the only worker that broadcasts to WS.
  const priority = event.priority ?? "normal"
  const channel  = event.channel  ?? "inapp"

  return (
    <div className="rounded-lg border bg-card p-3 shadow-xs animate-in fade-in duration-300">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <Badge className={channelClass[channel] ?? channelClass["inapp"]} variant="outline">
            {channel}
          </Badge>
          <span className="text-sm font-medium">{event.type}</span>
          <Badge className={priorityClass[priority]} variant="outline">
            {priority}
          </Badge>
        </div>
        <RelativeTime timestamp={event.timestamp} />
      </div>
      <p className="mt-1.5 text-xs text-muted-foreground font-mono">
        message_id: {event.message_id}
      </p>
      <pre
        className={`mt-2 text-xs font-mono bg-muted rounded p-2 cursor-pointer overflow-hidden ${expanded ? "" : "line-clamp-3"}`}
        onClick={() => setExpanded(e => !e)}
      >
        {payloadStr}
      </pre>
    </div>
  )
}
