"use client"

import React, { useState, useEffect } from "react"
import { Loader2Icon } from "lucide-react"
import { toast } from "sonner"
import { AppSidebar } from "@/components/app-sidebar"
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
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { postEvent } from "@/lib/api"

const EVENT_TYPES = [
  "payment.completed",
  "payment.failed",
  "order.shipped",
  "order.cancelled",
  "user.signup",
  "user.deleted",
  "alert.critical",
  "alert.warning",
]

const PRIORITIES = ["high", "normal", "low"] as const
type Priority = (typeof PRIORITIES)[number]

const PAYLOAD_TEMPLATES: Record<string, string> = {
  "payment.completed": JSON.stringify({ amount: 99.99, currency: "USD", customer_id: "cust_123" }, null, 2),
  "order.shipped":     JSON.stringify({ order_id: "ORD-456", tracking: "1Z999AA10123456784" }, null, 2),
  "user.signup":       JSON.stringify({ email: "user@example.com", plan: "free" }, null, 2),
}

const DEFAULT_EVENT_TYPE = "payment.completed"
const DEFAULT_PRIORITY: Priority = "normal"

export default function PublishPage() {
  const [eventType, setEventType]       = useState(DEFAULT_EVENT_TYPE)
  const [priority, setPriority]         = useState<Priority>(DEFAULT_PRIORITY)
  const [payload, setPayload]           = useState(PAYLOAD_TEMPLATES[DEFAULT_EVENT_TYPE])
  const [payloadError, setPayloadError] = useState<string | null>(null)
  const [submitting, setSubmitting]     = useState(false)

  useEffect(() => { document.title = "Publish Event — Nexus" }, [])

  const handleEventTypeChange = (value: string) => {
    setEventType(value)
    setPayload(PAYLOAD_TEMPLATES[value] ?? "{}")
    setPayloadError(null)
  }

  const handlePayloadChange = (value: string) => {
    setPayload(value)
    if (payloadError) setPayloadError(null)
  }

  const handleSubmit = async () => {
    let parsedPayload: unknown
    try {
      parsedPayload = JSON.parse(payload)
    } catch {
      setPayloadError("Invalid JSON")
      return
    }

    setSubmitting(true)
    try {
      const result = await postEvent({ type: eventType, priority, payload: parsedPayload })
      toast.success(`Event published — message_id: ${result.message_id}`, {
        action: {
          label: "View in Live Feed →",
          onClick: () => { window.location.href = "/live" },
        },
      })
      setEventType(DEFAULT_EVENT_TYPE)
      setPriority(DEFAULT_PRIORITY)
      setPayload(PAYLOAD_TEMPLATES[DEFAULT_EVENT_TYPE])
      setPayloadError(null)
    } catch (e) {
      toast.error(String(e))
    } finally {
      setSubmitting(false)
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
        <SiteHeader title="Publish Event" />
        <div className="flex flex-1 flex-col gap-2 p-4 md:p-6">
          <p className="text-sm text-muted-foreground">
            Send a test event into the Nexus pipeline
          </p>
          <Card>
            <CardHeader>
              <CardTitle>New Event</CardTitle>
              <CardDescription>Configure and publish a message to the exchange</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-5">
              {/* Event Type */}
              <div className="flex flex-col gap-2">
                <Label htmlFor="event-type">Event Type</Label>
                <Select value={eventType} onValueChange={handleEventTypeChange}>
                  <SelectTrigger id="event-type">
                    <SelectValue placeholder="Select event type" />
                  </SelectTrigger>
                  <SelectContent>
                    {EVENT_TYPES.map(t => (
                      <SelectItem key={t} value={t}>{t}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Priority */}
              <div className="flex flex-col gap-2">
                <Label>Priority</Label>
                <div className="flex gap-2">
                  {PRIORITIES.map(p => (
                    <Button
                      key={p}
                      variant={priority === p ? "default" : "outline"}
                      size="sm"
                      onClick={() => setPriority(p)}
                      className="capitalize"
                    >
                      {p}
                    </Button>
                  ))}
                </div>
              </div>

              {/* Payload */}
              <div className="flex flex-col gap-2">
                <Label htmlFor="payload">Payload</Label>
                <Textarea
                  id="payload"
                  rows={6}
                  className="font-mono text-sm"
                  value={payload}
                  onChange={e => handlePayloadChange(e.target.value)}
                />
                {payloadError && (
                  <p className="text-sm text-red-500">{payloadError}</p>
                )}
              </div>

              {/* Submit */}
              <Button
                className="w-full"
                disabled={submitting}
                onClick={handleSubmit}
              >
                {submitting ? (
                  <>
                    <Loader2Icon className="mr-2 h-4 w-4 animate-spin" />
                    Publishing...
                  </>
                ) : (
                  "Publish Event"
                )}
              </Button>
            </CardContent>
          </Card>
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
