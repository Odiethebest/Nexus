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
] as const

type EventType = (typeof EVENT_TYPES)[number]
type Priority  = "high" | "normal" | "low"

const PAYLOAD_TEMPLATES: Partial<Record<EventType, string>> = {
  "payment.completed": '{\n  "amount": 99.99,\n  "currency": "USD",\n  "customer_id": "cust_123"\n}',
  "payment.failed":    '{\n  "amount": 49.99,\n  "currency": "USD",\n  "error_code": "insufficient_funds",\n  "customer_id": "cust_123"\n}',
  "order.shipped":     '{\n  "order_id": "ORD-456",\n  "tracking": "1Z999AA10123456784"\n}',
  "order.cancelled":   '{\n  "order_id": "ORD-789",\n  "reason": "customer_request",\n  "refund_amount": 29.99\n}',
  "user.signup":       '{\n  "email": "user@example.com",\n  "plan": "free"\n}',
  "user.deleted":      '{\n  "user_id": "usr_456",\n  "reason": "gdpr_request"\n}',
  "alert.critical":    '{\n  "service": "payment-gateway",\n  "message": "High error rate detected",\n  "threshold": 0.05,\n  "current": 0.12\n}',
  "alert.warning":     '{\n  "service": "worker",\n  "message": "Queue depth exceeding threshold",\n  "queue": "email.high",\n  "depth": 450\n}',
}

const DEFAULT_EVENT_TYPE: EventType = "payment.completed"
const DEFAULT_PRIORITY: Priority    = "normal"
const DEFAULT_PAYLOAD               = PAYLOAD_TEMPLATES[DEFAULT_EVENT_TYPE]!

export default function PublishPage() {
  // Single state object keeps eventType + payload in sync atomically —
  // avoids the React 19 / Radix Select controlled-value race where two
  // separate setState calls can be processed across render boundaries.
  const [form, setForm] = useState<{ eventType: EventType; payload: string }>({
    eventType: DEFAULT_EVENT_TYPE,
    payload:   DEFAULT_PAYLOAD,
  })
  const [priority,     setPriority]     = useState<Priority>(DEFAULT_PRIORITY)
  const [payloadError, setPayloadError] = useState<string | null>(null)
  const [submitting,   setSubmitting]   = useState(false)

  useEffect(() => { document.title = "Publish Event — Nexus" }, [])

  const handleEventTypeChange = (value: string) => {
    const et = value as EventType
    setForm({ eventType: et, payload: PAYLOAD_TEMPLATES[et] ?? "{}" })
    setPayloadError(null)
  }

  const handlePayloadChange = (value: string) => {
    setForm(prev => ({ ...prev, payload: value }))
    if (payloadError) setPayloadError(null)
  }

  const handleSubmit = async () => {
    let parsedPayload: unknown
    try {
      parsedPayload = JSON.parse(form.payload)
    } catch {
      setPayloadError("Invalid JSON")
      return
    }

    setSubmitting(true)
    try {
      const body = { type: form.eventType, priority, payload: parsedPayload }
      console.log("[publish] POST /events body:", JSON.stringify(body))
      const result = await postEvent(body)
      console.log("[publish] response:", result)
      toast.success(`Event published — message_id: ${result.message_id}`, {
        action: {
          label: "View in Live Feed →",
          onClick: () => { window.location.href = "/live" },
        },
      })
      // Reset only the payload to the template for the current event type —
      // do not reset eventType so the user stays on what they were publishing.
      setForm(prev => ({ ...prev, payload: PAYLOAD_TEMPLATES[prev.eventType] ?? "{}" }))
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
                <Select
                  value={form.eventType}
                  onValueChange={handleEventTypeChange}
                >
                  <SelectTrigger id="event-type" className="w-full">
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
                  <Button
                    type="button"
                    size="sm"
                    variant={priority === "high" ? "default" : "outline"}
                    onClick={() => setPriority("high")}
                    className="capitalize"
                  >
                    high
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant={priority === "normal" ? "default" : "outline"}
                    onClick={() => setPriority("normal")}
                    className="capitalize"
                  >
                    normal
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant={priority === "low" ? "default" : "outline"}
                    onClick={() => setPriority("low")}
                    className="capitalize"
                  >
                    low
                  </Button>
                </div>
              </div>

              {/* Payload */}
              <div className="flex flex-col gap-2">
                <Label htmlFor="payload">Payload</Label>
                <Textarea
                  id="payload"
                  rows={6}
                  className="font-mono text-sm"
                  value={form.payload}
                  onChange={e => handlePayloadChange(e.target.value)}
                />
                {payloadError && (
                  <p className="text-sm text-red-500">{payloadError}</p>
                )}
              </div>

              {/* Submit */}
              <Button
                type="button"
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
