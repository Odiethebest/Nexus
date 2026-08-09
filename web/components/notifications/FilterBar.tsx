"use client"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import type { Channel, Priority, Status } from "@/types"

type ChannelFilter = "all" | Channel
type StatusFilter   = "all" | Status
type PriorityFilter = "all" | Priority

interface FilterBarProps {
  channel:       ChannelFilter
  status:        StatusFilter
  priority:      PriorityFilter
  search:        string
  count:         number
  onChannel:     (v: ChannelFilter) => void
  onStatus:      (v: StatusFilter)  => void
  onPriority:    (v: PriorityFilter) => void
  onSearch:      (v: string) => void
  onClear:       () => void
  hasActiveFilter: boolean
}

const CHANNELS: ChannelFilter[] = ["all", "email", "inapp", "webhook"]
// Mirrors the Status union — "duplicate" used to be offered here but never
// matched a row, and "skipped" (the usual webhook outcome) was missing.
const PRIORITIES: PriorityFilter[] = ["all", "high", "normal", "low"]
const STATUSES: StatusFilter[]  = ["all", "delivered", "skipped", "failed", "dlq"]

export function FilterBar({
  channel, status, priority, search, count,
  onChannel, onStatus, onPriority, onSearch, onClear,
  hasActiveFilter,
}: FilterBarProps) {
  return (
    <div className="flex flex-wrap items-center gap-2 px-4 lg:px-6">
      {/* Channel */}
      <div className="flex items-center gap-1">
        {CHANNELS.map(c => (
          <Button
            key={c}
            size="sm"
            variant={channel === c ? "default" : "outline"}
            onClick={() => onChannel(c)}
            className="capitalize h-7 text-xs"
          >
            {c}
          </Button>
        ))}
      </div>

      {/* Status */}
      <div className="flex items-center gap-1">
        {STATUSES.map(s => (
          <Button
            key={s}
            size="sm"
            variant={status === s ? "default" : "outline"}
            onClick={() => onStatus(s)}
            className="capitalize h-7 text-xs"
          >
            {s}
          </Button>
        ))}
      </div>

      {/* Priority */}
      <div className="flex items-center gap-1">
        {PRIORITIES.map(p => (
          <Button
            key={p}
            size="sm"
            variant={priority === p ? "default" : "outline"}
            onClick={() => onPriority(p)}
            className="capitalize h-7 text-xs"
          >
            {p}
          </Button>
        ))}
      </div>

      {/* Search */}
      <Input
        placeholder="Search event type…"
        value={search}
        onChange={e => onSearch(e.target.value)}
        className="h-7 w-44 text-xs"
      />

      {/* Clear filters */}
      {hasActiveFilter && (
        <Button size="sm" variant="ghost" className="h-7 text-xs" onClick={onClear}>
          Clear All
        </Button>
      )}

      {/* Count */}
      <span className="ml-auto text-xs text-muted-foreground">
        {count} notification{count !== 1 ? "s" : ""}
      </span>
    </div>
  )
}
