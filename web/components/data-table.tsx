"use client"

import * as React from "react"
import {
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table"
import { formatDistanceToNow } from "date-fns"
import { BellIcon } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import type { Notification } from "@/types"

function RelativeTime({ iso }: { iso: string }) {
  const [label, setLabel] = React.useState(() =>
    formatDistanceToNow(new Date(iso), { addSuffix: true })
  )

  React.useEffect(() => {
    const id = setInterval(() => {
      setLabel(formatDistanceToNow(new Date(iso), { addSuffix: true }))
    }, 30000)
    return () => clearInterval(id)
  }, [iso])

  return <span className="text-sm text-muted-foreground">{label}</span>
}

const channelClass: Record<string, string> = {
  email:   "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
  inapp:   "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
  webhook: "bg-pink-100 text-pink-800 dark:bg-pink-900 dark:text-pink-200",
}

// Keys are the statuses the worker actually persists (see the Status union).
const statusClass: Record<string, string> = {
  delivered: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
  skipped:   "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-200",
  failed:    "bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200",
  dlq:       "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
}

const columns: ColumnDef<Notification>[] = [
  {
    accessorKey: "message_id",
    header: "ID",
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground">
        {row.original.message_id.slice(0, 8)}
      </span>
    ),
  },
  {
    accessorKey: "channel",
    header: "Channel",
    cell: ({ row }) => (
      <Badge className={channelClass[row.original.channel] ?? ""} variant="outline">
        {row.original.channel}
      </Badge>
    ),
  },
  {
    accessorKey: "event_type",
    header: "Event Type",
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ row }) => (
      <Badge className={statusClass[row.original.status] ?? ""} variant="outline">
        {row.original.status}
      </Badge>
    ),
  },
  {
    accessorKey: "created_at",
    header: "Time",
    cell: ({ row }) => <RelativeTime iso={row.original.created_at} />,
  },
]

export function DataTable({ data, loading }: { data: Notification[] | null; loading?: boolean }) {
  // Memoised so the identity is stable across renders; a bare `data ?? []`
  // allocates a new array every render and defeats the memo below.
  const safeData = React.useMemo(() => data ?? [], [data])

  const groupedIds = React.useMemo(() => {
    const counts = new Map<string, number>()
    safeData.forEach(n => counts.set(n.message_id, (counts.get(n.message_id) ?? 0) + 1))
    return new Set([...counts.entries()].filter(([, c]) => c > 1).map(([id]) => id))
  }, [safeData])

  const table = useReactTable({
    data: safeData,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: 50 } },
  })

  if (loading && safeData.length === 0) {
    return (
      <div className="flex flex-col gap-4 px-4 lg:px-6">
        <div className="overflow-hidden rounded-lg border">
          <Table>
            <TableHeader className="sticky top-0 z-10 bg-muted">
              <TableRow>
                <TableHead>ID</TableHead>
                <TableHead>Channel</TableHead>
                <TableHead>Event Type</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Time</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {[0, 1, 2, 3, 4].map(i => (
                <TableRow key={i}>
                  <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                  <TableCell><Skeleton className="h-5 w-14 rounded-full" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-32" /></TableCell>
                  <TableCell><Skeleton className="h-5 w-18 rounded-full" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </div>
    )
  }

  if (safeData.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-16 text-center px-4 lg:px-6">
        <BellIcon className="size-10 text-muted-foreground" />
        <p className="font-medium">No notifications yet</p>
        <p className="text-sm text-muted-foreground">
          Events will appear once the system receives messages
        </p>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-4 px-4 lg:px-6">
      <div className="overflow-hidden rounded-lg border">
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-muted">
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <TableHead key={header.id}>
                    {header.isPlaceholder
                      ? null
                      : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.map((row) => (
              <TableRow
                key={row.id}
                className={groupedIds.has(row.original.message_id) ? "border-l-2 border-muted" : ""}
              >
                {row.getVisibleCells().map((cell) => (
                  <TableCell key={cell.id}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      {table.getPageCount() > 1 && (
        <div className="flex items-center justify-end gap-2">
          <span className="text-sm text-muted-foreground">
            Page {table.getState().pagination.pageIndex + 1} of {table.getPageCount()}
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
          >
            Previous
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
          >
            Next
          </Button>
        </div>
      )}
    </div>
  )
}
