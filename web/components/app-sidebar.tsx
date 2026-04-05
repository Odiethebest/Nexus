"use client"

import * as React from "react"

import { NavDocuments } from "@/components/nav-documents"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"
import {
  LayoutDashboardIcon,
  ActivityIcon,
  BellIcon,
  ZapIcon,
  InboxIcon,
  SendIcon,
  CommandIcon,
} from "lucide-react"

const data = {
  system: [
    { name: "Dashboard", url: "/dashboard", icon: <LayoutDashboardIcon /> },
    { name: "Live Feed", url: "/live", icon: <ActivityIcon /> },
    { name: "Notifications", url: "/notifications", icon: <BellIcon /> },
  ],
  operations: [
    { name: "Load Test", url: "/loadtest", icon: <ZapIcon /> },
    { name: "DLQ", url: "/dlq", icon: <InboxIcon /> },
    { name: "Publish", url: "/publish", icon: <SendIcon /> },
  ],
}

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  return (
    <Sidebar collapsible="offcanvas" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              asChild
              className="data-[slot=sidebar-menu-button]:p-1.5!"
            >
              <a href="/dashboard">
                <CommandIcon className="size-5!" />
                <span className="text-base font-semibold">Nexus</span>
              </a>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <NavDocuments items={data.system} label="System" />
        <NavDocuments items={data.operations} label="Operations" />
      </SidebarContent>
      <SidebarFooter>
        <div className="px-3 py-2 text-xs text-muted-foreground">Nexus — notification system</div>
      </SidebarFooter>
    </Sidebar>
  )
}
