export function TopBar({ title }: { title: string }) {
  return (
    <header className="flex h-14 items-center border-b border-border bg-card px-6">
      <h1 className="text-sm font-medium text-foreground">{title}</h1>
    </header>
  )
}
