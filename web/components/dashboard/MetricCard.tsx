import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'

type Variant = 'good' | 'bad' | 'neutral'

interface MetricCardProps {
  title:   string
  value:   number | null
  unit:    string
  variant: Variant
  loading: boolean
}

const variantMap: Record<Variant, string> = {
  good:    'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
  bad:     'bg-red-500/15 text-red-400 border-red-500/30',
  neutral: 'bg-muted text-muted-foreground border-border',
}

export function MetricCard({ title, value, unit, variant, loading }: MetricCardProps) {
  return (
    <Card className="bg-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent className="flex items-end justify-between gap-2">
        {loading || value === null ? (
          <Skeleton className="h-9 w-24" />
        ) : (
          <span className="text-3xl font-bold tabular-nums text-foreground">
            {typeof value === 'number' && !Number.isInteger(value)
              ? value.toFixed(1)
              : value}
          </span>
        )}
        <Badge
          variant="outline"
          className={variantMap[variant]}
        >
          {unit}
        </Badge>
      </CardContent>
    </Card>
  )
}
