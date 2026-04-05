import { NextResponse } from 'next/server'

const METRICS_URL = process.env.METRICS_INTERNAL_URL ?? 'http://localhost:9091'

export async function GET() {
  try {
    const res = await fetch(`${METRICS_URL}/metrics`, {
      headers: { Accept: 'text/plain' },
      next: { revalidate: 0 },
    })
    const text = await res.text()
    return new NextResponse(text, {
      headers: { 'Content-Type': 'text/plain; version=0.0.4' },
    })
  } catch (err) {
    return NextResponse.json(
      { error: 'Failed to reach metrics endpoint', detail: String(err) },
      { status: 502 }
    )
  }
}
