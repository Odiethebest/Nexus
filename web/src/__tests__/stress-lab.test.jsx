import { fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { StressLabPanel } from '../App.jsx'

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function enterAdminKey(value = 'secret-key') {
  fireEvent.change(screen.getByPlaceholderText(/x-admin-key/i), { target: { value } })
}

describe('StressLabPanel state machine', () => {
  it('transitions idle -> running after start and initial sync', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ run_id: 101, status: 'created' }, 202))
      .mockResolvedValueOnce(jsonResponse({
        run: { status: 'running' },
        health_score: 73,
        snapshot: {
          rps: 48.7,
          p95_ms: 88.2,
          error_rate_pct: 0.12,
          vus: 22,
          insight: 'Traffic is ramping smoothly.',
        },
        series: { rps: [[1, 12], [2, 48.7]] },
        signals: ['steady ramp'],
        warnings: [],
      }))
    vi.stubGlobal('fetch', fetchMock)

    render(<StressLabPanel />)
    enterAdminKey()

    fireEvent.click(screen.getByRole('button', { name: /start load test/i }))

    await screen.findByText('STATUS: running')
    expect(screen.getByText('PHASE: running')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /load test running/i })).toBeDisabled()
    expect(fetchMock).toHaveBeenNthCalledWith(1, '/ops/loadtest/start', expect.objectContaining({
      method: 'POST',
      headers: expect.objectContaining({ 'Content-Type': 'application/json' }),
    }))
    expect(fetchMock).toHaveBeenNthCalledWith(2, '/ops/loadtest/101')
  })

  it('transitions to completed and shows final summary', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ run_id: 7, status: 'created' }, 202))
      .mockResolvedValueOnce(jsonResponse({
        run: { status: 'completed' },
        health_score: 91,
        snapshot: {
          rps: 120.5,
          p95_ms: 109.0,
          error_rate_pct: 0.23,
          vus: 64,
          insight: 'Final snapshot shows stable latency.',
        },
        series: { rps: [[1, 72], [2, 120.5]] },
        signals: ['Throughput held steady'],
        warnings: [],
      }))
    vi.stubGlobal('fetch', fetchMock)

    render(<StressLabPanel />)
    enterAdminKey()

    fireEvent.click(screen.getByRole('button', { name: /start load test/i }))

    await screen.findByText('STATUS: completed')
    expect(screen.getByText('PHASE: completed')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /run completed/i })).toBeEnabled()
    expect(screen.getByText('FINAL SCORE')).toBeInTheDocument()
  })
})

describe('StressLabPanel API contract parsing', () => {
  it('parses numeric strings and warnings from status payload', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ run_id: '33', status: 'queued' }, 202))
      .mockResolvedValueOnce(jsonResponse({
        run: { status: 'processing_metrics' },
        health_score: '88',
        snapshot: {
          rps: '99.9',
          p95_ms: '111.2',
          error_rate_pct: '0.45',
          vus: '42',
          insight: 'Metrics parsed from contract.',
        },
        series: { rps: [[1, '60.0'], [2, '99.9']] },
        signals: ['signal_a', null],
        warnings: [null, 'sampling lag'],
      }))
    vi.stubGlobal('fetch', fetchMock)

    const { container } = render(<StressLabPanel />)
    enterAdminKey()

    fireEvent.click(screen.getByRole('button', { name: /start load test/i }))

    await screen.findByText('STATUS: processing_metrics')
    expect(screen.getByText('PHASE: analyzing')).toBeInTheDocument()
    expect(screen.getByText('Signal warning: sampling lag')).toBeInTheDocument()
    expect(screen.getByText('Metrics parsed from contract.')).toBeInTheDocument()

    const cards = container.querySelectorAll('.stress-card')
    expect(within(cards[0]).getByText('99.9')).toBeInTheDocument()
    expect(within(cards[1]).getByText('111.2')).toBeInTheDocument()
    expect(within(cards[2]).getByText('0.45%')).toBeInTheDocument()
    expect(within(cards[3]).getByText('42')).toBeInTheDocument()
  })

  it('extracts cooldown countdown from start error JSON', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ error: 'start throttled by cooldown, retry in 1m5s' }, 429))
    vi.stubGlobal('fetch', fetchMock)

    render(<StressLabPanel />)
    enterAdminKey()

    fireEvent.click(screen.getByRole('button', { name: /start load test/i }))

    await screen.findByText('// ERR: start throttled by cooldown, retry in 1m5s')
    expect(screen.getByText('Try again in 01:05')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /start load test/i })).toBeDisabled()
  })
})

describe('StressLabPanel snapshots', () => {
  it('matches idle state snapshot', () => {
    const { container } = render(<StressLabPanel />)
    expect(container.querySelector('.stress-panel')).toMatchSnapshot()
  })

  it('matches completed state snapshot', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ run_id: 52, status: 'created' }, 202))
      .mockResolvedValueOnce(jsonResponse({
        run: { status: 'completed' },
        health_score: 84,
        snapshot: {
          rps: 88.4,
          p95_ms: 103.3,
          error_rate_pct: 0.39,
          vus: 35,
          insight: 'Run closed with healthy margins.',
        },
        series: { rps: [[1, 40], [2, 88.4]] },
        signals: ['Saturation stayed below threshold'],
        warnings: [],
      }))
    vi.stubGlobal('fetch', fetchMock)

    const { container } = render(<StressLabPanel />)
    enterAdminKey()

    fireEvent.click(screen.getByRole('button', { name: /start load test/i }))
    await screen.findByText('STATUS: completed')

    expect(container.querySelector('.stress-panel')).toMatchSnapshot()
  })
})
