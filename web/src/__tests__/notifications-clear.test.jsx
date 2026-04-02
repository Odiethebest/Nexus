import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import App from '../App.jsx'

const CLEAR_KEY = 'nexus.notifications.clear_after'

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function installWebSocketMock() {
  class FakeWebSocket {
    constructor(url) {
      this.url = url
      this.readyState = 1
      setTimeout(() => this.onopen?.(), 0)
    }

    close() {
      this.readyState = 3
      this.onclose?.()
    }

    send() {}
  }

  vi.stubGlobal('WebSocket', FakeWebSocket)
}

function storedNotificationRow({
  messageID = 'msg-1',
  type = 'order.created',
  priority = 'high',
  payload = { order_id: 'ord-1' },
  timestamp = '2026-04-01T12:00:00Z',
} = {}) {
  return {
    message_id: messageID,
    channel: 'inapp',
    event_type: type,
    status: 'delivered',
    payload: JSON.stringify({
      message_id: messageID,
      type,
      priority,
      payload,
      timestamp,
    }),
    created_at: timestamp,
  }
}

describe('Live notifications clear behavior', () => {
  it('clears visible notifications and stores clear cutoff', async () => {
    installWebSocketMock()

    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse([storedNotificationRow()]))
    vi.stubGlobal('fetch', fetchMock)

    render(<App />)

    await screen.findByText('order.created')
    expect(screen.getByText('1 EVT')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /clear/i }))

    await waitFor(() => {
      expect(screen.getByText('0 EVT')).toBeInTheDocument()
    })
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/notifications/clear',
        expect.objectContaining({
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        }),
      )
    })
    expect(screen.queryByText('order.created')).not.toBeInTheDocument()

    const rawCutoff = localStorage.getItem(CLEAR_KEY)
    expect(rawCutoff).not.toBeNull()
    expect(Number(rawCutoff)).toBeGreaterThan(0)
  })

  it('keeps old notifications hidden after refresh when clear cutoff exists', async () => {
    installWebSocketMock()

    localStorage.setItem(CLEAR_KEY, String(Date.parse('2026-04-01T13:00:00Z')))

    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse([
          storedNotificationRow({
            messageID: 'msg-old',
            timestamp: '2026-04-01T12:00:00Z',
          }),
        ]),
      )
    vi.stubGlobal('fetch', fetchMock)

    render(<App />)

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith('/notifications')
    })

    await waitFor(() => {
      expect(screen.getByText('0 EVT')).toBeInTheDocument()
    })
    expect(screen.queryByText('order.created')).not.toBeInTheDocument()
    expect(screen.getByText('Awaiting Signal')).toBeInTheDocument()
  })
})
