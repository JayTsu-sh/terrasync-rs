import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { cancelTask, type MigrationTask } from '../tasks'

const task: MigrationTask = {
  id: 'scan', name: 'scan', task_type: 'scan', source_endpoint_id: 'source',
  status: 'running', config: {}, created_at: '',
}

beforeEach(() => vi.useFakeTimers())
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); vi.unstubAllGlobals() })

describe('cancelTask completion confirmation', () => {
  it('waits for the backend to stop instead of resolving when cancellation is only accepted', async () => {
    let status: MigrationTask['status'] = 'running'
    const fetchMock = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) =>
      init?.method === 'POST' ? new Response(null, { status: 200 }) : Response.json({ ...task, status }),
    )
    vi.stubGlobal('fetch', fetchMock)
    let finished = false
    const cancellation = cancelTask(task.id).then(() => { finished = true })
    await vi.advanceTimersByTimeAsync(0)
    expect(finished).toBe(false)
    status = 'cancelled'
    await vi.advanceTimersByTimeAsync(200)
    await cancellation
    expect(finished).toBe(true)
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'POST')).toHaveLength(1)
    expect(fetchMock.mock.calls.every(([url]) => !String(url).includes('/executions'))).toBe(true)
  })

  it('does not poll if the cancellation request is rejected', async () => {
    const fetchMock = vi.fn().mockResolvedValue(Response.json({ error: 'Cannot cancel' }, { status: 409 }))
    vi.stubGlobal('fetch', fetchMock)
    await expect(cancelTask(task.id)).rejects.toThrow('Cannot cancel')
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('does not report success if the backend stays running', async () => {
    vi.stubGlobal('fetch', vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) =>
      init?.method === 'POST' ? new Response(null, { status: 200 }) : Response.json(task),
    ))
    const assertion = expect(cancelTask(task.id)).rejects.toThrow('取消请求已发送')
    await vi.advanceTimersByTimeAsync(30_000)
    await assertion
  })
})
