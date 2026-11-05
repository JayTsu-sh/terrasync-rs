import { describe, expect, it } from 'vitest'
import { getListActions } from './task-rules'
describe('scan resume actions', () => {
  it('offers cancellation while running and resume after interruption', () => {
    expect(getListActions('running', 'scan').find(a => a.key === 'cancel')?.label).toBe('取消')
    expect(getListActions('cancelled', 'scan', true).find(a => a.key === 'start')?.label).toBe('继续扫描')
    expect(getListActions('failed', 'scan', true).find(a => a.key === 'start')?.label).toBe('继续扫描')
    expect(getListActions('cancelled', 'scan', false).find(a => a.key === 'start')?.label).toBe('重新扫描')
    expect(getListActions('cancelled', 'sync').find(a => a.key === 'start')?.label).toBe('启动')
  })
})
