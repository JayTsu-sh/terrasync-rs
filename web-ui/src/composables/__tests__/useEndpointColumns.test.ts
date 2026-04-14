import { describe, it, expect } from 'vitest'
import { ref } from 'vue'
import { useEndpointColumns } from '../useEndpointColumns'

describe('useEndpointColumns - 列定义', () => {
  const connectivityStatus = ref<Record<string, 'testing' | 'ok' | 'fail'>>({})
  const callbacks = { onDetail: () => {}, onDelete: () => {} }

  const { columns } = useEndpointColumns(connectivityStatus, callbacks)
  const cols = columns as any[]

  it('列顺序: 名称 → 连接状态 → 类型 → 存储地址 → 创建时间 → 操作', () => {
    const keys = cols.map((c) => c.key)
    expect(keys).toEqual(['name', 'connectivity', 'endpoint_type', 'summary', 'created_at', 'actions'])
  })

  it('共 6 列', () => {
    expect(cols).toHaveLength(6)
  })

  it('名称列可排序', () => {
    const nameCol = cols.find((c) => c.key === 'name')
    expect(nameCol?.sorter).toBeDefined()
  })

  it('连接状态列有筛选选项', () => {
    const connCol = cols.find((c) => c.key === 'connectivity')
    expect(connCol.filterOptions).toBeDefined()
    expect(connCol.filterOptions).toHaveLength(3)
    const labels = connCol.filterOptions.map((o: any) => o.label)
    expect(labels).toContain('已连接')
    expect(labels).toContain('连接失败')
    expect(labels).toContain('检测中')
  })

  it('操作列在最后', () => {
    expect(cols[cols.length - 1].key).toBe('actions')
  })

  it('操作列居中对齐', () => {
    const actionsCol = cols.find((c) => c.key === 'actions')
    expect(actionsCol.align).toBe('center')
  })
})
