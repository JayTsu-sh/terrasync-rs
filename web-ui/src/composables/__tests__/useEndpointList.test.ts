import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('../../api/endpoints', () => ({
  listEndpoints: vi.fn().mockResolvedValue([]),
  testConnection: vi.fn().mockResolvedValue(undefined),
  deleteEndpoint: vi.fn().mockResolvedValue(undefined),
}))

vi.mock('../usePolling', () => ({
  usePolling: () => ({ start: vi.fn(), stop: vi.fn() }),
}))

import { useEndpointList } from '../useEndpointList'
import type { Endpoint } from '../../api/endpoints'

function makeEndpoint(overrides: Partial<Endpoint> & { id: string; name: string }): Endpoint {
  return {
    endpoint_type: 'local',
    config: { type: 'local', path: '/data/test' },
    created_at: '2024-01-01T00:00:00Z',
    ...overrides,
  } as Endpoint
}

describe('useEndpointList - 搜索过滤', () => {
  let list: ReturnType<typeof useEndpointList>

  const sampleEndpoints = [
    makeEndpoint({
      id: '1',
      name: '生产NFS存储',
      endpoint_type: 'nfs',
      config: { type: 'nfs', server: '10.0.0.1', port: 2049, export_path: '/data' },
    }),
    makeEndpoint({
      id: '2',
      name: '备份S3存储',
      endpoint_type: 's3',
      config: {
        type: 's3',
        host: 'backup.s3.example.com',
        port: 9000,
        access_key: 'ak',
        secret_key: 'sk',
        bucket: 'backup',
      },
    }),
    makeEndpoint({
      id: '3',
      name: '本地测试目录',
      endpoint_type: 'local',
      config: { type: 'local', path: '/data/test/dataset' },
    }),
  ]

  beforeEach(() => {
    list = useEndpointList()
    // 直接设置 endpoints（绕过 API 调用）
    list.endpoints.value = sampleEndpoints
  })

  it('searchQuery 为空时返回全部 endpoints', () => {
    list.searchQuery.value = ''
    expect(list.filteredEndpoints.value).toHaveLength(3)
  })

  it('按名称搜索能正确过滤', () => {
    list.searchQuery.value = 'NFS'
    expect(list.filteredEndpoints.value).toHaveLength(1)
    expect(list.filteredEndpoints.value[0].name).toBe('生产NFS存储')
  })

  it('按地址搜索能正确过滤', () => {
    list.searchQuery.value = 'backup.s3'
    expect(list.filteredEndpoints.value).toHaveLength(1)
    expect(list.filteredEndpoints.value[0].name).toBe('备份S3存储')
  })

  it('搜索大小写不敏感', () => {
    list.searchQuery.value = 'nfs'
    expect(list.filteredEndpoints.value).toHaveLength(1)
    expect(list.filteredEndpoints.value[0].name).toBe('生产NFS存储')
  })

  it('搜索无结果时返回空数组', () => {
    list.searchQuery.value = '不存在的目标'
    expect(list.filteredEndpoints.value).toHaveLength(0)
  })

  it('搜索空白字符串等同于空搜索', () => {
    list.searchQuery.value = '   '
    expect(list.filteredEndpoints.value).toHaveLength(3)
  })
})
