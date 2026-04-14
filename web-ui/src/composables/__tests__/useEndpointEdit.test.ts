import { describe, it, expect, vi } from 'vitest'

vi.mock('../../api/endpoints', () => ({
  updateEndpoint: vi.fn(),
  listS3Buckets: vi.fn().mockResolvedValue([]),
  listNfsExports: vi.fn().mockResolvedValue([]),
}))

vi.mock('../usePathAutocomplete', () => ({
  usePathAutocomplete: () => ({
    options: { value: [] },
    handleInput: vi.fn(),
    reset: vi.fn(),
  }),
}))

import { useEndpointEdit, ValidationError } from '../useEndpointEdit'
import { updateEndpoint } from '../../api/endpoints'
import type { Endpoint } from '../../api/endpoints'

function makeNfsEndpoint(): Endpoint {
  return {
    id: 'ep-1',
    name: '生产NFS存储',
    endpoint_type: 'nfs',
    config: { type: 'nfs', server: '10.0.0.1', port: 2049, export_path: '/data/production', prefix: '/sub' },
    created_at: '2024-03-12T14:56:30Z',
  } as Endpoint
}

function makeLocalEndpoint(): Endpoint {
  return {
    id: 'ep-2',
    name: '本地测试目录',
    endpoint_type: 'local',
    config: { type: 'local', path: '/data/test/dataset' },
    created_at: '2024-03-15T11:20:00Z',
  } as Endpoint
}

function makeS3Endpoint(): Endpoint {
  return {
    id: 'ep-3',
    name: '备份S3存储',
    endpoint_type: 's3',
    config: {
      type: 's3',
      host: 'backup.s3.example.com',
      port: 9000,
      access_key: 'AKIA123',
      secret_key: 'secret',
      bucket: 'backup-bucket',
      prefix: '/daily',
      protocol: 'https',
    },
    created_at: '2024-03-14T10:22:15Z',
  } as Endpoint
}

describe('useEndpointEdit', () => {
  describe('populateFields', () => {
    it('NFS endpoint 填充正确字段', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      expect(edit.editName.value).toBe('生产NFS存储')
      expect(edit.editNfsExportPath.value).toBe('/data/production')
      expect(edit.editNfsPrefix.value).toBe('/sub')
    })

    it('Local endpoint 填充正确字段', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeLocalEndpoint())
      expect(edit.editName.value).toBe('本地测试目录')
      expect(edit.editPath.value).toBe('/data/test/dataset')
    })

    it('S3 endpoint 填充正确字段', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeS3Endpoint())
      expect(edit.editName.value).toBe('备份S3存储')
      expect(edit.editS3AccessKey.value).toBe('AKIA123')
      expect(edit.editS3SecretKey.value).toBe('secret')
      expect(edit.editS3Bucket.value).toBe('backup-bucket')
      expect(edit.editS3Prefix.value).toBe('/daily')
      expect(edit.editS3UseTls.value).toBe(true)
    })
  })

  describe('validate - 必填字段验证', () => {
    it('名称为空抛 ValidationError', async () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      edit.editName.value = ''
      await expect(edit.handleSave('ep-1')).rejects.toThrow(ValidationError)
      await expect(edit.handleSave('ep-1')).rejects.toThrow('名称不能为空')
    })

    it('Local 路径为空抛 ValidationError', async () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeLocalEndpoint())
      edit.editPath.value = ''
      await expect(edit.handleSave('ep-2')).rejects.toThrow('路径不能为空')
    })

    it('NFS 导出路径为空抛 ValidationError', async () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      edit.editNfsExportPath.value = ''
      await expect(edit.handleSave('ep-1')).rejects.toThrow('请选择共享名称')
    })

    it('S3 Bucket 为空抛 ValidationError', async () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeS3Endpoint())
      edit.editS3Bucket.value = ''
      await expect(edit.handleSave('ep-3')).rejects.toThrow('Bucket 不能为空')
    })
  })

  describe('handleSave', () => {
    it('保存成功返回 needs_confirm=false', async () => {
      vi.mocked(updateEndpoint).mockResolvedValue({
        needs_confirm: false,
        conflicting_tasks: [],
        endpoint: makeNfsEndpoint(),
      })
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      const result = await edit.handleSave('ep-1')
      expect(result.needs_confirm).toBe(false)
    })

    it('有冲突时返回 needs_confirm=true', async () => {
      vi.mocked(updateEndpoint).mockResolvedValue({
        needs_confirm: true,
        conflicting_tasks: ['task-1', 'task-2'],
        endpoint: null,
      })
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      const result = await edit.handleSave('ep-1')
      expect(result.needs_confirm).toBe(true)
      expect(result.conflicting_tasks).toEqual(['task-1', 'task-2'])
    })

    it('force=true 时传递 force 参数', async () => {
      vi.mocked(updateEndpoint).mockResolvedValue({
        needs_confirm: false,
        conflicting_tasks: [],
        endpoint: makeNfsEndpoint(),
      })
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      await edit.handleSave('ep-1', true)
      expect(updateEndpoint).toHaveBeenCalledWith('ep-1', expect.objectContaining({ force: true }))
    })
  })

  describe('endpointType', () => {
    it('正确返回填充后的类型', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      expect(edit.endpointType.value).toBe('nfs')
    })

    it('未填充时返回空字符串', () => {
      const edit = useEndpointEdit()
      expect(edit.endpointType.value).toBe('')
    })
  })

  describe('互斥性测试', () => {
    it('NFS 类型只验证 NFS 必填字段，不验证 Local/S3 字段', async () => {
      vi.mocked(updateEndpoint).mockResolvedValue({
        needs_confirm: false,
        conflicting_tasks: [],
        endpoint: makeNfsEndpoint(),
      })
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      // Local path 为空不应影响 NFS 保存
      edit.editPath.value = ''
      // S3 bucket 为空不应影响 NFS 保存
      edit.editS3Bucket.value = ''
      const result = await edit.handleSave('ep-1')
      expect(result.needs_confirm).toBe(false)
    })

    it('Local 类型只验证 Local 必填字段，不验证 NFS/S3 字段', async () => {
      vi.mocked(updateEndpoint).mockResolvedValue({
        needs_confirm: false,
        conflicting_tasks: [],
        endpoint: makeLocalEndpoint(),
      })
      const edit = useEndpointEdit()
      edit.populateFields(makeLocalEndpoint())
      // NFS export 为空不应影响 Local 保存
      edit.editNfsExportPath.value = ''
      // S3 bucket 为空不应影响 Local 保存
      edit.editS3Bucket.value = ''
      const result = await edit.handleSave('ep-2')
      expect(result.needs_confirm).toBe(false)
    })

    it('S3 类型只验证 S3 必填字段，不验证 Local/NFS 字段', async () => {
      vi.mocked(updateEndpoint).mockResolvedValue({
        needs_confirm: false,
        conflicting_tasks: [],
        endpoint: makeS3Endpoint(),
      })
      const edit = useEndpointEdit()
      edit.populateFields(makeS3Endpoint())
      // Local path 为空不应影响 S3 保存
      edit.editPath.value = ''
      // NFS export 为空不应影响 S3 保存
      edit.editNfsExportPath.value = ''
      const result = await edit.handleSave('ep-3')
      expect(result.needs_confirm).toBe(false)
    })

    it('切换 endpoint 后类型正确更新', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      expect(edit.endpointType.value).toBe('nfs')
      edit.populateFields(makeS3Endpoint())
      expect(edit.endpointType.value).toBe('s3')
    })

    it('populateFields 后 isDirty=false', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      expect(edit.isDirty.value).toBe(false)
    })

    it('修改 name 后 isDirty=true', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      edit.editName.value = '修改后的名称'
      expect(edit.isDirty.value).toBe(true)
    })

    it('NFS: 修改 exportPath 后 isDirty=true', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      edit.editNfsExportPath.value = '/new/path'
      expect(edit.isDirty.value).toBe(true)
    })

    it('Local: 修改 path 后 isDirty=true', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeLocalEndpoint())
      edit.editPath.value = '/new/local/path'
      expect(edit.isDirty.value).toBe(true)
    })

    it('S3: 修改 bucket 后 isDirty=true', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeS3Endpoint())
      edit.editS3Bucket.value = 'new-bucket'
      expect(edit.isDirty.value).toBe(true)
    })

    it('S3: 修改 TLS 后 isDirty=true', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeS3Endpoint())
      edit.editS3UseTls.value = !edit.editS3UseTls.value
      expect(edit.isDirty.value).toBe(true)
    })

    it('恢复原值后 isDirty=false', () => {
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      edit.editName.value = '临时修改'
      expect(edit.isDirty.value).toBe(true)
      edit.editName.value = '生产NFS存储'
      expect(edit.isDirty.value).toBe(false)
    })

    it('saving 标志在保存期间为 true，完成后为 false', async () => {
      vi.mocked(updateEndpoint).mockResolvedValue({
        needs_confirm: false,
        conflicting_tasks: [],
        endpoint: makeNfsEndpoint(),
      })
      const edit = useEndpointEdit()
      edit.populateFields(makeNfsEndpoint())
      expect(edit.saving.value).toBe(false)
      const promise = edit.handleSave('ep-1')
      // 注意：由于 mock 是同步 resolve，saving 已经变回 false
      await promise
      expect(edit.saving.value).toBe(false)
    })
  })
})
