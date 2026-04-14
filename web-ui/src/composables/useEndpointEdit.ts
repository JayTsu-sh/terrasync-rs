import { ref, computed } from 'vue'
import type { Endpoint } from '../api/endpoints'
import { updateEndpoint } from '../api/endpoints'
import { usePathAutocomplete } from './usePathAutocomplete'

export interface SaveResult {
  needs_confirm: boolean
  conflicting_tasks: string[]
  endpoint?: Endpoint
}

export class ValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ValidationError'
  }
}

export function useEndpointEdit() {
  const saving = ref(false)
  const endpointType = ref('')

  // Edit fields
  const editName = ref('')
  const editPath = ref('')
  const editNfsExportPath = ref('')
  const editNfsPrefix = ref('')
  const editS3AccessKey = ref('')
  const editS3SecretKey = ref('')
  const editS3Bucket = ref('')
  const editS3Prefix = ref('')
  const editS3UseTls = ref(false)

  // Store server info for S3/NFS autocomplete
  const serverInfo = ref<{ server?: string; port?: number; host?: string }>({})

  const isLocal = computed(() => endpointType.value === 'local')
  const isNfs = computed(() => endpointType.value === 'nfs')
  const isS3 = computed(() => endpointType.value === 's3')

  // Path autocomplete
  const { options: localPathOptions, handleInput: handleEditPathInput } = usePathAutocomplete(editPath)

  const { options: nfsPrefixOptions, handleInput: handleNfsPrefixInput } = usePathAutocomplete(editNfsPrefix, () => {
    if (!isNfs.value || !serverInfo.value.server || !editNfsExportPath.value) return undefined
    return `nfs://${serverInfo.value.server}:${serverInfo.value.port}/${editNfsExportPath.value.replace(/^\//, '')}`
  })

  const { options: s3PrefixOptions, handleInput: handleS3PrefixInput } = usePathAutocomplete(editS3Prefix, () => {
    if (!isS3.value || !serverInfo.value.host) return undefined
    const ak = editS3AccessKey.value.trim()
    const sk = editS3SecretKey.value.trim()
    const bucket = editS3Bucket.value.trim()
    if (!ak || !sk || !bucket) return undefined
    const scheme = editS3UseTls.value ? 's3+https' : 's3'
    return `${scheme}://${ak}:${sk}@${bucket}.${serverInfo.value.host}:${serverInfo.value.port}`
  })

  const originalValues = ref({
    name: '',
    path: '',
    nfsExportPath: '',
    nfsPrefix: '',
    s3AccessKey: '',
    s3SecretKey: '',
    s3Bucket: '',
    s3Prefix: '',
    s3UseTls: false,
  })

  const isDirty = computed(() => {
    const o = originalValues.value
    if (editName.value !== o.name) return true
    if (isLocal.value) return editPath.value !== o.path
    if (isNfs.value) return editNfsExportPath.value !== o.nfsExportPath || editNfsPrefix.value !== o.nfsPrefix
    if (isS3.value) {
      return (
        editS3AccessKey.value !== o.s3AccessKey ||
        editS3SecretKey.value !== o.s3SecretKey ||
        editS3Bucket.value !== o.s3Bucket ||
        editS3Prefix.value !== o.s3Prefix ||
        editS3UseTls.value !== o.s3UseTls
      )
    }
    return false
  })

  function snapshotOriginalValues() {
    originalValues.value = {
      name: editName.value,
      path: editPath.value,
      nfsExportPath: editNfsExportPath.value,
      nfsPrefix: editNfsPrefix.value,
      s3AccessKey: editS3AccessKey.value,
      s3SecretKey: editS3SecretKey.value,
      s3Bucket: editS3Bucket.value,
      s3Prefix: editS3Prefix.value,
      s3UseTls: editS3UseTls.value,
    }
  }

  function populateFields(ep: Endpoint) {
    editName.value = ep.name
    endpointType.value = ep.config.type

    if (ep.config.type === 'local') {
      editPath.value = ep.config.path || ''
    } else if (ep.config.type === 'nfs') {
      editNfsExportPath.value = ep.config.export_path || ''
      editNfsPrefix.value = ep.config.prefix || ''
      serverInfo.value = { server: ep.config.server, port: ep.config.port }
    } else if (ep.config.type === 's3') {
      editS3AccessKey.value = ep.config.access_key || ''
      editS3SecretKey.value = ep.config.secret_key || ''
      editS3Bucket.value = ep.config.bucket || ''
      editS3Prefix.value = ep.config.prefix || ''
      editS3UseTls.value = ep.config.protocol === 'https'
      serverInfo.value = { host: ep.config.host, port: ep.config.port }
    }
    snapshotOriginalValues()
  }

  async function handleSave(endpointId: string, force = false): Promise<SaveResult> {
    const name = editName.value.trim()
    if (!name) throw new ValidationError('名称不能为空')

    let config: any
    if (isLocal.value) {
      if (!editPath.value.trim()) throw new ValidationError('路径不能为空')
      config = { type: 'local', path: editPath.value.trim() }
    } else if (isNfs.value) {
      if (!editNfsExportPath.value) throw new ValidationError('请选择共享名称')
      config = {
        type: 'nfs',
        server: serverInfo.value.server,
        port: serverInfo.value.port,
        export_path: editNfsExportPath.value,
        prefix: editNfsPrefix.value || undefined,
      }
    } else if (isS3.value) {
      if (!editS3Bucket.value.trim()) throw new ValidationError('Bucket 不能为空')
      config = {
        type: 's3',
        host: serverInfo.value.host,
        port: serverInfo.value.port,
        access_key: editS3AccessKey.value.trim() || undefined,
        secret_key: editS3SecretKey.value.trim() || undefined,
        bucket: editS3Bucket.value.trim(),
        prefix: editS3Prefix.value.trim() || undefined,
        protocol: editS3UseTls.value ? 'https' : 'http',
      }
    } else {
      throw new Error('未知的端点类型')
    }

    saving.value = true
    try {
      const resp = await updateEndpoint(endpointId, { name, config, force })
      if (resp.needs_confirm) {
        return { needs_confirm: true, conflicting_tasks: resp.conflicting_tasks }
      }
      return { needs_confirm: false, conflicting_tasks: [], endpoint: resp.endpoint ?? undefined }
    } finally {
      saving.value = false
    }
  }

  return {
    saving,
    endpointType,
    isDirty,
    editName,
    editPath,
    editNfsExportPath,
    editNfsPrefix,
    editS3AccessKey,
    editS3SecretKey,
    editS3Bucket,
    editS3Prefix,
    editS3UseTls,
    isLocal,
    isNfs,
    isS3,
    localPathOptions,
    handleEditPathInput,
    nfsPrefixOptions,
    handleNfsPrefixInput,
    s3PrefixOptions,
    handleS3PrefixInput,
    serverInfo,
    populateFields,
    handleSave,
  }
}
