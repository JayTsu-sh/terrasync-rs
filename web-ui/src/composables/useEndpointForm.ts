import { ref, watch, onUnmounted } from 'vue'
import type { EndpointConfig, NfsExport, S3BucketInfo } from '../api/endpoints'
import { createEndpoint, listNfsExports, listS3Buckets } from '../api/endpoints'
import { createPath } from '../api/paths'
import { createTask, startTask } from '../api/tasks'
import { usePathAutocomplete } from './usePathAutocomplete'

export interface CreateResult {
  endpointCreated: boolean
  scanStarted: boolean
  scanError?: string
}

export function useEndpointForm() {
  const defaultFormData = () => ({
    name: '',
    type: 'local' as 'local' | 'nfs' | 's3',
    path: '',
    server: '',
    port: 2049,
    export_path: '',
    prefix: '',
    access_key: '',
    secret_key: '',
    bucket: '',
    host: '',
    s3_port: 9000,
    use_tls: false,
    start_scan: false,
  })

  const formData = ref(defaultFormData())
  const nfsExports = ref<NfsExport[]>([])
  const loadingExports = ref(false)
  const exportError = ref('')
  let debounceTimer: ReturnType<typeof setTimeout> | null = null

  // Local path autocomplete
  const localPath = ref('')
  const {
    options: localPathOptions,
    handleInput: handlePathInput,
    reset: resetLocalPath,
  } = usePathAutocomplete(localPath)

  watch(localPath, (val) => {
    formData.value.path = val
  })
  watch(
    () => formData.value.path,
    (val) => {
      if (val !== localPath.value) localPath.value = val
    },
  )

  // NFS prefix autocomplete
  const nfsPrefixRef = ref('')
  const {
    options: nfsPrefixOptions,
    handleInput: handlePrefixInput,
    reset: resetNfsPrefix,
  } = usePathAutocomplete(nfsPrefixRef, () => {
    if (!formData.value.server.trim() || !formData.value.export_path) return undefined
    return `nfs://${formData.value.server}:${formData.value.port}/${formData.value.export_path.replace(/^\//, '')}`
  })

  watch(nfsPrefixRef, (val) => {
    formData.value.prefix = val
  })
  watch(
    () => formData.value.prefix,
    (val) => {
      if (val !== nfsPrefixRef.value) nfsPrefixRef.value = val
    },
  )

  // S3 bucket discovery
  const s3Buckets = ref<S3BucketInfo[]>([])
  const loadingBuckets = ref(false)
  const bucketError = ref('')
  let s3DebounceTimer: ReturnType<typeof setTimeout> | null = null

  // S3 prefix autocomplete
  const s3PrefixRef = ref('')
  const {
    options: s3PrefixOptions,
    handleInput: handleS3PrefixInput,
    reset: resetS3Prefix,
  } = usePathAutocomplete(s3PrefixRef, () => {
    const d = formData.value
    if (!d.host.trim() || !d.access_key.trim() || !d.secret_key.trim() || !d.bucket) return undefined
    const scheme = d.use_tls ? 's3+https' : 's3'
    return `${scheme}://${d.access_key}:${d.secret_key}@${d.bucket}.${d.host}:${d.s3_port}`
  })

  watch(s3PrefixRef, (val) => {
    formData.value.prefix = val
  })
  watch(
    () => formData.value.prefix,
    (val) => {
      if (val !== s3PrefixRef.value) s3PrefixRef.value = val
    },
  )

  function s3FieldsReady(): boolean {
    const d = formData.value
    return !!(d.host.trim() && d.s3_port && d.access_key.trim() && d.secret_key.trim())
  }

  function fetchS3BucketsDebounced() {
    if (s3DebounceTimer) clearTimeout(s3DebounceTimer)
    if (!s3FieldsReady()) {
      s3Buckets.value = []
      bucketError.value = ''
      loadingBuckets.value = false
      return
    }
    s3DebounceTimer = setTimeout(async () => {
      loadingBuckets.value = true
      bucketError.value = ''
      try {
        const server = `${formData.value.host.trim()}:${formData.value.s3_port}`
        s3Buckets.value = await listS3Buckets(
          server,
          formData.value.access_key,
          formData.value.secret_key,
          formData.value.use_tls,
        )
        bucketError.value = ''
        if (s3Buckets.value.length > 0 && !formData.value.bucket) {
          formData.value.bucket = s3Buckets.value[0].name
        }
      } catch {
        s3Buckets.value = []
        bucketError.value = ''
      } finally {
        loadingBuckets.value = false
      }
    }, 800)
  }

  watch(
    () => [
      formData.value.host,
      formData.value.s3_port,
      formData.value.access_key,
      formData.value.secret_key,
      formData.value.use_tls,
    ],
    () => {
      if (formData.value.type === 's3') {
        formData.value.bucket = ''
        fetchS3BucketsDebounced()
      }
    },
  )

  // NFS export discovery
  function fetchNfsExports(server: string, port: number) {
    if (debounceTimer) clearTimeout(debounceTimer)
    if (!server.trim()) {
      nfsExports.value = []
      exportError.value = ''
      return
    }
    loadingExports.value = true
    exportError.value = ''
    debounceTimer = setTimeout(async () => {
      try {
        nfsExports.value = await listNfsExports(server.trim(), port)
        exportError.value = ''
        if (nfsExports.value.length > 0 && !formData.value.export_path) {
          formData.value.export_path = nfsExports.value[0].path
        }
      } catch (e: any) {
        nfsExports.value = []
        exportError.value = e.message || '查询失败'
      } finally {
        loadingExports.value = false
      }
    }, 500)
  }

  watch(
    () => formData.value.server,
    (server) => {
      if (formData.value.type === 'nfs') {
        formData.value.export_path = ''
        fetchNfsExports(server, formData.value.port)
      }
    },
  )

  watch(
    () => formData.value.port,
    (port) => {
      if (formData.value.type === 'nfs' && formData.value.server.trim()) {
        formData.value.export_path = ''
        fetchNfsExports(formData.value.server, port)
      }
    },
  )

  watch(
    () => formData.value.type,
    () => {
      nfsExports.value = []
      exportError.value = ''
      s3Buckets.value = []
      bucketError.value = ''
      if (formData.value.type === 'nfs' && formData.value.server.trim()) {
        fetchNfsExports(formData.value.server, formData.value.port)
      }
      if (
        formData.value.type === 's3' &&
        formData.value.host.trim() &&
        formData.value.access_key.trim() &&
        formData.value.secret_key.trim()
      ) {
        fetchS3BucketsDebounced()
      }
    },
  )

  // Config builder
  function buildConfig(): EndpointConfig {
    const d = formData.value
    if (d.type === 'local') return { type: 'local', path: d.path }
    if (d.type === 'nfs')
      return {
        type: 'nfs',
        server: d.server,
        port: d.port,
        export_path: d.export_path,
        prefix: d.prefix || undefined,
      }
    return {
      type: 's3',
      access_key: d.access_key,
      secret_key: d.secret_key,
      bucket: d.bucket,
      host: d.host,
      port: d.s3_port,
      prefix: d.prefix || undefined,
      protocol: d.use_tls ? 'https' : 'http',
    }
  }

  function toEndpointType(configType: string): string {
    return configType === 'nfs' ? 'nfs_v3' : configType
  }

  // Create handler — throws on endpoint creation failure, returns result for partial success
  async function handleCreate(): Promise<CreateResult> {
    const ep = await createEndpoint({
      name: formData.value.name,
      endpoint_type: toEndpointType(formData.value.type),
      config: buildConfig(),
    })

    const result: CreateResult = { endpointCreated: true, scanStarted: false }

    if (formData.value.start_scan) {
      try {
        const path = await createPath(ep.id, { sub_path: '/' })
        const task = await createTask({
          name: `扫描-${ep.name}`,
          task_type: 'scan',
          source_endpoint_id: ep.id,
          source_path_id: path.id,
        })
        await startTask(task.id)
        result.scanStarted = true
      } catch (e: any) {
        result.scanError = e.message
      }
    }

    formData.value = defaultFormData()
    return result
  }

  function handleModalClose() {
    if (debounceTimer) {
      clearTimeout(debounceTimer)
      debounceTimer = null
    }
    if (s3DebounceTimer) {
      clearTimeout(s3DebounceTimer)
      s3DebounceTimer = null
    }
    resetLocalPath()
    resetNfsPrefix()
    resetS3Prefix()
    formData.value = defaultFormData()
    nfsExports.value = []
    exportError.value = ''
    s3Buckets.value = []
    bucketError.value = ''
  }

  const typeOptions = [
    { label: '本地文件系统', value: 'local' },
    { label: 'NFS v3', value: 'nfs' },
    { label: 'S3', value: 's3' },
  ]

  onUnmounted(() => {
    if (debounceTimer) clearTimeout(debounceTimer)
    if (s3DebounceTimer) clearTimeout(s3DebounceTimer)
  })

  return {
    formData,
    nfsExports,
    loadingExports,
    exportError,
    s3Buckets,
    loadingBuckets,
    bucketError,
    localPathOptions,
    handlePathInput,
    nfsPrefixOptions,
    handlePrefixInput,
    s3PrefixOptions,
    handleS3PrefixInput,
    typeOptions,
    handleCreate,
    handleModalClose,
  }
}
