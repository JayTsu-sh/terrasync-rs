// ========== Endpoint 类型 ==========

export interface EndpointConfig {
  type: 'local' | 'nfs' | 's3'
  path?: string
  server?: string
  port?: number
  export_path?: string
  prefix?: string
  access_key?: string
  secret_key?: string
  bucket?: string
  host?: string
  protocol?: string
}

export interface Endpoint {
  id: string
  name: string
  endpoint_type: string
  endpoint_type_display: string
  config: EndpointConfig
  created_at: string
  updated_at: string
}

// ========== Task 类型 ==========

export interface TaskConfig {
  match_expr?: string
  exclude_expr?: string
  depth?: number
  enable_integrity_check?: boolean
  enable_acl?: boolean
  qos?: string
  block_size?: string
  iops?: number
  concurrency?: number
}

export interface MigrationTask {
  id: string
  name: string
  task_type: string
  source_endpoint_id: string
  dest_endpoint_id?: string
  config: TaskConfig
  status: string
  created_at: string
  started_at?: string
  finished_at?: string
  error_message?: string
}

export interface TaskExecution {
  id: string
  task_id: string
  execution_type: string
  status: string
  started_at: string
  finished_at?: string
  stats?: {
    scanned_files: number
    scanned_dirs: number
    total_bytes: number
    transferred_bytes: number
    error_count: number
    duration_secs: number
    avg_speed_bytes_per_sec: number
  }
  error_message?: string
}

export interface MigrationPath {
  id: string
  endpoint_id: string
  sub_path: string
  description?: string
  created_at: string
}

// ========== Mock 数据 ==========

export const mockEndpoints: Endpoint[] = [
  {
    id: 'ep-local-1',
    name: 'Local-Data',
    endpoint_type: 'local',
    endpoint_type_display: '本地文件系统',
    config: { type: 'local', path: '/data/storage' },
    created_at: '2026-01-10T08:00:00Z',
    updated_at: '2026-01-10T08:00:00Z',
  },
  {
    id: 'ep-nfs-1',
    name: 'NAS-01',
    endpoint_type: 'nfs_v3',
    endpoint_type_display: 'NFS v3',
    config: { type: 'nfs', server: '10.0.0.1', port: 2049, export_path: '/data', prefix: '/project' },
    created_at: '2026-01-15T10:00:00Z',
    updated_at: '2026-01-15T10:00:00Z',
  },
  {
    id: 'ep-s3-1',
    name: 'S3-Backup',
    endpoint_type: 's3',
    endpoint_type_display: 'S3',
    config: { type: 's3', host: 's3.example.com', port: 9000, access_key: 'AKID123', secret_key: 'secret', bucket: 'backup', protocol: 'http' },
    created_at: '2026-02-01T12:00:00Z',
    updated_at: '2026-02-01T12:00:00Z',
  },
]

export const mockPaths: MigrationPath[] = [
  { id: 'path-1', endpoint_id: 'ep-local-1', sub_path: '/', created_at: '2026-01-10T08:00:00Z' },
  { id: 'path-2', endpoint_id: 'ep-nfs-1', sub_path: '/', created_at: '2026-01-15T10:00:00Z' },
  { id: 'path-3', endpoint_id: 'ep-s3-1', sub_path: '/', created_at: '2026-02-01T12:00:00Z' },
]

export function makeTask(overrides: Partial<MigrationTask> & { id: string; name: string; status: string }): MigrationTask {
  return {
    task_type: 'sync',
    source_endpoint_id: 'ep-local-1',
    config: { depth: 0 },
    created_at: '2026-03-01T00:00:00Z',
    ...overrides,
  }
}

export const mockTasksByStatus = {
  pending: [makeTask({ id: 't-pending', name: 'scan-pending', status: 'pending' })],
  running: [makeTask({ id: 't-running', name: 'scan-running', status: 'running', started_at: '2026-03-01T00:01:00Z' })],
  completed: [makeTask({ id: 't-done', name: 'scan-done', status: 'completed', started_at: '2026-03-01T00:01:00Z', finished_at: '2026-03-01T00:10:00Z' })],
  failed: [makeTask({ id: 't-fail', name: 'scan-fail', status: 'failed', error_message: 'connection refused' })],
  cancelled: [makeTask({ id: 't-cancel', name: 'scan-cancel', status: 'cancelled' })],
}

export const mockAllTasks: MigrationTask[] = [
  ...mockTasksByStatus.pending,
  ...mockTasksByStatus.running,
  ...mockTasksByStatus.completed,
  ...mockTasksByStatus.failed,
  ...mockTasksByStatus.cancelled,
]

export const mockExecutions: TaskExecution[] = [
  {
    id: 'exec-1',
    task_id: 't-done',
    execution_type: 'full',
    status: 'completed',
    started_at: '2026-03-01T00:01:00Z',
    finished_at: '2026-03-01T00:10:00Z',
    stats: {
      scanned_files: 15000,
      scanned_dirs: 200,
      total_bytes: 1073741824,
      transferred_bytes: 0,
      error_count: 0,
      duration_secs: 540,
      avg_speed_bytes_per_sec: 1988410,
    },
  },
]

// ========== Filter Fields ==========

export interface FilterField {
  name: string
  label: string
  value_type: string
  operators: { value: string; label: string }[]
  enum_values?: string[]
}

export const mockFilterFields: FilterField[] = [
  { name: 'name', label: '文件名', value_type: 'string', operators: [{ value: '==', label: '等于' }, { value: '!=', label: '不等于' }, { value: '=~', label: '匹配' }] },
  { name: 'path', label: '路径', value_type: 'string', operators: [{ value: '==', label: '等于' }, { value: '=~', label: '匹配' }] },
  { name: 'size', label: '文件大小', value_type: 'number', operators: [{ value: '>', label: '大于' }, { value: '<', label: '小于' }, { value: '>=', label: '大于等于' }, { value: '<=', label: '小于等于' }] },
  { name: 'type', label: '类型', value_type: 'enum', operators: [{ value: '==', label: '等于' }], enum_values: ['file', 'dir'] },
  { name: 'modified', label: '修改时间', value_type: 'time', operators: [{ value: '>', label: '晚于' }, { value: '<', label: '早于' }] },
  { name: 'extension', label: '扩展名', value_type: 'string', operators: [{ value: '==', label: '等于' }, { value: '!=', label: '不等于' }] },
]

// ========== Task Logs ==========

export interface TaskLog {
  id: string
  execution_id: string
  level: string
  message: string
  file_path?: string
  created_at: string
}

export const mockTaskLogs: TaskLog[] = [
  { id: 'log-1', execution_id: 'exec-1', level: 'info', message: 'Scan started', created_at: '2026-03-01T00:01:00Z' },
  { id: 'log-2', execution_id: 'exec-1', level: 'warn', message: 'Permission denied: /data/secret', file_path: '/data/secret', created_at: '2026-03-01T00:02:00Z' },
  { id: 'log-3', execution_id: 'exec-1', level: 'error', message: 'Failed to read file: /data/corrupt.dat', file_path: '/data/corrupt.dat', created_at: '2026-03-01T00:03:00Z' },
  { id: 'log-4', execution_id: 'exec-1', level: 'info', message: 'Scan completed successfully', created_at: '2026-03-01T00:10:00Z' },
]

// ========== Config ==========

export const mockConfig = {
  items: [
    { key: 'database.clickhouse.dsn', value: 'http://localhost:8123' },
    { key: 'database.clickhouse.username', value: 'default' },
    { key: 'database.clickhouse.password', value: '' },
    { key: 'database.clickhouse.database', value: 'default' },
    { key: 'database.batch_size', value: 200000 },
    { key: 'log.level', value: 'info' },
  ],
}
