import type { TaskStatus } from '../shared/task-rules'
import { throwIfError } from './fetch-utils'

const BASE = '/api/v1'

export interface TaskConfig {
  scan_resume?: boolean
  match_expr?: string
  exclude_expr?: string
  depth?: number
  enable_integrity_check?: boolean
  enable_acl?: boolean
  qos?: string
  peak_qos_rate?: number
  block_size?: string
  iops?: number
  concurrency?: number
  reset_scan_on_start?: boolean
}

export interface StorageFailureDetail {
  code: 'storage_connection_lost' | 'storage_operation_timeout'
  backend: string
  operation: string
  path: string
  retryable: boolean
  operation_timeout_secs: number
}

export interface MigrationTask {
  id: string
  name: string
  task_type: string
  source_endpoint_id: string
  source_path_id?: string
  dest_endpoint_id?: string
  dest_path_id?: string
  config: TaskConfig
  status: TaskStatus
  created_at: string
  started_at?: string
  finished_at?: string
  error_message?: string
  storage_error?: StorageFailureDetail
}

export interface TaskExecution {
  id: string
  task_id: string
  execution_type: string
  status: TaskStatus
  started_at: string
  finished_at?: string
  stats?: ExecutionStats
  error_message?: string
  storage_error?: StorageFailureDetail
}

export interface ExecutionStats {
  scanned_files: number
  scanned_dirs: number
  total_bytes: number
  transferred_bytes: number
  error_count: number
  duration_secs: number
  avg_speed_bytes_per_sec: number
  scan?: ScanSnapshot
  scan_runtime?: ScanRuntimeState
}

export interface EntryKindCounts {
  directories: number
  regular_files: number
  symlinks: number
  block_devices: number
  character_devices: number
  fifos: number
  sockets: number
  unknown: number
}

export interface ScanSnapshot {
  scanned_entries: number
  filtered_entries: number
  retained_entries: number
  restarts: number
  distributions_complete: boolean
  distributions_reconciled: boolean
  counts: {
    scanned: EntryKindCounts
    filtered: EntryKindCounts
    retained: EntryKindCounts
    failed: EntryKindCounts
    vanished: EntryKindCounts
    pruned_directories: number
    truncated_directories: number
    regular_file_bytes: number
    unknown_file_sizes: number
    versions: number
    delete_markers: number
    all_versions_bytes: number
    completed_listings: number
    interrupted_listings: number
    unreadable_listings: number
    vanished_listings: number
  }
  complete: boolean
  capabilities: {
    backend: string
    symlinks: StatisticSupport
    special_files: StatisticSupport
    directory_objects: boolean
    metadata: { family: string; support: StatisticSupport; requested: boolean }[]
  }
}

export type StatisticSupport = 'supported' | 'unsupported' | 'not_applicable' | 'unknown'

export interface ScanRuntimeState {
  state: 'running' | 'recovering' | 'failed'
  network_faults: number
  recovery_attempts: number
  successful_recoveries: number
  current_error: string | null
}

export interface CallbackIdentity {
  round_id: string
  attempt_id: string
  round_started_at_millis: number
  attempt_started_at_millis: number
  sequence: number
}

export interface DiagnosticEvent {
  event_id: string
  round_id: string
  attempt_id: string
  origin: string
  code: string
  message: string
  path: string | null
}

export interface DiagnosticPage {
  events: DiagnosticEvent[]
  omitted_count: number
  next_offset: number | null
}

export interface TaskLog {
  id: string
  execution_id: string
  level: string
  message: string
  file_path?: string
  created_at: string
}

export async function listTasks(params?: { status?: string; task_type?: string }): Promise<MigrationTask[]> {
  const qs = new URLSearchParams()
  if (params?.status) qs.set('status', params.status)
  if (params?.task_type) qs.set('task_type', params.task_type)
  const res = await fetch(`${BASE}/tasks?${qs}`)
  await throwIfError(res)
  return res.json()
}

export async function getTask(id: string): Promise<MigrationTask> {
  const res = await fetch(`${BASE}/tasks/${id}`)
  await throwIfError(res)
  return res.json()
}

export async function createTask(data: {
  name: string
  task_type?: string
  source_endpoint_id: string
  source_path_id?: string
  dest_endpoint_id?: string
  config?: TaskConfig
}): Promise<MigrationTask> {
  const res = await fetch(`${BASE}/tasks`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  await throwIfError(res)
  return res.json()
}

export interface UpdateTaskRequest {
  name?: string
  source_endpoint_id?: string
  source_path_id?: string
  config?: TaskConfig
}

export async function updateTask(id: string, data: UpdateTaskRequest): Promise<MigrationTask> {
  const res = await fetch(`${BASE}/tasks/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  await throwIfError(res)
  return res.json()
}

export async function deleteTask(id: string): Promise<void> {
  const res = await fetch(`${BASE}/tasks/${id}`, { method: 'DELETE' })
  await throwIfError(res)
}

export async function startTask(id: string): Promise<string> {
  const res = await fetch(`${BASE}/tasks/${id}/start`, { method: 'POST' })
  await throwIfError(res)
  return res.json()
}

export async function cancelTask(id: string): Promise<void> {
  const res = await fetch(`${BASE}/tasks/${id}/cancel`, { method: 'POST' })
  await throwIfError(res)
  // POST 仅确认请求已接收；等到真实终态再让调用方刷新，避免读到 running 后空等三秒轮询。
  // 这里只查单个任务状态，不重复加载执行历史、进度或 ClickHouse 分析。
  for (let attempt = 0; attempt < 150; attempt++) {
    const task = await getTask(id)
    if (['cancelled', 'completed', 'failed'].includes(task.status)) return
    await new Promise<void>((resolve) => setTimeout(resolve, 200))
  }
  throw new Error('取消请求已发送，但任务尚未停止，请稍后刷新状态')
}

export async function listExecutions(taskId: string): Promise<TaskExecution[]> {
  const res = await fetch(`${BASE}/tasks/${taskId}/executions`)
  await throwIfError(res)
  return res.json()
}

export async function getExecution(id: string): Promise<TaskExecution> {
  const res = await fetch(`${BASE}/executions/${id}`)
  await throwIfError(res)
  return res.json()
}

export async function deleteExecution(id: string): Promise<void> {
  const res = await fetch(`${BASE}/executions/${id}`, { method: 'DELETE' })
  await throwIfError(res)
}

export interface TaskProgressSnapshot {
  scanned_files: number
  scanned_dirs: number
  total_bytes: number
  error_count: number
  elapsed_secs: number
  speed_bytes_per_sec: number
  scan?: ScanSnapshot
  scan_runtime?: ScanRuntimeState
  callback_identity?: CallbackIdentity
}

export async function getTaskProgress(taskId: string): Promise<TaskProgressSnapshot | null> {
  const res = await fetch(`${BASE}/tasks/${taskId}/progress`)
  await throwIfError(res)
  const data = await res.json()
  return data ?? null
}

export async function getExecutionLogs(
  id: string,
  params?: { level?: string; offset?: number; limit?: number },
): Promise<TaskLog[]> {
  const qs = new URLSearchParams()
  if (params?.level) qs.set('level', params.level)
  if (params?.offset !== undefined) qs.set('offset', String(params.offset))
  if (params?.limit !== undefined) qs.set('limit', String(params.limit))
  const res = await fetch(`${BASE}/executions/${id}/logs?${qs}`)
  await throwIfError(res)
  return res.json()
}
