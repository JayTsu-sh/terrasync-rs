import type { TaskStatus } from '../shared/task-rules'
import { throwIfError } from './fetch-utils'

const BASE = '/api/v1'

export interface TaskConfig {
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
}

export interface ExecutionStats {
  scanned_files: number
  scanned_dirs: number
  total_bytes: number
  transferred_bytes: number
  error_count: number
  duration_secs: number
  avg_speed_bytes_per_sec: number
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
