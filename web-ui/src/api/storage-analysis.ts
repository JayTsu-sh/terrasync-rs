import { throwIfError } from './fetch-utils'

export interface AnalysisNode {
  path: string
  name: string
  kind: 'directory' | 'file' | 'other'
  bytes: number
  file_count: number
  directory_count: number
  modified_at: string | null
  children?: AnalysisNode[]
}

export interface StorageAnalysis {
  task_id: string
  path: string
  source_path: string
  scanned_at: string | null
  is_partial: boolean
  total_bytes: number
  file_count: number
  directory_count: number
  children: AnalysisNode[]
}

export async function getStorageAnalysis(
  taskId: string,
  query: { path: string; metric: 'size' | 'count'; limit?: number },
  signal?: AbortSignal,
): Promise<StorageAnalysis> {
  const params = new URLSearchParams({ path: query.path, metric: query.metric, limit: String(query.limit ?? 120) })
  const response = await fetch(`/api/v1/tasks/${encodeURIComponent(taskId)}/analysis/tree?${params}`, { signal })
  await throwIfError(response)
  return response.json()
}
