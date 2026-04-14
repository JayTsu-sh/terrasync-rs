import { throwIfError } from './fetch-utils'

const BASE = '/api/v1'

export interface FileTypeEntry {
  ext: string
  count: number
  total_size: number
}

export interface SizeEntry {
  range: string
  count: number
  total_size: number
}

export interface TimeEntry {
  bucket: string
  count: number
}

export interface AnalyticsData {
  file_type_distribution: FileTypeEntry[]
  size_distribution: SizeEntry[]
  time_distribution: TimeEntry[]
  db_error?: string
}

export async function getTaskAnalytics(taskId: string): Promise<AnalyticsData> {
  const res = await fetch(`${BASE}/tasks/${taskId}/analytics`)
  await throwIfError(res)
  return res.json()
}
