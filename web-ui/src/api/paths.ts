import { throwIfError } from './fetch-utils'

const BASE = '/api/v1'

export interface MigrationPath {
  id: string
  endpoint_id: string
  sub_path: string
  description?: string
  created_at: string
}

export async function listPaths(endpointId: string): Promise<MigrationPath[]> {
  const res = await fetch(`${BASE}/endpoints/${endpointId}/paths`)
  await throwIfError(res)
  return res.json()
}

export async function createPath(
  endpointId: string,
  data: { sub_path: string; description?: string },
): Promise<MigrationPath> {
  const res = await fetch(`${BASE}/endpoints/${endpointId}/paths`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  await throwIfError(res)
  return res.json()
}

export async function deletePath(id: string): Promise<void> {
  const res = await fetch(`${BASE}/paths/${id}`, { method: 'DELETE' })
  await throwIfError(res)
}
