import { throwIfError } from './fetch-utils'

const BASE = '/api/v1'

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

export async function listEndpoints(): Promise<Endpoint[]> {
  const res = await fetch(`${BASE}/endpoints`)
  await throwIfError(res)
  return res.json()
}

export async function getEndpoint(id: string): Promise<Endpoint> {
  const res = await fetch(`${BASE}/endpoints/${id}`)
  await throwIfError(res)
  return res.json()
}

export async function createEndpoint(data: {
  name: string
  endpoint_type: string
  config: EndpointConfig
}): Promise<Endpoint> {
  const res = await fetch(`${BASE}/endpoints`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  await throwIfError(res)
  return res.json()
}

export interface UpdateEndpointResponse {
  endpoint: Endpoint | null
  needs_confirm: boolean
  conflicting_tasks: string[]
}

export async function updateEndpoint(
  id: string,
  data: { name?: string; config?: EndpointConfig; force?: boolean },
): Promise<UpdateEndpointResponse> {
  const res = await fetch(`${BASE}/endpoints/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  await throwIfError(res)
  return res.json()
}

export async function deleteEndpoint(id: string): Promise<void> {
  const res = await fetch(`${BASE}/endpoints/${id}`, { method: 'DELETE' })
  await throwIfError(res)
}

export async function testConnection(id: string): Promise<string> {
  const res = await fetch(`${BASE}/endpoints/${id}/test`, { method: 'POST' })
  await throwIfError(res)
  return res.json()
}

export interface NfsExport {
  path: string
  groups: string[]
}

export interface DirEntryItem {
  name: string
  full_path: string
}

export interface ListDirsResponse {
  parent: string
  entries: DirEntryItem[]
  /** 服务端平台路径分隔符（`/` 或 `\`） */
  sep: string
}

export async function listDirs(path: string, storageUrl?: string): Promise<ListDirsResponse> {
  const body: Record<string, unknown> = { path }
  if (storageUrl) body.storage_url = storageUrl
  const res = await fetch(`${BASE}/fs/list-dirs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  await throwIfError(res)
  return res.json()
}

export interface S3BucketInfo {
  name: string
  creation_date: string | null
}

export async function listS3Buckets(
  server: string,
  access_key: string,
  secret_key: string,
  use_https?: boolean,
): Promise<S3BucketInfo[]> {
  const res = await fetch(`${BASE}/s3/buckets`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ server, access_key, secret_key, use_https }),
  })
  await throwIfError(res)
  return res.json()
}

export async function listNfsExports(server: string, port?: number): Promise<NfsExport[]> {
  const res = await fetch(`${BASE}/nfs/exports`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ server, port }),
  })
  await throwIfError(res)
  return res.json()
}
