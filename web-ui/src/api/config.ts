import { throwIfError } from './fetch-utils'

const BASE = '/api/v1'

export interface ConfigItem {
  key: string
  value: unknown
}

export interface ConfigView {
  items: ConfigItem[]
}

export async function getConfig(): Promise<ConfigView> {
  const res = await fetch(`${BASE}/config`)
  await throwIfError(res)
  return res.json()
}

export async function updateConfig(updates: { key: string; value: unknown }[]): Promise<void> {
  const res = await fetch(`${BASE}/config`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ updates }),
  })
  await throwIfError(res)
}

export async function testClickhouse(params: {
  dsn: string
  username: string
  password: string
  database: string
}): Promise<{ success: boolean; message: string }> {
  const res = await fetch(`${BASE}/config/test-clickhouse`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  })
  await throwIfError(res)
  return res.json()
}
