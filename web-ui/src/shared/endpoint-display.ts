import { CloudOutline, FolderOutline, ServerOutline } from '@vicons/ionicons5'
import type { Endpoint } from '../api/endpoints'

export interface EndpointTypeInfo {
  label: string
  color: string
  bg: string
  border: string
  icon: typeof CloudOutline
}

const TYPE_MAP: Record<string, EndpointTypeInfo> = {
  local: { label: '本地', color: '#faad14', bg: '#fffbe6', border: '#ffe58f', icon: FolderOutline },
  nfs: { label: 'NFSv3', color: '#1677ff', bg: '#e6f4ff', border: '#91caff', icon: ServerOutline },
  nfs_v3: { label: 'NFS v3', color: '#1677ff', bg: '#e6f4ff', border: '#91caff', icon: ServerOutline },
  s3: { label: 'S3', color: '#722ed1', bg: '#f9f0ff', border: '#d3adf7', icon: CloudOutline },
}

const DEFAULT_TYPE_INFO: EndpointTypeInfo = {
  label: '',
  color: '#666',
  bg: '#f5f5f5',
  border: '#d9d9d9',
  icon: ServerOutline,
}

export function getEndpointTypeInfo(type: string): EndpointTypeInfo {
  const info = TYPE_MAP[type]
  if (info) return info
  return { ...DEFAULT_TYPE_INFO, label: type.toUpperCase() }
}

export function getEndpointAddress(ep: Endpoint, opts?: { includePrefix?: boolean }): string {
  const c = ep.config
  const suffix = (prefix?: string) => {
    if (!opts?.includePrefix || !prefix) return ''
    return prefix.startsWith('/') ? prefix : `/${prefix}`
  }
  if (c.type === 'local') return c.path || ''
  if (c.type === 'nfs') return `${c.server}:${c.port}${c.export_path || ''}${suffix(c.prefix)}`
  if (c.type === 's3') return `${c.bucket}.${c.host}:${c.port}${suffix(c.prefix)}`
  return ''
}
