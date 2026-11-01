import { color } from 'echarts/core'
import type { AnalysisNode } from '../api/storage-analysis'
import { formatBytes, formatNumber } from './formatters'

export function getTreemapTone(node: AnalysisNode, palette: string[]) {
  const hash = Array.from(`${node.kind}:${node.path}`).reduce(
    (value, char) => (value * 31 + char.charCodeAt(0)) >>> 0,
    0,
  )
  const accent = palette[hash % palette.length] ?? 'currentColor'
  return {
    accent,
    fill: color.lift(accent, 0.91),
    border: color.lift(accent, 0.65),
    text: color.modifyHSL(accent, undefined, 0.45, 0.25),
  }
}

export function formatTreemapLabel(node: AnalysisNode | undefined, metric: 'size' | 'count', total: number) {
  if (!node) return ''
  const value = metric === 'count' ? node.file_count : node.bytes
  const share = total > 0 ? `${((value / total) * 100).toFixed(1)}%` : '0.0%'
  const amount = metric === 'count' ? `${formatNumber(node.file_count)} 个文件` : formatBytes(node.bytes)
  return `{name|${node.name}}\n{amount|${amount}}\n{detail|${share} · ${formatNumber(node.file_count)} 个文件}`
}

/** 年龄固定在扫描时刻，缺失或无效的时间独立显示。 */
export function ageBucket(modifiedAt: string | null, scannedAt: string | null) {
  if (!modifiedAt) return 5
  const modified = Date.parse(modifiedAt)
  const reference = scannedAt ? Date.parse(scannedAt) : Date.now()
  if (!Number.isFinite(modified) || !Number.isFinite(reference)) return 5
  const days = Math.max(0, (reference - modified) / 86_400_000)
  return days < 7 ? 0 : days < 30 ? 1 : days < 180 ? 2 : days < 365 ? 3 : 4
}
