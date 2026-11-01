import { describe, expect, it } from 'vitest'
import { color } from 'echarts/core'
import { getTreemapTone, formatTreemapLabel, ageBucket } from './treemap-presentation'
import type { AnalysisNode } from '../api/storage-analysis'

const node: AnalysisNode = {
  path: 'projects',
  name: 'projects',
  kind: 'directory',
  bytes: 1024 ** 3,
  file_count: 2400,
  directory_count: 12,
  modified_at: null,
}
const palette = ['rgb(59,130,246)', 'rgb(34,197,94)', 'rgb(245,158,11)', 'rgb(239,68,68)']

describe('treemap presentation', () => {
  it('uses a pale fill with a distinct accent and readable dark text', () => {
    const tone = getTreemapTone(node, palette)
    expect(tone.fill).not.toBe(tone.accent)
    expect(tone.text).not.toBe(tone.fill)
    const luminance = (value: string) => {
      const rgb = color.parse(value) ?? []
      const channels = rgb.slice(0, 3).map((channel) => {
        const s = channel / 255
        return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4
      })
      return (channels[0] ?? 0) * 0.2126 + (channels[1] ?? 0) * 0.7152 + (channels[2] ?? 0) * 0.0722
    }
    expect((luminance(tone.fill) + 0.05) / (luminance(tone.text) + 0.05)).toBeGreaterThanOrEqual(4.5)
    expect(getTreemapTone({ ...node }, palette)).toEqual(tone)
  })

  it('labels the real metric, share and file count instead of only a folder name', () => {
    const label = formatTreemapLabel(node, 'size', 2 * node.bytes)
    expect(label).toContain('projects')
    expect(label).toContain('1.00 GB')
    expect(label).toContain('50.0%')
    expect(label).toContain('2,400')
    expect(formatTreemapLabel(node, 'count', 4800)).toContain('50.0%')
    expect(formatTreemapLabel(undefined, 'size', 0)).toBe('')
  })

  it('treats absent or invalid timestamps as unknown and uses the snapshot time', () => {
    expect(ageBucket(null, '2026-10-31T00:00:00Z')).toBe(5)
    expect(ageBucket('invalid', '2026-10-31T00:00:00Z')).toBe(5)
    expect(ageBucket('2026-10-30T00:00:00Z', '2026-10-31T00:00:00Z')).toBe(0)
    expect(ageBucket('2023-01-01T00:00:00Z', '2026-10-31T00:00:00Z')).toBe(4)
  })
})
