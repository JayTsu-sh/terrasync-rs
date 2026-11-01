<script setup lang="ts">
import { computed } from 'vue'
import { color } from 'echarts/core'
import { NEmpty, useThemeVars } from 'naive-ui'
import type { AnalysisNode } from '../api/storage-analysis'
import { containsAnalysisNode, type ExplorerMode } from '../composables/useStorageExplorer'
import { VChart } from '../composables/useEChartsLoader'
import { formatBytes, formatNumber } from '../shared/formatters'
import { ageBucket, formatTreemapLabel, getTreemapTone } from '../shared/treemap-presentation'

const props = defineProps<{
  nodes: AnalysisNode[]
  mode: ExplorerMode
  selectedPath?: string
  selectedKind?: AnalysisNode['kind']
  markedNodes: AnalysisNode[]
  scannedAt: string | null
}>()
const emit = defineEmits<{ select: [node: AnalysisNode]; open: [node: AnalysisNode] }>()
const theme = useThemeVars()
const metric = computed(() => (props.mode === 'count' ? 'count' : 'size'))
const total = computed(() =>
  props.nodes.reduce((sum, node) => sum + (metric.value === 'count' ? node.file_count : node.bytes), 0),
)
const palette = computed(() => [
  theme.value.primaryColor,
  theme.value.warningColor,
  theme.value.infoColor,
  theme.value.successColor,
  theme.value.errorColor,
  theme.value.textColor2,
])
const ageColors = computed(() => [
  theme.value.successColor,
  theme.value.primaryColor,
  theme.value.infoColor,
  theme.value.warningColor,
  theme.value.errorColor,
  theme.value.textColor3,
])
const hasArea = computed(() => props.nodes.some((node) => (props.mode === 'count' ? node.file_count : node.bytes) > 0))
const legend = computed(() =>
  props.mode === 'age'
    ? [
        ...['7 天内', '7–30 天', '1–6 个月', '半年–一年', '一年以上', '时间未知'].map((name, index) => ({
          name,
          color: ageColors.value[index],
        })),
      ]
    : props.nodes
        .filter((node) => node.bytes > 0 || node.file_count > 0)
        .slice(0, 8)
        .map((node) => ({ name: node.name, color: getTreemapTone(node, palette.value).accent })),
)

function isSelected(node: AnalysisNode) {
  return props.selectedPath === node.path && props.selectedKind === node.kind
}

function isMarked(node: AnalysisNode) {
  return props.markedNodes.some((mark) => containsAnalysisNode(mark, node))
}

function chartNode(node: AnalysisNode, parentAccent?: string, parentId = ''): object {
  const tone = getTreemapTone(node, parentAccent ? [parentAccent] : palette.value)
  const accent =
    props.mode === 'age' ? (ageColors.value[ageBucket(node.modified_at, props.scannedAt)] ?? tone.accent) : tone.accent
  const grouped = Boolean(node.children?.length)
  const value = metric.value === 'count' ? node.file_count : node.bytes
  const share = total.value > 0 ? value / total.value : 0
  const significant = share >= 0.15
  const ink = getTreemapTone(node, [accent]).text
  const selected = isSelected(node)
  const marked = isMarked(node)
  const { children, ...entry } = node
  const id = `${parentId}/${node.kind}:${node.path}`
  const rich = {
    name: { color: theme.value.textColor1, fontSize: significant ? 15 : 12, fontWeight: 600, lineHeight: 24 },
    amount: {
      color: ink,
      fontSize: significant ? 28 : 16,
      fontWeight: 600,
      lineHeight: significant ? 40 : 28,
    },
    detail: { color: theme.value.textColor2, fontSize: 11, lineHeight: 22 },
    header: { color: ink, fontSize: 13, fontWeight: 600, lineHeight: 40, verticalAlign: 'middle' },
    meta: {
      color: theme.value.textColor2,
      fontSize: 11,
      lineHeight: 40,
      verticalAlign: 'middle',
      padding: [0, 0, 0, 10],
    },
  }
  return {
    id,
    name: node.name,
    node: entry,
    value,
    children: children?.map((child) => chartNode(child, tone.accent, id)),
    itemStyle: {
      color: color.lift(accent, parentAccent ? 0.86 : 0.93),
      borderColor: selected ? theme.value.primaryColor : marked ? theme.value.warningColor : color.lift(accent, 0.68),
      borderWidth: selected || marked ? 2 : 1,
      borderRadius: 8,
      gapWidth: grouped ? 6 : 0,
    },
    label: {
      padding: significant ? 18 : 10,
      rich,
      formatter: () =>
        share < 0.08
          ? `{name|${entry.name}}\n{detail|${(share * 100).toFixed(1)}%}`
          : formatTreemapLabel(entry, metric.value, total.value),
    },
    upperLabel: {
      show: grouped,
      height: 40,
      lineHeight: 40,
      verticalAlign: 'middle',
      padding: [0, 12],
      rich,
      formatter: () =>
        `{header|${marked ? '✓ ' : ''}${node.name}}{meta|${metric.value === 'count' ? formatNumber(node.file_count) + ' 个文件' : formatBytes(node.bytes)}}`,
    },
    emphasis: {
      itemStyle: { borderColor: accent, shadowColor: color.modifyAlpha(accent, 0.13), shadowBlur: 10 },
      label: { show: true },
    },
  }
}

const option = computed(() => ({
  animationDurationUpdate: 320,
  animationEasingUpdate: 'cubicOut' as const,
  tooltip: {
    renderMode: 'richText',
    confine: true,
    formatter: (params: { data?: { node?: AnalysisNode } }) => {
      const node = params.data?.node
      if (!node) return ''
      const value = metric.value === 'count' ? node.file_count : node.bytes
      return `${node.path || node.name}\n逻辑容量  ${formatBytes(node.bytes)}\n文件数  ${formatNumber(node.file_count)}\n占当前视图  ${total.value > 0 ? ((value / total.value) * 100).toFixed(1) : '0.0'}%`
    },
  },
  series: [
    {
      type: 'treemap',
      animationDuration: 180,
      animationDurationUpdate: 220,
      left: 8,
      right: 8,
      top: 8,
      bottom: 8,
      roam: false,
      nodeClick: false,
      breadcrumb: { show: false },
      sort: 'desc',
      visibleMin: 24,
      childrenVisibleMin: 3000,
      label: {
        show: true,
        position: 'insideTopLeft',
        color: theme.value.textColor1,
        fontFamily: theme.value.fontFamily,
        overflow: 'truncate',
        formatter: (params: { data?: { node?: AnalysisNode } }) => {
          return formatTreemapLabel(params.data?.node, metric.value, total.value)
        },
      },
      upperLabel: { show: false },
      itemStyle: { gapWidth: 8, borderWidth: 0 },
      data: props.nodes.map((node) => chartNode(node)),
    },
  ],
}))

function onChartClick(params: { data?: unknown }, open = false) {
  if (!params.data || typeof params.data !== 'object' || !('node' in params.data)) return
  const node = params.data.node as AnalysisNode | undefined
  if (!node) return
  if (open) emit('open', node)
  else emit('select', node)
}
</script>

<template>
  <div data-testid="storage-treemap" class="min-w-0">
    <div class="h-[400px] rounded-xl border border-gray-100 bg-gray-50/70 overflow-hidden">
      <VChart
        v-if="hasArea"
        :option="option"
        autoresize
        class="h-full w-full"
        @click="onChartClick($event)"
        @dblclick="onChartClick($event, true)"
      />
      <div v-else class="h-full flex items-center justify-center px-6">
        <NEmpty
          :description="
            mode === 'count' ? '此范围没有文件，请查看下方目录列表' : '此范围没有可绘制的容量，可切换文件数或查看列表'
          "
        />
      </div>
    </div>
    <div class="flex flex-wrap items-center gap-x-4 gap-y-2 pt-3 text-xs text-gray-500">
      <span
        v-for="item in legend"
        :key="item.name"
        class="inline-flex items-center gap-1.5 max-w-[140px]"
        :title="item.name"
      >
        <svg width="8" height="8" aria-hidden="true"><rect width="8" height="8" rx="2" :fill="item.color" /></svg>
        <span class="truncate">{{ item.name }}</span>
      </span>
      <span class="ml-auto text-gray-400">单击查看详情 · 双击目录进入</span>
    </div>
    <p v-if="mode === 'age'" class="m-0 mt-2 text-xs text-gray-400">
      面积仍表示逻辑容量；目录颜色按其后代最近修改时间显示，不代表访问频率。
    </p>
  </div>
</template>
