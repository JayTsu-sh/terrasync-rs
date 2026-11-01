<script setup lang="ts">
import { computed, h, ref, toRef } from 'vue'
import {
  NAlert,
  NBreadcrumb,
  NBreadcrumbItem,
  NButton,
  NButtonGroup,
  NDataTable,
  NEmpty,
  NIcon,
  NInput,
  NModal,
  NResult,
  NSpin,
  NTag,
  type DataTableColumns,
} from 'naive-ui'
import {
  ArrowBackOutline,
  ArrowForwardOutline,
  ArrowUpOutline,
  RefreshOutline,
  SearchOutline,
  FolderOutline,
  DocumentOutline,
  GridOutline,
  DownloadOutline,
  CheckmarkOutline,
  CloseOutline,
} from '@vicons/ionicons5'
import type { AnalysisNode } from '../api/storage-analysis'
import { useStorageExplorer, type ExplorerMode } from '../composables/useStorageExplorer'
import { useTreemapPreview } from '../composables/useTreemapPreview'
import { formatBytes, formatNumber } from '../shared/formatters'
import StorageTreemap from './StorageTreemap.vue'

const props = withDefaults(defineProps<{ taskId: string; active?: boolean }>(), { active: true })
const {
  data,
  loading,
  error,
  mode,
  path,
  search,
  selected,
  marks,
  canBack,
  canForward,
  visibleNodes,
  markedBytes,
  markedFiles,
  breadcrumbs,
  reload,
  navigate,
  openNode,
  back,
  forward,
  coveringMark,
  toggleMark,
  exportSelection,
} = useStorageExplorer(toRef(props, 'taskId'), toRef(props, 'active'))
const showReview = ref(false)
const previewEnabled = ref(true)
const previewMetric = computed(() => (mode.value === 'count' ? 'count' : 'size'))
const preview = useTreemapPreview(toRef(props, 'taskId'), data, previewMetric, previewEnabled)
const chartNodes = computed(() => {
  const visible = new Set(visibleNodes.value.map((node) => `${node.kind}:${node.path}`))
  return preview.nodes.value.filter((node) => visible.has(`${node.kind}:${node.path}`))
})
const modes: { value: ExplorerMode; label: string }[] = [
  { value: 'size', label: '逻辑容量' },
  { value: 'count', label: '文件数量' },
  { value: 'age', label: '修改年龄' },
]
const displayNode = computed<AnalysisNode | null>(
  () =>
    selected.value ??
    (data.value
      ? {
          path: data.value.path,
          name: data.value.path.split('/').at(-1) || '扫描根目录',
          kind: 'directory',
          bytes: data.value.total_bytes,
          file_count: data.value.file_count,
          directory_count: data.value.directory_count,
          modified_at:
            data.value.children
              .map((node) => node.modified_at)
              .filter((value): value is string => Boolean(value))
              .sort()
              .at(-1) ?? null,
        }
      : null),
)
const selectedShare = computed(() =>
  data.value && displayNode.value && data.value.total_bytes > 0
    ? ((displayNode.value.bytes / data.value.total_bytes) * 100).toFixed(1)
    : '0.0',
)
const selectedCover = computed(() => (selected.value ? coveringMark(selected.value) : undefined))
const childCovered = computed(() => selectedCover.value && selectedCover.value.path !== selected.value?.path)
const visibleBytes = computed(() => visibleNodes.value.reduce((sum, node) => sum + node.bytes, 0))
const hasOverflow = computed(() => data.value?.children.some((node) => node.kind === 'other'))
const scanTime = computed(() => (data.value?.scanned_at ? formatTime(data.value.scanned_at) : '时间未知'))

function formatTime(value: string | null) {
  if (!value) return '未知'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? '未知' : date.toLocaleString('zh-CN', { hour12: false })
}

function selectNode(node: AnalysisNode) {
  selected.value = node
}

function saveSelection() {
  const blob = new Blob([exportSelection()], { type: 'application/json;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `terrasync-selection-${props.taskId}.json`
  anchor.click()
  URL.revokeObjectURL(url)
}

function rowProps(row: AnalysisNode) {
  return {
    tabindex: 0,
    'aria-label': `选择 ${row.name}`,
    class:
      selected.value?.path === row.path && selected.value?.kind === row.kind
        ? 'bg-blue-50 cursor-pointer'
        : 'cursor-pointer',
    onClick: () => selectNode(row),
    onDblclick: () => {
      void openNode(row)
    },
    onKeydown: (event: KeyboardEvent) => {
      if (event.target !== event.currentTarget) return
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault()
        selectNode(row)
      }
    },
  }
}

const columns = computed<DataTableColumns<AnalysisNode>>(() => [
  {
    title: '名称',
    key: 'name',
    minWidth: 200,
    ellipsis: { tooltip: true },
    render: (row) =>
      h('div', { class: 'flex items-center gap-2 min-w-0' }, [
        h(
          NIcon,
          { class: row.kind === 'directory' ? 'text-blue-500 shrink-0' : 'text-gray-400 shrink-0', size: 18 },
          {
            default: () =>
              h(row.kind === 'directory' ? FolderOutline : row.kind === 'file' ? DocumentOutline : GridOutline),
          },
        ),
        h('span', { class: 'truncate', title: row.path || row.name }, row.name),
        coveringMark(row)
          ? h(NTag, { size: 'small', type: 'warning', bordered: false }, { default: () => '已标记' })
          : null,
      ]),
  },
  {
    title: '逻辑容量',
    key: 'bytes',
    width: 130,
    sorter: (a, b) => a.bytes - b.bytes,
    render: (row) => formatBytes(row.bytes),
  },
  {
    title: '文件数',
    key: 'file_count',
    width: 110,
    sorter: (a, b) => a.file_count - b.file_count,
    render: (row) => formatNumber(row.file_count),
  },
  {
    title: '容量占比',
    key: 'share',
    width: 100,
    render: (row) => `${data.value?.total_bytes ? ((row.bytes / data.value.total_bytes) * 100).toFixed(1) : '0.0'}%`,
  },
  {
    title: '操作',
    key: 'actions',
    width: 150,
    render: (row) =>
      row.kind === 'other'
        ? h('span', { class: 'text-gray-400' }, '汇总项')
        : h('div', { class: 'flex items-center gap-3' }, [
            row.kind === 'directory'
              ? h(
                  NButton,
                  {
                    text: true,
                    type: 'primary',
                    'aria-label': `进入 ${row.name}`,
                    onClick: (event: MouseEvent) => {
                      event.stopPropagation()
                      void openNode(row)
                    },
                  },
                  { default: () => '进入' },
                )
              : null,
            h(
              NButton,
              {
                text: true,
                type: coveringMark(row) ? 'warning' : 'primary',
                disabled: Boolean(coveringMark(row) && coveringMark(row)?.path !== row.path),
                'aria-label': `${coveringMark(row) ? '取消标记' : '标记'} ${row.name}`,
                onClick: (event: MouseEvent) => {
                  event.stopPropagation()
                  toggleMark(row)
                },
              },
              { default: () => (coveringMark(row) ? '已标记' : '标记') },
            ),
          ]),
  },
])
</script>

<template>
  <section data-testid="storage-explorer" class="bg-white rounded-lg border border-gray-100 shadow-sm overflow-hidden">
    <div class="px-5 py-4 border-b border-gray-100 flex items-start justify-between gap-4">
      <div>
        <h2 class="m-0 text-lg font-semibold text-gray-900 flex items-center gap-2">
          <NIcon :size="20" class="text-blue-500"><GridOutline /></NIcon> 存储空间分析
        </h2>
        <p class="m-0 mt-1 text-sm text-gray-500">逐层查看数据分布，标记需要进一步处理的目录与文件。</p>
      </div>
      <NTag size="small" :bordered="false" type="info" class="shrink-0">最新扫描结果</NTag>
    </div>

    <div
      v-if="loading"
      class="flex flex-col items-center justify-center gap-4 min-h-[480px] text-gray-400"
      role="status"
    >
      <NSpin :size="28" /><span>正在读取目录分析…</span>
    </div>
    <div v-else-if="error" class="p-8 min-h-[360px] flex items-center justify-center" role="alert">
      <NResult status="warning" title="暂时无法加载分析" :description="error">
        <template #footer
          ><div class="flex justify-center gap-3">
            <NButton v-if="path" @click="navigate('')">返回扫描根目录</NButton
            ><NButton @click="reload">重新加载</NButton>
          </div></template
        >
      </NResult>
    </div>
    <template v-else-if="data">
      <div class="px-5 pt-4">
        <NAlert v-if="data.is_partial" type="warning" :bordered="false" class="mb-4"
          >此结果包含扫描错误或尚未完成，仅表示已观测数据，不能作为完整存储清单。</NAlert
        >
        <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-4">
          <div class="rounded-lg bg-gray-50 px-4 py-3">
            <div class="text-xs text-gray-500 mb-1">当前目录 · 逻辑容量</div>
            <div class="text-2xl text-gray-900 font-semibold tabular-nums">{{ formatBytes(data.total_bytes) }}</div>
          </div>
          <div class="rounded-lg bg-gray-50 px-4 py-3">
            <div class="text-xs text-gray-500 mb-1">文件数量</div>
            <div class="text-2xl text-gray-900 font-semibold tabular-nums">{{ formatNumber(data.file_count) }}</div>
          </div>
          <div class="rounded-lg bg-gray-50 px-4 py-3">
            <div class="text-xs text-gray-500 mb-1">子目录数量</div>
            <div class="text-2xl text-gray-900 font-semibold tabular-nums">
              {{ formatNumber(data.directory_count) }}
            </div>
          </div>
        </div>
        <div class="flex flex-wrap items-center justify-between gap-3 border-y border-gray-100 py-3">
          <div class="flex items-center gap-3 min-w-0">
            <NButtonGroup size="small">
              <NButton aria-label="返回上一目录" :disabled="!canBack" @click="back"
                ><template #icon
                  ><NIcon><ArrowBackOutline /></NIcon></template
              ></NButton>
              <NButton aria-label="前进下一目录" :disabled="!canForward" @click="forward"
                ><template #icon
                  ><NIcon><ArrowForwardOutline /></NIcon></template
              ></NButton>
              <NButton
                aria-label="上一级目录"
                :disabled="!path"
                @click="navigate(path.split('/').slice(0, -1).join('/'))"
                ><template #icon
                  ><NIcon><ArrowUpOutline /></NIcon></template
              ></NButton>
            </NButtonGroup>
            <NBreadcrumb>
              <NBreadcrumbItem v-for="crumb in breadcrumbs" :key="crumb.path" @click="navigate(crumb.path)">{{
                crumb.name
              }}</NBreadcrumbItem>
            </NBreadcrumb>
          </div>
          <div class="flex items-center gap-2">
            <NButtonGroup size="small">
              <NButton
                v-for="item in modes"
                :key="item.value"
                :type="mode === item.value ? 'primary' : 'default'"
                @click="mode = item.value"
                >{{ item.label }}</NButton
              >
            </NButtonGroup>
            <NButton size="small" aria-label="刷新目录分析" @click="reload"
              ><template #icon
                ><NIcon><RefreshOutline /></NIcon></template
            ></NButton>
          </div>
        </div>
      </div>

      <div class="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_280px]">
        <div class="p-5 min-w-0">
          <div class="flex flex-wrap items-center justify-between gap-3 mb-3">
            <div class="text-sm text-gray-500">
              <span class="font-medium text-gray-900">目录空间分布</span>
              <span class="ml-2 text-xs text-gray-400">{{ data.children.length }} 个显示项</span>
            </div>
            <div class="flex flex-wrap items-center gap-3">
              <NSpin v-if="preview.loading.value" :size="12" aria-label="正在展开子目录" />
              <NButtonGroup size="small">
                <NButton
                  :type="!previewEnabled ? 'primary' : 'default'"
                  aria-label="显示一层目录"
                  @click="previewEnabled = false"
                  >1 层</NButton
                >
                <NButton
                  :type="previewEnabled ? 'primary' : 'default'"
                  aria-label="显示两层目录"
                  @click="previewEnabled = true"
                  >2 层</NButton
                >
              </NButtonGroup>
              <div class="w-full sm:w-[200px]">
                <NInput v-model:value="search" clearable placeholder="搜索当前显示项" aria-label="搜索当前显示项"
                  ><template #prefix
                    ><NIcon><SearchOutline /></NIcon></template
                ></NInput>
              </div>
            </div>
          </div>
          <div v-if="search.trim()" class="mb-3 text-xs text-gray-500" role="status">
            仅筛选当前已加载的 {{ data.children.length }} 项，匹配 {{ visibleNodes.length }} 项 /
            {{ formatBytes(visibleBytes) }}；顶部总量保持不变。
          </div>
          <StorageTreemap
            :nodes="chartNodes"
            :mode="mode"
            :selected-path="selected?.path"
            :selected-kind="selected?.kind"
            :marked-nodes="marks"
            :scanned-at="data.scanned_at"
            @select="selectNode"
            @open="openNode"
          />
          <p v-if="hasOverflow" class="m-0 mt-3 text-xs text-gray-400">
            “其他”为未单独显示项目的合计，不对应真实路径；按容量或数量切换可查看相应主要项目。
          </p>
        </div>

        <aside class="border-t xl:border-t-0 xl:border-l border-gray-100 p-5 bg-gray-50/50">
          <div class="flex items-center justify-between text-xs font-semibold text-gray-500 mb-4">
            <span>{{ selected ? '当前选择' : '当前目录' }}</span
            ><NTag v-if="displayNode" size="small" :bordered="false">{{
              displayNode.kind === 'directory' ? '目录' : displayNode.kind === 'file' ? '文件' : '汇总'
            }}</NTag>
          </div>
          <template v-if="displayNode">
            <div class="flex items-start gap-2 mb-3 text-gray-900 font-medium break-all">
              <NIcon :size="20" class="text-blue-500 shrink-0"
                ><FolderOutline v-if="displayNode.kind === 'directory'" /><DocumentOutline v-else /></NIcon
              >{{ displayNode.name }}
            </div>
            <div class="text-3xl font-semibold tabular-nums text-gray-900 tracking-tight">
              {{ formatBytes(displayNode.bytes) }}
            </div>
            <div class="text-xs text-gray-400 mt-1 mb-5">逻辑容量 · 占当前目录 {{ selectedShare }}%</div>
            <dl class="m-0 text-sm space-y-3">
              <div class="flex justify-between gap-2">
                <dt class="text-gray-500">文件数</dt>
                <dd class="m-0 tabular-nums text-gray-900">{{ formatNumber(displayNode.file_count) }}</dd>
              </div>
              <div class="flex justify-between gap-2">
                <dt class="text-gray-500">子目录数</dt>
                <dd class="m-0 tabular-nums text-gray-900">{{ formatNumber(displayNode.directory_count) }}</dd>
              </div>
              <div>
                <dt class="text-gray-500 mb-1">{{ selected ? '最近修改' : '显示项最近修改' }}</dt>
                <dd class="m-0 text-gray-900">{{ formatTime(displayNode.modified_at) }}</dd>
              </div>
              <div v-if="displayNode.path">
                <dt class="text-gray-500 mb-1">相对路径</dt>
                <dd class="m-0 break-all text-gray-900">{{ displayNode.path }}</dd>
              </div>
            </dl>
            <div v-if="selected" class="flex flex-col gap-2 mt-5">
              <NButton
                v-if="selected.kind === 'directory'"
                :aria-label="`进入 ${selected.name}`"
                @click="openNode(selected)"
                >进入目录<template #icon
                  ><NIcon><ArrowForwardOutline /></NIcon></template
              ></NButton>
              <NButton
                v-if="selected.kind !== 'other'"
                :type="selectedCover ? 'warning' : 'primary'"
                :disabled="Boolean(childCovered)"
                :aria-label="`${selectedCover ? '取消标记' : '标记'} ${selected.name}`"
                @click="toggleMark(selected)"
              >
                <template #icon
                  ><NIcon><CheckmarkOutline /></NIcon></template
                >{{ childCovered ? '父目录已标记' : selectedCover ? '取消标记' : '加入标记' }}
              </NButton>
            </div>
            <p v-else class="text-xs text-gray-400 mt-5 mb-0">单击区块或列表项查看详情，双击目录继续探索。</p>
          </template>
          <div v-else class="py-10"><NEmpty description="选择一个区块查看详情" /></div>

          <div class="border-t border-gray-200 mt-6 pt-4">
            <div class="flex justify-between items-center mb-3">
              <span class="text-sm font-medium text-gray-900"
                >已标记 <span class="text-gray-400 font-normal">{{ marks.length }} 项</span></span
              ><NButton v-if="marks.length" text size="small" @click="marks = []">清空</NButton>
            </div>
            <div v-if="marks.length" class="max-h-40 overflow-y-auto space-y-2">
              <div
                v-for="mark in marks"
                :key="`${mark.kind}:${mark.path}`"
                class="flex items-center justify-between gap-2 text-xs"
              >
                <span class="truncate text-gray-600" :title="mark.path">{{ mark.name }}</span>
                <NButton text size="tiny" :aria-label="`移除标记 ${mark.name}`" @click="toggleMark(mark)"
                  ><NIcon><CloseOutline /></NIcon
                ></NButton>
              </div>
            </div>
            <p v-else class="text-xs text-gray-400 m-0">标记目录或文件，复核后导出清单。</p>
            <div class="mt-3 flex justify-between text-sm">
              <span class="text-gray-500">已选逻辑容量</span
              ><span class="font-semibold text-gray-900">{{ formatBytes(markedBytes) }}</span>
            </div>
            <NButton block class="mt-4" :disabled="!marks.length" @click="showReview = true">复核已标记清单</NButton>
          </div>
        </aside>
      </div>

      <div class="px-5 pb-5 border-t border-gray-100 pt-4">
        <div class="flex flex-wrap items-center justify-between gap-2 mb-3">
          <h3 class="text-sm font-semibold text-gray-900 m-0">目录与文件</h3>
          <span class="text-xs text-gray-400">支持排序与键盘选择 · 零字节项目保留在列表中</span>
        </div>
        <NDataTable
          :columns="columns"
          :data="visibleNodes"
          :row-key="(node: AnalysisNode) => `${node.kind}:${node.path}`"
          :row-props="rowProps"
          :pagination="{ pageSize: 10 }"
          :scroll-x="720"
          striped
          :bordered="false"
        />
        <div class="flex flex-wrap justify-between gap-2 mt-4 text-xs text-gray-400">
          <span class="break-all">扫描来源：{{ data.source_path }}</span
          ><span>扫描时间：{{ scanTime }}</span>
        </div>
        <p class="m-0 mt-2 text-xs text-gray-400">容量仅表示扫描范围内的逻辑数据量，不代表物理磁盘占用或可释放空间。</p>
      </div>
    </template>
    <div v-else class="p-12"><NEmpty description="选择扫描任务后查看数据分析" /></div>

    <NModal
      v-model:show="showReview"
      preset="card"
      title="复核已标记清单"
      class="w-[680px] max-w-[calc(100vw-32px)]"
      :bordered="false"
    >
      <NAlert type="info" :bordered="false" class="mb-4"
        >此操作仅导出路径与扫描信息。父目录已包含其子项，容量不会重复累计。</NAlert
      >
      <div class="flex items-center justify-between text-sm mb-4">
        <span>{{ marks.length }} 项 · {{ formatNumber(markedFiles) }} 个文件</span
        ><strong>{{ formatBytes(markedBytes) }}</strong>
      </div>
      <div class="max-h-80 overflow-y-auto divide-y divide-gray-100">
        <div
          v-for="mark in marks"
          :key="`${mark.kind}:${mark.path}`"
          class="flex items-center justify-between gap-3 py-3"
        >
          <span class="break-all text-gray-900">{{ mark.path }}</span>
          <div class="flex items-center gap-3 shrink-0">
            <span class="text-gray-500">{{ formatBytes(mark.bytes) }}</span
            ><NButton text :aria-label="`移除标记 ${mark.name}`" @click="toggleMark(mark)">移除</NButton>
          </div>
        </div>
      </div>
      <NEmpty v-if="!marks.length" description="还没有标记任何项目" />
      <template #footer
        ><div class="flex justify-end gap-3">
          <NButton @click="showReview = false">返回分析</NButton
          ><NButton type="primary" :disabled="!marks.length" @click="saveSelection"
            ><template #icon
              ><NIcon><DownloadOutline /></NIcon></template
            >导出 JSON 清单</NButton
          >
        </div></template
      >
    </NModal>
  </section>
</template>
