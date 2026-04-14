import { h } from 'vue'
import { NTag } from 'naive-ui'
import type { DataTableColumn } from 'naive-ui'
import type { MigrationTask } from '../api/tasks'
import { getListActions, statusColorMap, statusLabelMap, taskTypeLabelMap } from '../shared/task-rules'
import type { TaskStatus } from '../shared/task-rules'

export function useTaskColumns(callbacks: {
  onDetail: (id: string) => void
  onStart: (id: string) => void
  onCancel: (id: string) => void
  onDelete: (task: MigrationTask) => void
}) {
  const actionHandlers: Record<string, (row: MigrationTask) => void> = {
    detail: (row) => callbacks.onDetail(row.id),
    start: (row) => callbacks.onStart(row.id),
    cancel: (row) => callbacks.onCancel(row.id),
    delete: (row) => callbacks.onDelete(row),
  }

  const columns: DataTableColumn<MigrationTask>[] = [
    {
      title: '名称',
      key: 'name',
      width: 220,
      sorter: (a, b) => a.name.localeCompare(b.name),
      // ⚠️ 非默认: font-medium 用于名称加粗
      render: (row) => h('span', { class: 'font-medium text-gray-900' }, row.name),
    },
    {
      title: '操作',
      key: 'actions',
      width: 160,
      render: (row) => {
        const actions = getListActions(row.status as TaskStatus)
        // ⚠️ 非默认: 语义颜色(蓝详情/绿启动/黄取消/红删除) + cursor-pointer
        const links = actions.map((a) =>
          h('span', { class: `${a.color} cursor-pointer`, onClick: () => actionHandlers[a.key](row) }, a.label),
        )
        return h('div', { class: 'flex gap-3' }, links)
      },
    },
    {
      title: '类型',
      key: 'task_type',
      width: 120,
      align: 'center',
      render: (row) => taskTypeLabelMap[row.task_type] || row.task_type,
    },
    {
      title: '状态',
      key: 'status',
      width: 100,
      align: 'center',
      filterOptions: [
        { label: '等待中', value: 'pending' },
        { label: '运行中', value: 'running' },
        { label: '已完成', value: 'completed' },
        { label: '失败', value: 'failed' },
        { label: '已取消', value: 'cancelled' },
      ],
      filter: (value, row) => row.status === String(value),
      render: (row) =>
        h(
          NTag,
          { type: statusColorMap[row.status as TaskStatus] as any, size: 'small' },
          { default: () => statusLabelMap[row.status as TaskStatus] || row.status },
        ),
    },
    {
      title: '创建时间',
      key: 'created_at',
      width: 170,
      align: 'center',
      // ⚠️ 非默认: font-mono 用于等宽数字对齐
      render: (row) => h('span', { class: 'text-gray-500 font-mono' }, new Date(row.created_at).toLocaleString()),
    },
  ]

  return { columns }
}
