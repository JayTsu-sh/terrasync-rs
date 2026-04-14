import { h } from 'vue'
import type { Ref } from 'vue'
import { NIcon, NTag } from 'naive-ui'
import type { DataTableColumn } from 'naive-ui'
import type { Endpoint } from '../api/endpoints'
import { getEndpointTypeInfo, getEndpointAddress } from '../shared/endpoint-display'

export function useEndpointColumns(
  connectivityStatus: Ref<Record<string, 'testing' | 'ok' | 'fail'>>,
  callbacks: {
    onDetail: (id: string) => void
    onDelete: (ep: Endpoint) => void
  },
) {
  function connectivityCell(id: string) {
    const status = connectivityStatus.value[id]
    const map = {
      testing: { dotClass: 'bg-yellow-400', text: '检测中', textClass: 'text-yellow-500' },
      ok: { dotClass: 'bg-green-500', text: '已连接', textClass: 'text-green-600' },
      fail: { dotClass: 'bg-red-500', text: '连接失败', textClass: 'text-red-500' },
    }
    const { dotClass, text, textClass } = map[status as keyof typeof map] || map.testing
    return h('div', { class: 'flex items-center gap-1.5' }, [
      h('span', { class: `inline-block w-2.5 h-2.5 rounded-full ${dotClass}` }),
      h('span', { class: textClass }, text),
    ])
  }

  function renderTypeCell(type: string) {
    const info = getEndpointTypeInfo(type)
    return h(
      NTag,
      {
        size: 'small',
        color: { color: info.bg, textColor: info.color, borderColor: info.border },
        class: 'min-w-[72px] justify-center',
      },
      {
        default: () => info.label,
        icon: () => h(NIcon, { size: 12 }, { default: () => h(info.icon) }),
      },
    )
  }

  const columns: DataTableColumn<Endpoint>[] = [
    {
      title: '名称',
      key: 'name',
      width: 180,
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (row) => h('span', { class: 'font-medium text-gray-900' }, row.name),
    },
    {
      title: '连接状态',
      key: 'connectivity',
      width: 120,
      filterOptions: [
        { label: '已连接', value: 'ok' },
        { label: '连接失败', value: 'fail' },
        { label: '检测中', value: 'testing' },
      ],
      filter: (value, row) => connectivityStatus.value[row.id] === String(value),
      render: (row) => connectivityCell(row.id),
    },
    {
      title: '类型',
      key: 'endpoint_type',
      width: 110,
      align: 'center',
      sorter: (a, b) => a.endpoint_type.localeCompare(b.endpoint_type),
      render: (row) => renderTypeCell(row.endpoint_type),
    },
    {
      title: '存储地址',
      key: 'summary',
      render: (row) =>
        h(
          'span',
          {
            class: 'text-gray-500 font-mono block overflow-hidden text-ellipsis whitespace-nowrap',
          },
          getEndpointAddress(row, { includePrefix: true }),
        ),
    },
    {
      title: '创建时间',
      key: 'created_at',
      width: 170,
      align: 'center',
      render: (row) => h('span', { class: 'text-gray-500 font-mono' }, new Date(row.created_at).toLocaleString()),
    },
    {
      title: '操作',
      key: 'actions',
      width: 120,
      align: 'center',
      render: (row) =>
        h('div', { class: 'flex gap-4 justify-center' }, [
          h('span', { class: 'text-blue-500 cursor-pointer', onClick: () => callbacks.onDetail(row.id) }, '详情'),
          h('span', { class: 'text-red-500 cursor-pointer', onClick: () => callbacks.onDelete(row) }, '删除'),
        ]),
    },
  ]

  return { columns }
}
