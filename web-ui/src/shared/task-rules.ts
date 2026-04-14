// 任务操作规则唯一来源 — 新增约束只改这里

export type TaskStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'

/** 操作允许性规则 */
export const TASK_ACTIONS = {
  canStart: (s: TaskStatus) => s === 'pending' || s === 'cancelled' || s === 'failed' || s === 'completed',
  canCancel: (s: TaskStatus) => s === 'running',
  canDelete: (s: TaskStatus) => s !== 'running',
  canEdit: (s: TaskStatus) => s !== 'running',
} as const

/** 列表页每行操作的颜色映射 */
export type ListAction = { label: string; key: string; color: string }

/** 根据状态返回列表页该显示的操作列表 */
export function getListActions(status: TaskStatus): ListAction[] {
  const actions: ListAction[] = [{ label: '详情', key: 'detail', color: 'text-blue-500' }]
  if (TASK_ACTIONS.canStart(status)) actions.push({ label: '启动', key: 'start', color: 'text-green-500' })
  if (TASK_ACTIONS.canCancel(status)) actions.push({ label: '取消', key: 'cancel', color: 'text-yellow-500' })
  if (TASK_ACTIONS.canDelete(status)) actions.push({ label: '删除', key: 'delete', color: 'text-red-500' })
  return actions
}

/** 状态 → NTag type 映射 */
type TagType = 'default' | 'info' | 'success' | 'error' | 'warning' | 'primary'
export const statusColorMap: Record<TaskStatus, TagType> = {
  pending: 'default',
  running: 'info',
  completed: 'success',
  failed: 'error',
  cancelled: 'warning',
}

/** 状态 → 中文标签 */
export const statusLabelMap: Record<TaskStatus, string> = {
  pending: '等待中',
  running: '运行中',
  completed: '已完成',
  failed: '失败',
  cancelled: '已取消',
}

/** 任务类型 → 中文标签 */
export const taskTypeLabelMap: Record<string, string> = {
  scan: '扫描',
  sync: '迁移',
  integrity_check: '一致性检查',
}

/** 任务类型是否需要目标端点 */
export const needsDest = (taskType: string) => taskType === 'sync' || taskType === 'integrity_check'

/** 任务类型是否显示高级选项（QoS 等） */
export const needsAdvancedOptions = (taskType: string) => taskType === 'sync'
