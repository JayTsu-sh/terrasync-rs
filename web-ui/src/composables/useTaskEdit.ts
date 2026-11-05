import { ref, computed } from 'vue'
import type { Endpoint } from '../api/endpoints'
import { listEndpoints } from '../api/endpoints'
import type { MigrationTask, TaskConfig } from '../api/tasks'
import { updateTask } from '../api/tasks'
import { getFilterFields } from '../api/filters'
import type { FilterField } from '../api/filters'
import type { ConditionGroup } from '../shared/condition-serializer'
import { serializeConditionGroups, deserializeConditionGroups } from '../shared/condition-serializer'
import { needsDest, needsAdvancedOptions, taskTypeLabelMap } from '../shared/task-rules'

export class ValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ValidationError'
  }
}

export function useTaskEdit() {
  const form = ref({
    name: '',
    task_type: 'sync',
    source_endpoint_id: null as string | null,
    dest_endpoint_id: null as string | null,
    enable_integrity_check: false,
    scan_resume: false,
    qos: '',
    block_size: '',
    iops: undefined as number | undefined,
    concurrency: 8,
    depth: 0,
  })

  const originalConfig = ref<TaskConfig>({})
  const originalSource = ref<string | null>(null)
  let originalIncludes = ''
  let originalExcludes = ''

  const includeGroups = ref<ConditionGroup[]>([])
  const excludeGroups = ref<ConditionGroup[]>([])

  const endpoints = ref<Endpoint[]>([])
  const filterFields = ref<FilterField[]>([])
  const submitting = ref(false)

  const needDest = computed(() => needsDest(form.value.task_type))
  const showAdvanced = computed(() => needsAdvancedOptions(form.value.task_type))
  const showFilter = computed(() => form.value.task_type !== 'integrity_check')

  const endpointOptions = computed(() =>
    endpoints.value.map((e) => ({ label: `${e.name} (${e.endpoint_type_display})`, value: e.id })),
  )

  const taskTypeLabel = computed(() => taskTypeLabelMap[form.value.task_type] || form.value.task_type)

  const sourceLabel = computed(() => (form.value.task_type === 'scan' ? '扫描目标' : '源迁移目标'))
  const sourcePlaceholder = computed(() => (form.value.task_type === 'scan' ? '选择扫描目标' : '选择源迁移目标'))

  function populateForm(task: MigrationTask) {
    originalConfig.value = { ...task.config }
    originalSource.value = task.source_endpoint_id
    form.value = {
      name: task.name,
      task_type: task.task_type,
      source_endpoint_id: task.source_endpoint_id,
      dest_endpoint_id: task.dest_endpoint_id ?? null,
      enable_integrity_check: task.config.enable_integrity_check ?? false,
      scan_resume: task.config.scan_resume ?? false,
      qos: task.config.qos || '',
      block_size: task.config.block_size || '',
      iops: task.config.iops,
      concurrency: task.config.concurrency ?? 8,
      depth: task.config.depth ?? 0,
    }
    includeGroups.value = deserializeConditionGroups(task.config.match_expr || '')
    excludeGroups.value = deserializeConditionGroups(task.config.exclude_expr || '')
    originalIncludes = JSON.stringify(includeGroups.value)
    originalExcludes = JSON.stringify(excludeGroups.value)
  }

  async function fetchData() {
    const [eps, fields] = await Promise.all([listEndpoints(), getFilterFields()])
    endpoints.value = eps
    filterFields.value = fields
  }

  async function handleSubmit(taskId: string) {
    if (!form.value.name) throw new ValidationError('请填写任务名称')
    if (!form.value.source_endpoint_id) throw new ValidationError('请选择源端点')

    const matchExpr = JSON.stringify(includeGroups.value) === originalIncludes
      ? originalConfig.value.match_expr || ''
      : showFilter.value ? serializeConditionGroups(includeGroups.value) : ''
    const excludeExpr = JSON.stringify(excludeGroups.value) === originalExcludes
      ? originalConfig.value.exclude_expr || ''
      : showFilter.value ? serializeConditionGroups(excludeGroups.value) : ''

    const config: TaskConfig = {
      ...originalConfig.value,
      depth: form.value.depth ?? 0,
      match_expr: matchExpr || undefined,
      exclude_expr: excludeExpr || undefined,
      enable_integrity_check: form.value.enable_integrity_check,
          scan_resume: form.value.scan_resume,
      qos: form.value.qos || undefined,
      block_size: form.value.block_size || undefined,
      iops: form.value.iops,
      concurrency: form.value.concurrency,
    }

    submitting.value = true
    try {
      return await updateTask(taskId, {
        name: form.value.name,
        source_endpoint_id: form.value.source_endpoint_id!,
        ...(form.value.source_endpoint_id !== originalSource.value ? { source_path_id: '' } : {}),
        config,
      })
    } finally {
      submitting.value = false
    }
  }

  return {
    form,
    includeGroups,
    excludeGroups,
    endpoints,
    filterFields,
    submitting,
    needDest,
    showAdvanced,
    showFilter,
    endpointOptions,
    taskTypeLabel,
    sourceLabel,
    sourcePlaceholder,
    populateForm,
    fetchData,
    handleSubmit,
  }
}
