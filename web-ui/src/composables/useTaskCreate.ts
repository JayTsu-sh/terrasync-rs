import { ref, computed } from 'vue'
import type { Endpoint } from '../api/endpoints'
import { listEndpoints } from '../api/endpoints'
import { createTask } from '../api/tasks'
import { getFilterFields } from '../api/filters'
import type { FilterField } from '../api/filters'
import type { ConditionGroup } from '../shared/condition-serializer'
import { serializeConditionGroups } from '../shared/condition-serializer'
import { needsDest, needsAdvancedOptions, taskTypeLabelMap } from '../shared/task-rules'

export class ValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ValidationError'
  }
}

export function useTaskCreate() {
  const form = ref({
    name: '',
    task_type: 'sync',
    source_endpoint_id: null as string | null,
    dest_endpoint_id: null as string | null,
    enable_integrity_check: false,
    qos: '',
    block_size: '',
    iops: undefined as number | undefined,
    concurrency: 8,
  })

  const includeGroups = ref<ConditionGroup[]>([])
  const excludeGroups = ref<ConditionGroup[]>([])

  const endpoints = ref<Endpoint[]>([])
  const filterFields = ref<FilterField[]>([])
  const submitting = ref(false)

  const taskTypeOptions = Object.entries(taskTypeLabelMap).map(([value, label]) => ({ label, value }))

  const needDest = computed(() => needsDest(form.value.task_type))
  const showAdvanced = computed(() => needsAdvancedOptions(form.value.task_type))
  const showFilter = computed(() => form.value.task_type !== 'integrity_check')

  const endpointOptions = computed(() =>
    endpoints.value.map((e) => ({ label: `${e.name} (${e.endpoint_type_display})`, value: e.id })),
  )

  const sourceEndpoint = computed(() => endpoints.value.find((e) => e.id === form.value.source_endpoint_id) ?? null)
  const destEndpoint = computed(() => endpoints.value.find((e) => e.id === form.value.dest_endpoint_id) ?? null)

  const sourceLabel = computed(() => (form.value.task_type === 'scan' ? '扫描目标' : '源迁移目标'))
  const sourcePlaceholder = computed(() => (form.value.task_type === 'scan' ? '选择扫描目标' : '选择源迁移目标'))

  async function fetchData() {
    const [eps, fields] = await Promise.all([listEndpoints(), getFilterFields()])
    endpoints.value = eps
    filterFields.value = fields
  }

  async function handleSubmit() {
    if (!form.value.name) throw new ValidationError('请填写任务名称')
    if (!form.value.source_endpoint_id) throw new ValidationError('请选择源端点')
    if (needDest.value && !form.value.dest_endpoint_id) throw new ValidationError('请选择目标迁移目标')

    const matchExpr = showFilter.value ? serializeConditionGroups(includeGroups.value) : ''
    const excludeExpr = showFilter.value ? serializeConditionGroups(excludeGroups.value) : ''

    submitting.value = true
    try {
      return await createTask({
        name: form.value.name,
        task_type: form.value.task_type,
        source_endpoint_id: form.value.source_endpoint_id,
        dest_endpoint_id: needDest.value ? form.value.dest_endpoint_id! : undefined,
        config: {
          match_expr: matchExpr || undefined,
          exclude_expr: excludeExpr || undefined,
          enable_integrity_check: form.value.enable_integrity_check,
          qos: form.value.qos || undefined,
          block_size: form.value.block_size || undefined,
          iops: form.value.iops,
          concurrency: form.value.concurrency,
        },
      })
    } finally {
      submitting.value = false
    }
  }

  function resetForm() {
    form.value = {
      name: '',
      task_type: 'sync',
      source_endpoint_id: null,
      dest_endpoint_id: null,
      enable_integrity_check: false,
      qos: '',
      block_size: '',
      iops: undefined,
      concurrency: 8,
    }
    includeGroups.value = []
    excludeGroups.value = []
  }

  return {
    form,
    includeGroups,
    excludeGroups,
    endpoints,
    filterFields,
    submitting,
    taskTypeOptions,
    needDest,
    showAdvanced,
    showFilter,
    sourceEndpoint,
    destEndpoint,
    endpointOptions,
    sourceLabel,
    sourcePlaceholder,
    fetchData,
    handleSubmit,
    resetForm,
  }
}
