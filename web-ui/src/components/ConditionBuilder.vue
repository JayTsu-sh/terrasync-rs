<script setup lang="ts">
import { computed } from 'vue'
import { NSelect, NInput, NIcon, NTooltip } from 'naive-ui'
import { HelpCircleOutline } from '@vicons/ionicons5'
import type { FilterField } from '../api/filters'
import type { Condition, ConditionGroup } from '../shared/condition-serializer'

const props = withDefaults(
  defineProps<{
    fields: FilterField[]
    includeGroups: ConditionGroup[]
    excludeGroups: ConditionGroup[]
    readonly?: boolean
  }>(),
  {
    readonly: false,
  },
)

const emit = defineEmits<{
  'update:includeGroups': [value: ConditionGroup[]]
  'update:excludeGroups': [value: ConditionGroup[]]
}>()

const fieldOptions = computed(() => props.fields.map((f) => ({ label: f.label, value: f.name })))

function getOperatorOptions(fieldName: string) {
  const field = props.fields.find((f) => f.name === fieldName)
  if (!field) return []
  return field.operators.map((op) => ({ label: op.label, value: op.value }))
}

function getFieldDef(fieldName: string) {
  return props.fields.find((f) => f.name === fieldName)
}

// --- 通用组操作 ---

function updateCondition(
  groups: ConditionGroup[],
  groupIdx: number,
  condIdx: number,
  patch: Partial<Condition>,
): ConditionGroup[] {
  const updated = groups.map((g, gi) => {
    if (gi !== groupIdx) return g
    const conditions = g.conditions.map((c, ci) => {
      if (ci !== condIdx) return c
      if (patch.field && patch.field !== c.field) {
        return { field: patch.field, operator: '', value: '' }
      }
      return { ...c, ...patch }
    })
    return { conditions }
  })
  return updated
}

function addCondition(groups: ConditionGroup[], groupIdx: number): ConditionGroup[] {
  return groups.map((g, gi) => {
    if (gi !== groupIdx) return g
    return { conditions: [...g.conditions, { field: '', operator: '', value: '' }] }
  })
}

function removeCondition(groups: ConditionGroup[], groupIdx: number, condIdx: number): ConditionGroup[] {
  const updated = groups.map((g, gi) => {
    if (gi !== groupIdx) return g
    return { conditions: g.conditions.filter((_, ci) => ci !== condIdx) }
  })
  // 组内条件全删完时，自动移除该组
  return updated.filter((g) => g.conditions.length > 0)
}

function addGroup(groups: ConditionGroup[]): ConditionGroup[] {
  return [...groups, { conditions: [{ field: '', operator: '', value: '' }] }]
}

function removeGroup(groups: ConditionGroup[], groupIdx: number): ConditionGroup[] {
  return groups.filter((_, i) => i !== groupIdx)
}

// --- Include 操作 ---

function updateInclude(groupIdx: number, condIdx: number, patch: Partial<Condition>) {
  emit('update:includeGroups', updateCondition(props.includeGroups, groupIdx, condIdx, patch))
}
function addIncludeCondition(groupIdx: number) {
  emit('update:includeGroups', addCondition(props.includeGroups, groupIdx))
}
function removeIncludeCondition(groupIdx: number, condIdx: number) {
  emit('update:includeGroups', removeCondition(props.includeGroups, groupIdx, condIdx))
}
function addIncludeGroup() {
  emit('update:includeGroups', addGroup(props.includeGroups))
}
function removeIncludeGroup(groupIdx: number) {
  emit('update:includeGroups', removeGroup(props.includeGroups, groupIdx))
}

// --- Exclude 操作 ---

function updateExclude(groupIdx: number, condIdx: number, patch: Partial<Condition>) {
  emit('update:excludeGroups', updateCondition(props.excludeGroups, groupIdx, condIdx, patch))
}
function addExcludeCondition(groupIdx: number) {
  emit('update:excludeGroups', addCondition(props.excludeGroups, groupIdx))
}
function removeExcludeCondition(groupIdx: number, condIdx: number) {
  emit('update:excludeGroups', removeCondition(props.excludeGroups, groupIdx, condIdx))
}
function addExcludeGroup() {
  emit('update:excludeGroups', addGroup(props.excludeGroups))
}
function removeExcludeGroup(groupIdx: number) {
  emit('update:excludeGroups', removeGroup(props.excludeGroups, groupIdx))
}
</script>

<template>
  <div class="flex flex-col gap-3">
    <div class="flex items-center gap-1">
      <span class="text-xs text-gray-400 font-semibold tracking-wider">筛选条件</span>
      <NTooltip trigger="hover" :style="{ maxWidth: '320px' }">
        <template #trigger>
          <NIcon :size="14" class="text-gray-300 cursor-help"><HelpCircleOutline /></NIcon>
        </template>
        <div class="text-xs leading-relaxed">
          <div>包含条件（白名单）：只处理匹配的文件/目录</div>
          <div>排除条件（黑名单）：跳过匹配的文件/目录</div>
          <div class="mt-1">
            支持属性：name（文件名）、path（路径）、type（类型）、size（大小）、modified（修改时间）
          </div>
          <div>支持通配符：*（单层匹配）、**（任意深度）</div>
          <div class="mt-1">组内条件为 AND 关系，组间为 OR 关系</div>
        </div>
      </NTooltip>
    </div>
    <div class="flex flex-col gap-4 border border-gray-200 rounded-md p-4">
      <!-- 包含条件组 -->
      <template v-for="(group, gi) in includeGroups" :key="'inc-' + gi">
        <div v-if="gi > 0" class="text-center text-[10px] font-semibold text-gray-300 tracking-wider">OR</div>
        <div class="flex rounded-lg border border-blue-100 bg-white overflow-hidden">
          <div class="w-1 shrink-0 rounded-l bg-green-500"></div>
          <div class="flex-1 min-w-0">
            <div class="flex items-center justify-between px-4 py-3 border-b border-gray-100">
              <span class="text-sm font-semibold text-gray-900">包含条件</span>
              <button
                v-if="!readonly"
                class="flex items-center justify-center w-[22px] h-[22px] min-w-[22px] rounded-full border border-gray-200 bg-transparent cursor-pointer text-gray-300 text-xs leading-none p-0 hover:border-red-500 hover:text-red-500"
                @click="removeIncludeGroup(gi)"
              >
                <span class="not-italic">×</span>
              </button>
            </div>
            <div class="px-4 py-3 flex flex-col">
              <template v-for="(cond, ci) in group.conditions" :key="ci">
                <div v-if="ci > 0" class="text-center text-[10px] font-semibold text-gray-300 tracking-wider py-1">
                  AND
                </div>
                <div class="flex items-center gap-2">
                  <NSelect
                    :value="cond.field || null"
                    :options="fieldOptions"
                    placeholder="请选择"
                    size="small"
                    class="flex-1"
                    :disabled="readonly"
                    @update:value="(v: string) => updateInclude(gi, ci, { field: v })"
                  />
                  <NSelect
                    :value="cond.operator || null"
                    :options="getOperatorOptions(cond.field)"
                    placeholder="请选择"
                    size="small"
                    class="flex-1"
                    :disabled="readonly || !cond.field"
                    @update:value="(v: string) => updateInclude(gi, ci, { operator: v })"
                  />
                  <template v-if="getFieldDef(cond.field)?.value_type === 'enum'">
                    <NSelect
                      :value="cond.value || null"
                      :options="(getFieldDef(cond.field)?.enum_values ?? []).map((v) => ({ label: v, value: v }))"
                      placeholder="请选择"
                      size="small"
                      class="flex-1"
                      :disabled="readonly || !cond.operator"
                      @update:value="(v: string) => updateInclude(gi, ci, { value: v })"
                    />
                  </template>
                  <template v-else>
                    <NInput
                      :value="cond.value"
                      placeholder="请输入"
                      size="small"
                      class="flex-1"
                      :disabled="readonly || !cond.operator"
                      @update:value="(v: string) => updateInclude(gi, ci, { value: v })"
                    />
                  </template>
                  <button
                    v-if="!readonly"
                    class="flex items-center justify-center w-[22px] h-[22px] min-w-[22px] rounded-full border border-gray-200 bg-transparent cursor-pointer text-gray-300 text-xs leading-none p-0 hover:border-red-500 hover:text-red-500"
                    @click="removeIncludeCondition(gi, ci)"
                  >
                    <span class="not-italic">×</span>
                  </button>
                </div>
              </template>
              <span
                v-if="!readonly"
                class="text-blue-600 text-[13px] cursor-pointer mt-2 hover:underline"
                @click="addIncludeCondition(gi)"
                >添加条件</span
              >
            </div>
          </div>
        </div>
      </template>

      <span v-if="!readonly" class="text-blue-600 text-[13px] cursor-pointer hover:underline" @click="addIncludeGroup"
        >+ 添加{{ includeGroups.length > 0 ? '条件组（OR）' : '包含条件组' }}</span
      >

      <!-- 排除条件组 -->
      <template v-for="(group, gi) in excludeGroups" :key="'exc-' + gi">
        <div v-if="gi > 0" class="text-center text-[10px] font-semibold text-gray-300 tracking-wider">OR</div>
        <div class="flex rounded-lg border border-red-100 bg-white overflow-hidden">
          <div class="w-1 shrink-0 rounded-l bg-red-500"></div>
          <div class="flex-1 min-w-0">
            <div class="flex items-center justify-between px-4 py-3 border-b border-gray-100">
              <span class="text-sm font-semibold text-gray-900">排除条件</span>
              <button
                v-if="!readonly"
                class="flex items-center justify-center w-[22px] h-[22px] min-w-[22px] rounded-full border border-gray-200 bg-transparent cursor-pointer text-gray-300 text-xs leading-none p-0 hover:border-red-500 hover:text-red-500"
                @click="removeExcludeGroup(gi)"
              >
                <span class="not-italic">×</span>
              </button>
            </div>
            <div class="px-4 py-3 flex flex-col">
              <template v-for="(cond, ci) in group.conditions" :key="ci">
                <div v-if="ci > 0" class="text-center text-[10px] font-semibold text-gray-300 tracking-wider py-1">
                  AND
                </div>
                <div class="flex items-center gap-2">
                  <NSelect
                    :value="cond.field || null"
                    :options="fieldOptions"
                    placeholder="请选择"
                    size="small"
                    class="flex-1"
                    :disabled="readonly"
                    @update:value="(v: string) => updateExclude(gi, ci, { field: v })"
                  />
                  <NSelect
                    :value="cond.operator || null"
                    :options="getOperatorOptions(cond.field)"
                    placeholder="请选择"
                    size="small"
                    class="flex-1"
                    :disabled="readonly || !cond.field"
                    @update:value="(v: string) => updateExclude(gi, ci, { operator: v })"
                  />
                  <template v-if="getFieldDef(cond.field)?.value_type === 'enum'">
                    <NSelect
                      :value="cond.value || null"
                      :options="(getFieldDef(cond.field)?.enum_values ?? []).map((v) => ({ label: v, value: v }))"
                      placeholder="请选择"
                      size="small"
                      class="flex-1"
                      :disabled="readonly || !cond.operator"
                      @update:value="(v: string) => updateExclude(gi, ci, { value: v })"
                    />
                  </template>
                  <template v-else>
                    <NInput
                      :value="cond.value"
                      placeholder="请输入"
                      size="small"
                      class="flex-1"
                      :disabled="readonly || !cond.operator"
                      @update:value="(v: string) => updateExclude(gi, ci, { value: v })"
                    />
                  </template>
                  <button
                    v-if="!readonly"
                    class="flex items-center justify-center w-[22px] h-[22px] min-w-[22px] rounded-full border border-gray-200 bg-transparent cursor-pointer text-gray-300 text-xs leading-none p-0 hover:border-red-500 hover:text-red-500"
                    @click="removeExcludeCondition(gi, ci)"
                  >
                    <span class="not-italic">×</span>
                  </button>
                </div>
              </template>
              <span
                v-if="!readonly"
                class="text-blue-600 text-[13px] cursor-pointer mt-2 hover:underline"
                @click="addExcludeCondition(gi)"
                >添加条件</span
              >
            </div>
          </div>
        </div>
      </template>

      <span v-if="!readonly" class="text-blue-600 text-[13px] cursor-pointer hover:underline" @click="addExcludeGroup"
        >+ 添加{{ excludeGroups.length > 0 ? '条件组（OR）' : '排除条件组' }}</span
      >
    </div>
  </div>
</template>
