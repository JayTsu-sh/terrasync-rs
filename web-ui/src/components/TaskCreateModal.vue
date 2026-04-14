<script setup lang="ts">
import { watch } from 'vue'
import {
  NModal,
  NButton,
  NInput,
  NSelect,
  NInputNumber,
  NCheckbox,
  NIcon,
  NTooltip,
  NForm,
  NFormItem,
  useMessage,
} from 'naive-ui'
import { HelpCircleOutline } from '@vicons/ionicons5'
import ConditionBuilder from './ConditionBuilder.vue'
import { useTaskCreate, ValidationError } from '../composables/useTaskCreate'

const props = defineProps<{
  show: boolean
}>()

const emit = defineEmits<{
  'update:show': [value: boolean]
  created: []
}>()

const message = useMessage()

const {
  form,
  includeGroups,
  excludeGroups,
  filterFields,
  submitting,
  taskTypeOptions,
  needDest,
  showAdvanced,
  showFilter,
  endpointOptions,
  sourceLabel,
  sourcePlaceholder,
  fetchData,
  handleSubmit,
  resetForm,
} = useTaskCreate()

watch(
  () => props.show,
  async (visible) => {
    if (visible) {
      resetForm()
      try {
        await fetchData()
      } catch (e: any) {
        message.error(e.message || '加载数据失败')
      }
    }
  },
)

async function onSubmit() {
  try {
    await handleSubmit()
    message.success('任务创建成功')
    emit('created')
    emit('update:show', false)
  } catch (e: any) {
    if (e instanceof ValidationError) {
      message.warning(e.message)
    } else {
      message.error(e.message || '创建失败')
    }
  }
}
</script>

<template>
  <NModal :show="show" :mask-closable="false" @update:show="emit('update:show', $event)">
    <div class="bg-white rounded-xl shadow-2xl w-[560px]">
      <!-- Header -->
      <div class="flex items-center justify-between px-6 py-5 border-b border-gray-100">
        <span class="text-lg font-semibold text-gray-900">创建任务</span>
        <span class="cursor-pointer text-gray-400 hover:text-gray-600 text-xl" @click="emit('update:show', false)"
          >&times;</span
        >
      </div>

      <!-- Body -->
      <div class="px-6 py-5">
        <NForm :label-width="120" label-placement="left">
          <NFormItem label="任务名称" required>
            <NInput v-model:value="form.name" placeholder="" />
          </NFormItem>

          <NFormItem label="任务类型" required>
            <NSelect v-model:value="form.task_type" :options="taskTypeOptions" />
          </NFormItem>

          <NFormItem :label="sourceLabel" required>
            <NSelect
              v-model:value="form.source_endpoint_id"
              :options="endpointOptions"
              :placeholder="sourcePlaceholder"
              clearable
            />
          </NFormItem>

          <NFormItem v-if="needDest" label="目标迁移目标" required>
            <NSelect
              v-model:value="form.dest_endpoint_id"
              :options="endpointOptions"
              placeholder="选择目标迁移目标"
              clearable
            />
          </NFormItem>

          <!-- sync-only: 高级选项 -->
          <NFormItem v-if="showAdvanced">
            <template #label>
              <!-- ⚠️ 非默认: text-gray-300 cursor-help 帮助提示图标 -->
              <span class="inline-flex items-center gap-1">
                带宽限速
                <NTooltip trigger="hover">
                  <template #trigger>
                    <NIcon :size="14" class="text-gray-300 cursor-help"><HelpCircleOutline /></NIcon>
                  </template>
                  限制数据传输带宽，如 100MiB/s
                </NTooltip>
              </span>
            </template>
            <NInput v-model:value="form.qos" placeholder="不限制" />
          </NFormItem>

          <NFormItem v-if="showAdvanced">
            <template #label>
              <span class="inline-flex items-center gap-1">
                IOPS 限速
                <NTooltip trigger="hover">
                  <template #trigger>
                    <NIcon :size="14" class="text-gray-300 cursor-help"><HelpCircleOutline /></NIcon>
                  </template>
                  限制每秒 IO 操作次数
                </NTooltip>
              </span>
            </template>
            <!-- ⚠️ 非默认: w-full NInputNumber 默认不撑满 -->
            <NInputNumber v-model:value="form.iops" :min="1" placeholder="不限制" :show-button="false" class="w-full" />
          </NFormItem>

          <NFormItem v-if="showAdvanced">
            <template #label>
              <span class="inline-flex items-center gap-1">
                Block Size
                <NTooltip trigger="hover">
                  <template #trigger>
                    <NIcon :size="14" class="text-gray-300 cursor-help"><HelpCircleOutline /></NIcon>
                  </template>
                  数据分块大小，影响传输效率
                </NTooltip>
              </span>
            </template>
            <NInput v-model:value="form.block_size" placeholder="自适应" />
          </NFormItem>

          <NFormItem v-if="showAdvanced" label="完整性检查">
            <NCheckbox v-model:checked="form.enable_integrity_check" />
          </NFormItem>

          <NFormItem label="并发度">
            <NInputNumber v-model:value="form.concurrency" :min="1" :max="128" :show-button="false" class="w-full" />
          </NFormItem>
        </NForm>

        <!-- 筛选条件 -->
        <ConditionBuilder
          v-if="showFilter"
          :fields="filterFields"
          v-model:include-groups="includeGroups"
          v-model:exclude-groups="excludeGroups"
        />
      </div>

      <!-- Footer -->
      <div class="flex justify-end gap-3 px-6 py-4 border-t border-gray-100">
        <NButton @click="emit('update:show', false)">取消</NButton>
        <NButton type="primary" :loading="submitting" @click="onSubmit">创建</NButton>
      </div>
    </div>
  </NModal>
</template>
