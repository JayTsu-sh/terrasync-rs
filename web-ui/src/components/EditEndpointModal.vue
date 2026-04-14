<script setup lang="ts">
import { NAutoComplete, NButton, NCheckbox, NForm, NFormItem, NInput, NModal, useDialog, useMessage } from 'naive-ui'
import { watch } from 'vue'
import type { Endpoint } from '../api/endpoints'
import { useEndpointEdit, ValidationError } from '../composables/useEndpointEdit'

const props = defineProps<{ show: boolean; endpoint: Endpoint | null }>()
const emit = defineEmits<{
  'update:show': [value: boolean]
  saved: []
}>()

const message = useMessage()
const dialog = useDialog()

const {
  saving,
  isDirty,
  editName,
  editPath,
  editNfsExportPath,
  editNfsPrefix,
  editS3AccessKey,
  editS3SecretKey,
  editS3Bucket,
  editS3Prefix,
  editS3UseTls,
  isLocal,
  isNfs,
  isS3,
  localPathOptions,
  handleEditPathInput,
  nfsPrefixOptions,
  handleNfsPrefixInput,
  s3PrefixOptions,
  handleS3PrefixInput,
  populateFields,
  handleSave,
} = useEndpointEdit()

watch(
  () => props.show,
  (visible) => {
    if (visible && props.endpoint) {
      populateFields(props.endpoint)
    }
  },
)

function onClose() {
  emit('update:show', false)
}

async function onSave(force = false) {
  if (!props.endpoint) return
  try {
    const result = await handleSave(props.endpoint.id, force)
    if (result.needs_confirm) {
      dialog.warning({
        title: '确认修改',
        content: `修改将影响 ${result.conflicting_tasks.length} 个关联任务，确认继续？`,
        positiveText: '确认保存',
        negativeText: '取消',
        onPositiveClick: () => onSave(true),
      })
      return
    }
    message.success('保存成功')
    emit('update:show', false)
    emit('saved')
  } catch (e: any) {
    message.error(e instanceof ValidationError ? e.message : e.message || '保存失败')
  }
}
</script>

<template>
  <NModal :show="show" :mask-closable="false" @update:show="emit('update:show', $event)">
    <div class="bg-white rounded-xl shadow-2xl w-[560px]">
      <!-- Header -->
      <div class="flex items-center justify-between px-6 py-5 border-b border-gray-100">
        <span class="text-lg font-semibold text-gray-900">编辑目标</span>
        <span class="cursor-pointer text-gray-400 hover:text-gray-600 text-xl" @click="onClose">&times;</span>
      </div>

      <!-- Body -->
      <div class="px-6 py-5 space-y-5">
        <!-- Warning banner -->
        <div class="flex items-center gap-2 px-3.5 py-2.5 bg-blue-50 border border-blue-200 rounded-md">
          <span class="text-blue-500 text-sm">修改连接配置可能会影响关联的扫描/同步任务</span>
        </div>

        <!-- 基础信息 -->
        <div class="space-y-4">
          <div class="text-xs text-gray-400 font-semibold tracking-wider">基础信息</div>
          <NForm label-placement="top" :show-feedback="false" class="space-y-4">
            <NFormItem label="名称" required>
              <NInput v-model:value="editName" placeholder="" class="border-blue-500" />
            </NFormItem>
          </NForm>
        </div>

        <!-- 类型特定配置 -->
        <div class="space-y-4">
          <div class="text-xs text-gray-400 font-semibold tracking-wider">
            {{ isNfs ? 'NFS 连接配置' : isS3 ? 'S3 连接配置' : '路径配置' }}
          </div>
          <NForm label-placement="top" :show-feedback="false" class="space-y-4">
            <!-- Local -->
            <template v-if="isLocal">
              <NFormItem label="路径" required>
                <NAutoComplete
                  v-model:value="editPath"
                  :options="localPathOptions"
                  placeholder=""
                  :on-update:value="handleEditPathInput"
                  clearable
                />
              </NFormItem>
            </template>

            <!-- NFS -->
            <template v-if="isNfs">
              <div class="flex gap-3">
                <NFormItem label="服务器 IP" required class="flex-1">
                  <NInput :value="endpoint?.config.server" disabled />
                </NFormItem>
                <NFormItem label="端口" required class="w-[120px]">
                  <NInput :value="String(endpoint?.config.port)" disabled />
                </NFormItem>
              </div>
              <NFormItem label="共享路径" required>
                <NInput v-model:value="editNfsExportPath" placeholder="" />
              </NFormItem>
              <NFormItem label="前缀路径">
                <NAutoComplete
                  v-model:value="editNfsPrefix"
                  :options="nfsPrefixOptions"
                  placeholder="可选，输入 / 后自动补全"
                  :on-update:value="handleNfsPrefixInput"
                  clearable
                />
              </NFormItem>
            </template>

            <!-- S3 -->
            <template v-if="isS3">
              <div class="flex gap-3">
                <NFormItem label="服务器 IP" required class="flex-1">
                  <NInput :value="endpoint?.config.host" disabled />
                </NFormItem>
                <NFormItem label="端口" required class="w-[120px]">
                  <NInput :value="String(endpoint?.config.port)" disabled />
                </NFormItem>
              </div>
              <div class="flex gap-3">
                <NFormItem label="Access Key" required class="flex-1">
                  <NInput v-model:value="editS3AccessKey" type="password" show-password-on="click" placeholder="" />
                </NFormItem>
                <NFormItem label="Secret Key" required class="flex-1">
                  <NInput v-model:value="editS3SecretKey" type="password" show-password-on="click" placeholder="" />
                </NFormItem>
              </div>
              <div class="flex gap-3">
                <NFormItem label="存储桶" required class="flex-1">
                  <NInput v-model:value="editS3Bucket" placeholder="" />
                </NFormItem>
                <NFormItem label="对象前缀" class="flex-1">
                  <NAutoComplete
                    v-model:value="editS3Prefix"
                    :options="s3PrefixOptions"
                    placeholder="可选，输入 / 后自动补全"
                    :on-update:value="handleS3PrefixInput"
                    clearable
                  />
                </NFormItem>
              </div>
              <NCheckbox v-model:checked="editS3UseTls">启用 TLS (HTTPS)</NCheckbox>
            </template>
          </NForm>
        </div>
      </div>

      <!-- Footer -->
      <div class="flex justify-end gap-3 px-6 py-4 border-t border-gray-100">
        <NButton @click="onClose">取消</NButton>
        <NButton type="primary" :loading="saving" :disabled="!isDirty" @click="onSave()">保存</NButton>
      </div>
    </div>
  </NModal>
</template>
