<script setup lang="ts">
import {
  NAutoComplete,
  NButton,
  NCheckbox,
  NForm,
  NFormItem,
  NInput,
  NInputNumber,
  NModal,
  NSelect,
  NSpin,
  NText,
  useMessage,
} from 'naive-ui'
import { useEndpointForm } from '../composables/useEndpointForm'

const props = defineProps<{ show: boolean }>()
const emit = defineEmits<{
  'update:show': [value: boolean]
  created: []
}>()

const message = useMessage()

const {
  formData,
  nfsExports,
  loadingExports,
  exportError,
  s3Buckets,
  loadingBuckets,
  bucketError,
  localPathOptions,
  handlePathInput,
  nfsPrefixOptions,
  handlePrefixInput,
  s3PrefixOptions,
  handleS3PrefixInput,
  typeOptions,
  handleCreate,
  handleModalClose,
} = useEndpointForm()

function onClose() {
  handleModalClose()
  emit('update:show', false)
}

async function onCreate() {
  try {
    const result = await handleCreate()
    if (result.scanStarted) {
      message.success('创建成功，扫描任务已启动')
    } else if (result.scanError) {
      message.warning(`目标已创建，但启动扫描失败：${result.scanError}`)
    } else {
      message.success('创建成功')
    }
    emit('update:show', false)
    emit('created')
  } catch (e: any) {
    message.error(e.message || '创建失败')
  }
}
</script>

<template>
  <NModal :show="show" :mask-closable="false" @update:show="emit('update:show', $event)">
    <div class="bg-white rounded-xl shadow-2xl w-[520px]">
      <!-- Header -->
      <div class="flex items-center justify-between px-6 py-5 border-b border-gray-100">
        <span class="text-lg font-semibold text-gray-900">
          {{ formData.type === 'nfs' ? '新建NFS目标' : formData.type === 's3' ? '新建S3目标' : '新建本地目标' }}
        </span>
        <span class="cursor-pointer text-gray-400 hover:text-gray-600 text-xl" @click="onClose">&times;</span>
      </div>

      <!-- Body -->
      <div class="px-6 py-5 space-y-5">
        <!-- 基础信息 -->
        <div class="space-y-4">
          <div class="text-xs text-gray-400 font-semibold tracking-wider">基础信息</div>
          <NForm label-placement="top" :show-feedback="false" class="space-y-4">
            <NFormItem required label="名称">
              <NInput v-model:value="formData.name" placeholder="" />
            </NFormItem>
            <NFormItem label="类型">
              <NSelect v-model:value="formData.type" :options="typeOptions" />
            </NFormItem>
          </NForm>
        </div>

        <!-- 类型特定配置 -->
        <div class="space-y-4">
          <div class="text-xs text-gray-400 font-semibold tracking-wider">
            {{ formData.type === 'nfs' ? 'NFS 连接配置' : formData.type === 's3' ? 'S3 连接配置' : '路径配置' }}
          </div>
          <NForm label-placement="top" :show-feedback="false" class="space-y-4">
            <!-- Local -->
            <template v-if="formData.type === 'local'">
              <NFormItem required label="路径">
                <NAutoComplete
                  v-model:value="formData.path"
                  :options="localPathOptions"
                  placeholder=""
                  :on-update:value="handlePathInput"
                  clearable
                />
              </NFormItem>
            </template>

            <!-- NFS -->
            <template v-if="formData.type === 'nfs'">
              <div class="flex gap-3">
                <NFormItem required class="flex-1" label="服务器 IP">
                  <NInput v-model:value="formData.server" placeholder="" />
                </NFormItem>
                <NFormItem required class="w-[120px]" label="端口">
                  <NInputNumber v-model:value="formData.port" :min="1" :max="65535" :show-button="false" />
                </NFormItem>
              </div>
              <NFormItem required label="共享路径">
                <NSpin :show="loadingExports" size="small" class="w-full">
                  <NSelect
                    v-model:value="formData.export_path"
                    :options="nfsExports.map((e) => ({ label: e.path, value: e.path }))"
                    :loading="loadingExports"
                    :disabled="!nfsExports.length && !loadingExports"
                    placeholder="输入 Host 后自动查询"
                    filterable
                  />
                </NSpin>
                <NText v-if="exportError" type="error" class="text-xs mt-1">{{ exportError }}</NText>
              </NFormItem>
              <NFormItem label="前缀路径">
                <NAutoComplete
                  v-model:value="formData.prefix"
                  :options="nfsPrefixOptions"
                  placeholder="可选，输入 / 后自动补全"
                  :on-update:value="handlePrefixInput"
                  clearable
                />
              </NFormItem>
            </template>

            <!-- S3 -->
            <template v-if="formData.type === 's3'">
              <div class="flex gap-3">
                <NFormItem required class="flex-1" label="服务器 IP">
                  <NInput v-model:value="formData.host" placeholder="" />
                </NFormItem>
                <NFormItem required class="w-[120px]" label="端口">
                  <NInputNumber v-model:value="formData.s3_port" :min="1" :max="65535" :show-button="false" />
                </NFormItem>
              </div>
              <div class="flex gap-3">
                <NFormItem required class="flex-1" label="Access Key">
                  <NInput v-model:value="formData.access_key" type="password" show-password-on="click" placeholder="" />
                </NFormItem>
                <NFormItem required class="flex-1" label="Secret Key">
                  <NInput v-model:value="formData.secret_key" type="password" show-password-on="click" placeholder="" />
                </NFormItem>
              </div>
              <NFormItem required label="存储桶">
                <NSpin :show="loadingBuckets" size="small" class="w-full">
                  <NSelect
                    v-model:value="formData.bucket"
                    :options="s3Buckets.map((b) => ({ label: b.name, value: b.name }))"
                    :loading="loadingBuckets"
                    :disabled="!s3Buckets.length && !loadingBuckets"
                    placeholder="输入连接信息后自动查询"
                    filterable
                  />
                </NSpin>
                <NText v-if="bucketError" type="error" class="text-xs mt-1">{{ bucketError }}</NText>
              </NFormItem>
              <NFormItem label="对象前缀路径">
                <NAutoComplete
                  v-model:value="formData.prefix"
                  :options="s3PrefixOptions"
                  placeholder="可选，输入 / 后自动补全"
                  :on-update:value="handleS3PrefixInput"
                  clearable
                />
              </NFormItem>
            </template>
          </NForm>
        </div>

        <!-- 选项 -->
        <div class="space-y-3">
          <div class="text-xs text-gray-400 font-semibold tracking-wider">选项</div>
          <NCheckbox v-if="formData.type === 's3'" v-model:checked="formData.use_tls">启用 TLS (HTTPS)</NCheckbox>
          <NCheckbox v-model:checked="formData.start_scan">创建后自动开始扫描</NCheckbox>
        </div>
      </div>

      <!-- Footer -->
      <div class="flex justify-end gap-3 px-6 py-4 border-t border-gray-100">
        <NButton @click="onClose">取消</NButton>
        <NButton type="primary" @click="onCreate">创建</NButton>
      </div>
    </div>
  </NModal>
</template>
