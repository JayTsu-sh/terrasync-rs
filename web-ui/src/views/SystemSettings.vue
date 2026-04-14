<script setup lang="ts">
import { NButton, NForm, NFormItem, NInput, NInputNumber, NSelect, NTag, useMessage } from 'naive-ui'
import { onMounted, ref } from 'vue'
import { getConfig, testClickhouse, updateConfig } from '../api/config'
import PageHeader from '../components/PageHeader.vue'

const message = useMessage()
const loading = ref(false)
const saving = ref(false)
const testing = ref(false)
const activeTab = ref<'db' | 'log'>('db')

const dsn = ref('http://localhost:8123')
const clickhouseDatabase = ref('default')
const clickhouseUser = ref('default')
const clickhousePassword = ref('')
const clickhouseBatchSize = ref(100000)
const connStatus = ref<'unknown' | 'ok' | 'fail'>('unknown')

const logLevel = ref('info')
const logLevelOptions = [
  { label: 'Info', value: 'info' },
  { label: 'Debug', value: 'debug' },
  { label: 'Trace', value: 'trace' },
]

function parseDsn(d: string): { host: string; port: number } {
  try {
    const url = new URL(d)
    return { host: url.hostname, port: parseInt(url.port) || 8123 }
  } catch {
    return { host: d, port: 8123 }
  }
}

function buildDsn(host: string, port: number): string {
  return `http://${host}:${port}`
}

async function fetchSettings() {
  loading.value = true
  try {
    const config = await getConfig()
    const configMap = new Map<string, any>()
    for (const item of config.items) configMap.set(item.key, item.value)

    const host = (configMap.get('clickhouse_host') as string) || 'localhost'
    const port = (configMap.get('clickhouse_port') as number) || 8123
    dsn.value = buildDsn(host, port)
    clickhouseUser.value = (configMap.get('clickhouse_user') as string) || 'default'
    clickhousePassword.value = (configMap.get('clickhouse_password') as string) || ''
    clickhouseDatabase.value = (configMap.get('clickhouse_database') as string) || 'default'
    clickhouseBatchSize.value = (configMap.get('clickhouse_batch_size') as number) || 100000
    logLevel.value = (configMap.get('log_level') as string) || 'info'
  } catch (e: any) {
    message.error(e.message || '获取设置失败')
  } finally {
    loading.value = false
  }
}

async function handleSave() {
  saving.value = true
  try {
    const { host, port } = parseDsn(dsn.value)
    await updateConfig([
      { key: 'clickhouse_host', value: host },
      { key: 'clickhouse_port', value: port },
      { key: 'clickhouse_user', value: clickhouseUser.value },
      { key: 'clickhouse_password', value: clickhousePassword.value },
      { key: 'clickhouse_database', value: clickhouseDatabase.value },
      { key: 'clickhouse_batch_size', value: clickhouseBatchSize.value },
      { key: 'log_level', value: logLevel.value },
    ])
    message.success('保存成功')
  } catch (e: any) {
    message.error(e.message || '保存失败')
  } finally {
    saving.value = false
  }
}

async function handleTest() {
  testing.value = true
  connStatus.value = 'unknown'
  try {
    const result = await testClickhouse({
      dsn: dsn.value,
      username: clickhouseUser.value,
      password: clickhousePassword.value,
      database: clickhouseDatabase.value,
    })
    connStatus.value = result.success ? 'ok' : 'fail'
    result.success ? message.success('连接成功') : message.error(result.message || '连接失败')
  } catch (e: any) {
    connStatus.value = 'fail'
    message.error(e.message || '连接失败')
  } finally {
    testing.value = false
  }
}

async function silentTest() {
  connStatus.value = 'unknown'
  try {
    const result = await testClickhouse({
      dsn: dsn.value,
      username: clickhouseUser.value,
      password: clickhousePassword.value,
      database: clickhouseDatabase.value,
    })
    connStatus.value = result.success ? 'ok' : 'fail'
  } catch {
    connStatus.value = 'fail'
  }
}

onMounted(async () => {
  await fetchSettings()
  silentTest()
})
</script>

<template>
  <div>
    <PageHeader title="系统设置" />

    <div class="bg-white border border-gray-200 rounded-lg shadow-sm overflow-hidden max-w-2xl">
      <div class="flex border-b border-gray-200">
        <button
          class="px-5 py-3 text-sm font-medium transition-colors border-b-2 -mb-px"
          :class="
            activeTab === 'db'
              ? 'text-blue-600 border-blue-500 font-semibold'
              : 'text-gray-500 border-transparent hover:text-blue-600'
          "
          @click="activeTab = 'db'"
        >
          数据库设置
        </button>
        <button
          class="px-5 py-3 text-sm font-medium transition-colors border-b-2 -mb-px"
          :class="
            activeTab === 'log'
              ? 'text-blue-600 border-blue-500 font-semibold'
              : 'text-gray-500 border-transparent hover:text-blue-600'
          "
          @click="activeTab = 'log'"
        >
          日志设置
        </button>
      </div>

      <div v-if="activeTab === 'db'" class="px-6 py-5">
        <NForm label-placement="left" label-width="100">
          <NFormItem label="连接状态">
            <NTag v-if="connStatus === 'ok'" type="success" size="small">连接成功</NTag>
            <NTag v-else-if="connStatus === 'fail'" type="error" size="small">连接失败</NTag>
            <NTag v-else type="default" size="small">未检测</NTag>
          </NFormItem>
          <NFormItem label="DSN"><NInput v-model:value="dsn" placeholder="http://localhost:8123" /></NFormItem>
          <NFormItem label="数据库名"><NInput v-model:value="clickhouseDatabase" placeholder="default" /></NFormItem>
          <NFormItem label="用户名"><NInput v-model:value="clickhouseUser" placeholder="default" /></NFormItem>
          <NFormItem label="密码"
            ><NInput
              v-model:value="clickhousePassword"
              type="password"
              show-password-on="click"
              placeholder="请输入密码"
          /></NFormItem>
          <NFormItem label="批量大小">
            <NInputNumber
              v-model:value="clickhouseBatchSize"
              :min="1"
              :max="1000000"
              :show-button="false"
              class="w-[200px]"
              placeholder="100000"
            />
          </NFormItem>
          <NFormItem :show-label="false">
            <div class="flex gap-3 justify-end">
              <NButton :loading="testing" @click="handleTest">测试连接</NButton>
              <NButton type="primary" :loading="saving" @click="handleSave">保存</NButton>
            </div>
          </NFormItem>
        </NForm>
      </div>

      <div v-if="activeTab === 'log'" class="px-6 py-5">
        <NForm label-placement="left" label-width="100">
          <NFormItem label="日志级别">
            <NSelect v-model:value="logLevel" :options="logLevelOptions" class="w-[200px]" />
          </NFormItem>
          <NFormItem :show-label="false">
            <div class="flex gap-3 justify-end">
              <NButton type="primary" :loading="saving" @click="handleSave">保存</NButton>
            </div>
          </NFormItem>
        </NForm>
      </div>
    </div>
  </div>
</template>
