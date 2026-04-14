<script setup lang="ts">
import { NButton, NDataTable, NInput, NIcon, useDialog, useMessage } from 'naive-ui'
import { computed, onMounted, ref } from 'vue'
import { SearchOutline } from '@vicons/ionicons5'
import { useRouter } from 'vue-router'
import type { Endpoint } from '../api/endpoints'
import PageHeader from '../components/PageHeader.vue'
import CreateEndpointModal from '../components/CreateEndpointModal.vue'
import { useEndpointList } from '../composables/useEndpointList'
import { useEndpointColumns } from '../composables/useEndpointColumns'

const router = useRouter()
const dialog = useDialog()
const message = useMessage()
const showCreateModal = ref(false)

const { filteredEndpoints, loading, connectivityStatus, searchQuery, fetchEndpoints, removeEndpoint } =
  useEndpointList()

const pagination = computed(() =>
  filteredEndpoints.value.length > 10 ? { pageSize: 10, showSizePicker: true, pageSizes: [10, 20, 50] } : false,
)

const { columns } = useEndpointColumns(connectivityStatus, {
  onDetail: (id) => router.push(`/endpoints/${id}`),
  onDelete: (ep) => confirmDelete(ep),
})

async function loadEndpoints() {
  try {
    await fetchEndpoints()
  } catch (e: any) {
    message.error(e.message || '获取迁移目标列表失败')
  }
}

function confirmDelete(row: Endpoint) {
  dialog.warning({
    title: '确认删除',
    content: `确定要删除迁移目标「${row.name}」吗？关联的迁移路径也会被一起删除。`,
    positiveText: '删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await removeEndpoint(row.id)
        message.success('删除成功')
      } catch (e: any) {
        message.error(e.message)
      }
    },
  })
}

onMounted(loadEndpoints)
</script>

<template>
  <div>
    <PageHeader title="目标管理">
      <template #right>
        <div class="flex items-center gap-3">
          <NInput v-model:value="searchQuery" placeholder="搜索目标..." clearable class="w-60">
            <template #prefix>
              <NIcon :component="SearchOutline" class="text-gray-400" />
            </template>
          </NInput>
          <NButton type="primary" @click="showCreateModal = true">新建目标</NButton>
        </div>
      </template>
    </PageHeader>

    <div class="bg-white border border-gray-200 rounded-lg overflow-hidden shadow-sm">
      <NDataTable
        :columns="columns"
        :data="filteredEndpoints"
        :loading="loading"
        :bordered="false"
        :pagination="pagination"
        :striped="true"
      />
    </div>

    <CreateEndpointModal v-model:show="showCreateModal" @created="loadEndpoints" />
  </div>
</template>
