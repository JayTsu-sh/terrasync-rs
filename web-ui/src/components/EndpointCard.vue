<script setup lang="ts">
import { NIcon } from 'naive-ui'
import { DesktopOutline } from '@vicons/ionicons5'
import type { Endpoint } from '../api/endpoints'
import { getEndpointTypeInfo, getEndpointAddress } from '../shared/endpoint-display'

defineProps<{
  endpoint: Endpoint | null
  placeholder?: { label: string; description: string }
}>()
</script>

<template>
  <div v-if="endpoint" class="flex-1 flex items-center gap-3 bg-gray-50 border border-gray-100 rounded-lg px-5 py-4">
    <NIcon :size="20" :style="{ color: getEndpointTypeInfo(endpoint.endpoint_type).color }">
      <component :is="getEndpointTypeInfo(endpoint.endpoint_type).icon" />
    </NIcon>
    <div class="flex flex-col gap-1">
      <span class="text-sm font-semibold text-gray-900">{{ endpoint.name }}</span>
      <div class="flex items-center gap-1.5">
        <span
          class="px-1.5 py-px rounded text-[10px] font-medium"
          :style="{
            background: getEndpointTypeInfo(endpoint.endpoint_type).color + '15',
            color: getEndpointTypeInfo(endpoint.endpoint_type).color,
          }"
        >
          {{ getEndpointTypeInfo(endpoint.endpoint_type).label }}
        </span>
        <span class="text-xs text-gray-500 font-mono">{{ getEndpointAddress(endpoint) }}</span>
      </div>
    </div>
  </div>
  <div
    v-else-if="placeholder"
    class="flex-1 flex items-center gap-3 bg-gray-50 border border-gray-100 rounded-lg px-5 py-4 opacity-70"
  >
    <NIcon :size="20" class="text-gray-300">
      <DesktopOutline />
    </NIcon>
    <div class="flex flex-col gap-1">
      <span class="text-sm font-semibold text-gray-400">{{ placeholder.label }}</span>
      <span class="text-xs text-gray-400 font-mono">{{ placeholder.description }}</span>
    </div>
  </div>
</template>
