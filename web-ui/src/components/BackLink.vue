<script setup lang="ts">
import { useRouter } from 'vue-router'

const props = withDefaults(defineProps<{ to: string; label?: string }>(), {
  label: '返回列表',
})

const router = useRouter()
</script>

<template>
  <div class="mb-5">
    <div
      class="inline-flex items-center gap-1 cursor-pointer text-sm text-gray-500 transition-colors hover:text-blue-600"
      @click="router.push(props.to)"
    >
      <span>←</span>
      <span>{{ props.label }}</span>
    </div>
  </div>
</template>
