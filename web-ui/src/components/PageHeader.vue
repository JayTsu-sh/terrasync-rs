<script setup lang="ts">
defineProps<{ title?: string }>()
</script>

<template>
  <div class="flex justify-between items-center mb-4">
    <slot name="left">
      <h2 class="text-xl font-semibold text-gray-900 m-0">{{ title }}</h2>
    </slot>
    <div class="flex items-center gap-2">
      <slot name="right" />
    </div>
  </div>
</template>
