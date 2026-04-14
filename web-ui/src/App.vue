<script setup lang="ts">
import {
  NConfigProvider,
  NMessageProvider,
  NDialogProvider,
  NLayout,
  NLayoutSider,
  NMenu,
  NLayoutContent,
} from 'naive-ui'
import { useRouter, useRoute } from 'vue-router'
import { h, computed } from 'vue'
import type { MenuOption, GlobalThemeOverrides } from 'naive-ui'
import { ServerOutline, ListOutline, SettingsOutline } from '@vicons/ionicons5'
import { NIcon } from 'naive-ui'

const router = useRouter()
const route = useRoute()

// Tailwind v4 色板对齐 — themeOverrides 以 Tailwind 默认色阶为 single source of truth
const themeOverrides: GlobalThemeOverrides = {
  common: {
    primaryColor: '#3b82f6', // blue-500
    primaryColorHover: '#60a5fa', // blue-400
    primaryColorPressed: '#2563eb', // blue-600
    primaryColorSuppl: '#3b82f6', // blue-500
    successColor: '#22c55e', // green-500
    successColorHover: '#4ade80', // green-400
    successColorPressed: '#16a34a', // green-600
    warningColor: '#f59e0b', // amber-500
    warningColorHover: '#fbbf24', // amber-400
    warningColorPressed: '#d97706', // amber-600
    errorColor: '#ef4444', // red-500
    errorColorHover: '#f87171', // red-400
    errorColorPressed: '#dc2626', // red-600
    textColor1: '#111827', // gray-900
    textColor2: '#6b7280', // gray-500
    textColor3: '#9ca3af', // gray-400
    borderColor: '#d1d5db', // gray-300
    dividerColor: '#f3f4f6', // gray-100
    borderRadius: '6px', // rounded-md
    borderRadiusSmall: '4px', // rounded
    fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
  },
  Button: {
    borderRadiusMedium: '6px',
    borderRadiusSmall: '6px',
    paddingMedium: '0 16px',
    heightMedium: '36px', // h-9
  },
  Tag: {
    borderRadius: '4px',
  },
  Input: {
    borderRadius: '6px',
  },
  DataTable: {
    borderRadius: '8px',
    thColor: '#f9fafb', // gray-50
    thTextColor: '#111827', // gray-900
    thFontWeight: '600',
    tdFontSize: '14px', // text-sm
    borderColor: '#f3f4f6', // gray-100
    thColorHover: '#f3f4f6', // gray-100
    tdColorHover: '#f9fafb', // gray-50
  },
  Menu: {
    itemTextColor: '#6b7280', // gray-500
    itemIconColor: '#6b7280', // gray-500
    itemTextColorActive: '#3b82f6', // blue-500
    itemIconColorActive: '#3b82f6', // blue-500
    itemColorActive: '#eff6ff', // blue-50
    itemTextColorActiveHover: '#3b82f6', // blue-500
    itemIconColorActiveHover: '#3b82f6', // blue-500
    itemColorActiveHover: '#eff6ff', // blue-50
    itemTextColorHover: '#3b82f6', // blue-500
    itemIconColorHover: '#3b82f6', // blue-500
    itemColorHover: '#f9fafb', // gray-50
    itemHeight: '42px',
    fontSize: '14px',
  },
}

function renderIcon(icon: any) {
  return () => h(NIcon, null, { default: () => h(icon) })
}

const menuOptions: MenuOption[] = [
  {
    label: '目标管理',
    key: '/endpoints',
    icon: renderIcon(ServerOutline),
  },
  {
    label: '任务管理',
    key: '/tasks',
    icon: renderIcon(ListOutline),
  },
  {
    label: '系统设置',
    key: '/settings',
    icon: renderIcon(SettingsOutline),
  },
]

const activeKey = computed(() => {
  const path = route.path
  if (path.startsWith('/endpoints')) return '/endpoints'
  if (path.startsWith('/tasks')) return '/tasks'
  if (path.startsWith('/settings')) return '/settings'
  return path
})

function handleMenuUpdate(key: string) {
  router.push(key)
}
</script>

<template>
  <NConfigProvider :theme-overrides="themeOverrides">
    <NMessageProvider>
      <NDialogProvider>
        <NLayout has-sider class="h-screen">
          <NLayoutSider :width="220" content-class="sidebar-content" class="bg-white border-r border-gray-100">
            <div class="p-4 text-base font-semibold text-gray-900 border-b border-gray-100 font-sans">
              数据迁移管理系统
            </div>
            <NMenu :value="activeKey" :options="menuOptions" @update:value="handleMenuUpdate" />
          </NLayoutSider>
          <NLayoutContent content-class="main-content">
            <router-view />
          </NLayoutContent>
        </NLayout>
      </NDialogProvider>
    </NMessageProvider>
  </NConfigProvider>
</template>

<style scoped>
:deep(.n-layout-sider-scroll-container) {
  background: #ffffff;
}

:deep(.sidebar-content) {
  display: flex;
  flex-direction: column;
  background: #ffffff;
}

:deep(.main-content) {
  padding: 24px;
  overflow: auto;
  background: #f9fafb; /* gray-50 */
}

:deep(.n-data-table-th) {
  position: relative;
}

:deep(.n-data-table-th .n-data-table-th__title-wrapper) {
  justify-content: flex-start;
  gap: 4px;
}

:deep(.n-data-table-th:not(:last-child)::after) {
  content: '';
  position: absolute;
  right: 0;
  top: 25%;
  height: 50%;
  width: 1px;
  background: #e5e7eb; /* gray-200 */
}
</style>
