import { ref, computed } from 'vue'
import type { Endpoint } from '../api/endpoints'
import { listEndpoints, testConnection, deleteEndpoint } from '../api/endpoints'
import { usePolling } from './usePolling'
import { getEndpointAddress } from '../shared/endpoint-display'

export function useEndpointList() {
  const endpoints = ref<Endpoint[]>([])
  const loading = ref(false)
  const connectivityStatus = ref<Record<string, 'testing' | 'ok' | 'fail'>>({})
  const searchQuery = ref('')

  const filteredEndpoints = computed(() => {
    const q = searchQuery.value.trim().toLowerCase()
    if (!q) return endpoints.value
    return endpoints.value.filter((ep) => {
      const addr = getEndpointAddress(ep, { includePrefix: true })
      return ep.name.toLowerCase().includes(q) || addr.toLowerCase().includes(q)
    })
  })

  async function checkConnectivity(eps: Endpoint[]) {
    for (const ep of eps) {
      connectivityStatus.value[ep.id] = 'testing'
    }
    await Promise.all(
      eps.map(async (ep) => {
        try {
          await testConnection(ep.id)
          connectivityStatus.value[ep.id] = 'ok'
        } catch {
          connectivityStatus.value[ep.id] = 'fail'
        }
      }),
    )
  }

  const polling = usePolling(() => {
    if (endpoints.value.length > 0) {
      checkConnectivity(endpoints.value)
    }
  }, 30_000)

  async function fetchEndpoints() {
    loading.value = true
    try {
      endpoints.value = await listEndpoints()
      checkConnectivity(endpoints.value)
      polling.start()
    } finally {
      loading.value = false
    }
  }

  async function removeEndpoint(id: string) {
    await deleteEndpoint(id)
    await fetchEndpoints()
  }

  return { endpoints, loading, connectivityStatus, searchQuery, filteredEndpoints, fetchEndpoints, removeEndpoint }
}
