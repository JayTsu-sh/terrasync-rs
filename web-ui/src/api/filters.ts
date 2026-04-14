import { throwIfError } from './fetch-utils'

const BASE = '/api/v1'

export interface FilterOperator {
  value: string
  label: string
}

export interface FilterField {
  name: string
  label: string
  value_type: string
  operators: FilterOperator[]
  enum_values?: string[]
}

interface FilterFieldsResponse {
  fields: FilterField[]
}

export async function getFilterFields(): Promise<FilterField[]> {
  const res = await fetch(`${BASE}/filter-fields`)
  await throwIfError(res)
  const data: FilterFieldsResponse = await res.json()
  return data.fields
}
