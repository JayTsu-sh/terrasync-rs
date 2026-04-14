/**
 * 条件构建器数据模型与序列化
 *
 * 将结构化条件转为后端 filter 表达式字符串。
 * 后端语法: `<field> <op> <value>`，多条件用 `and` 连接。
 */

export interface Condition {
  field: string
  operator: string
  value: string
}

export interface ConditionGroup {
  conditions: Condition[]
}

/** 需要引号包裹值的字段类型 */
const QUOTED_FIELDS = new Set(['name', 'path', 'extension', 'dir_date'])

/** 值不加引号的字段类型 */
const UNQUOTED_FIELDS = new Set(['type', 'size'])

/**
 * 序列化单个条件为后端表达式片段
 *
 * - name/path/extension/dir_date: 值加双引号 → `name == "*.txt"`
 * - size: 纯数字 → `size > 1024`
 * - modified: 相对时间(如 3d)不加引号，绝对日期加引号 → `modified < 3d` 或 `modified > "2025-01-15"`
 * - type: 不加引号 → `type == file`
 */
function serializeCondition(c: Condition): string {
  const { field, operator, value } = c

  let formattedValue: string
  if (QUOTED_FIELDS.has(field)) {
    formattedValue = `"${value}"`
  } else if (UNQUOTED_FIELDS.has(field)) {
    formattedValue = value
  } else if (field === 'modified') {
    // 相对时间: 纯数字(< 8位) 或带 d 后缀，如 3d, 30, 0.5 → 不加引号
    // 绝对日期: 含 '-' 的 ISO 日期，或 8+ 位纯数字(≥10000000) → 加引号
    const isRelative = /^[\d.]+d?$/.test(value) && (!/^\d+$/.test(value) || Number(value) < 10000000)
    formattedValue = isRelative ? value : `"${value}"`
  } else {
    formattedValue = value
  }

  return `${field} ${operator} ${formattedValue}`
}

/**
 * 将条件数组序列化为后端 filter 表达式
 *
 * 过滤掉不完整的条件（缺少 field/operator/value）。
 * 同组条件用 `and` 连接。
 *
 * @returns 表达式字符串，无有效条件时返回空字符串
 */
export function serializeConditions(conditions: Condition[]): string {
  const valid = conditions.filter((c) => c.field && c.operator && c.value)
  if (valid.length === 0) return ''
  return valid.map(serializeCondition).join(' and ')
}

/**
 * 将条件组数组序列化为后端 filter 表达式
 *
 * 组内条件用 `and` 连接，组间用 `or` 连接。
 * 多组时每组加括号: `(a and b) or (c and d)`
 * 单组时不加括号: `a and b`（向后兼容）
 */
/** 已知操作符列表（按长度降序匹配，避免 `=` 误匹配 `==`） */
const OPERATORS = ['==', '!=', '>=', '<=', '=~', '!~', '>', '<']

/**
 * 反序列化单个条件片段 `field op value`
 */
function deserializeCondition(token: string): Condition | null {
  const trimmed = token.trim()
  if (!trimmed) return null

  for (const op of OPERATORS) {
    const idx = trimmed.indexOf(` ${op} `)
    if (idx !== -1) {
      const field = trimmed.slice(0, idx).trim()
      let value = trimmed.slice(idx + op.length + 2).trim()
      // 去掉引号
      if (value.startsWith('"') && value.endsWith('"')) {
        value = value.slice(1, -1)
      }
      if (field && value) return { field, operator: op, value }
    }
  }
  return null
}

/**
 * 反序列化条件表达式为条件组数组
 *
 * 格式: `(a and b) or (c and d)` 或 `a and b`
 */
export function deserializeConditionGroups(expr: string): ConditionGroup[] {
  if (!expr || !expr.trim()) return []

  const trimmed = expr.trim()

  // 检测是否有多组（包含 `) or (` 模式）
  let groupStrings: string[]
  if (trimmed.includes(') or (')) {
    groupStrings = trimmed.split(') or (').map((s, i, arr) => {
      if (i === 0) s = s.replace(/^\(/, '')
      if (i === arr.length - 1) s = s.replace(/\)$/, '')
      return s.trim()
    })
  } else {
    // 单组，去掉可能的外层括号
    groupStrings = [trimmed.replace(/^\(/, '').replace(/\)$/, '').trim()]
  }

  return groupStrings
    .map((gs) => {
      const conditions = gs
        .split(' and ')
        .map(deserializeCondition)
        .filter((c): c is Condition => c !== null)
      return { conditions }
    })
    .filter((g) => g.conditions.length > 0)
}

export function serializeConditionGroups(groups: ConditionGroup[]): string {
  const serialized = groups.map((g) => serializeConditions(g.conditions)).filter((s) => s !== '')

  if (serialized.length === 0) return ''
  if (serialized.length === 1) return serialized[0]
  return serialized.map((s) => `(${s})`).join(' or ')
}
