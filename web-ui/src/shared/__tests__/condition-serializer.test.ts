import { describe, test, expect } from 'vitest'
import {
  serializeConditions,
  serializeConditionGroups,
  deserializeConditionGroups,
  type Condition,
  type ConditionGroup,
} from '../condition-serializer'

describe('serializeConditions', () => {
  test('name 字段值加双引号', () => {
    const conditions: Condition[] = [{ field: 'name', operator: '==', value: '*.txt' }]
    expect(serializeConditions(conditions)).toBe('name == "*.txt"')
  })

  test('path 字段值加双引号', () => {
    const conditions: Condition[] = [{ field: 'path', operator: '=~', value: '/data/*' }]
    expect(serializeConditions(conditions)).toBe('path =~ "/data/*"')
  })

  test('extension 字段值加双引号', () => {
    const conditions: Condition[] = [{ field: 'extension', operator: '==', value: '.jpg' }]
    expect(serializeConditions(conditions)).toBe('extension == ".jpg"')
  })

  test('size 字段值不加引号', () => {
    const conditions: Condition[] = [{ field: 'size', operator: '>', value: '1024' }]
    expect(serializeConditions(conditions)).toBe('size > 1024')
  })

  test('type 字段值不加引号', () => {
    const conditions: Condition[] = [{ field: 'type', operator: '==', value: 'file' }]
    expect(serializeConditions(conditions)).toBe('type == file')
  })

  test('modified 相对时间（带 d 后缀）不加引号', () => {
    const conditions: Condition[] = [{ field: 'modified', operator: '<', value: '3d' }]
    expect(serializeConditions(conditions)).toBe('modified < 3d')
  })

  test('modified 纯数字小于阈值视为相对时间，不加引号', () => {
    const conditions: Condition[] = [{ field: 'modified', operator: '<', value: '30' }]
    expect(serializeConditions(conditions)).toBe('modified < 30')
  })

  test('modified 绝对日期加引号', () => {
    const conditions: Condition[] = [{ field: 'modified', operator: '>', value: '2025-01-15' }]
    expect(serializeConditions(conditions)).toBe('modified > "2025-01-15"')
  })

  test('多条件用 and 连接', () => {
    const conditions: Condition[] = [
      { field: 'name', operator: '==', value: '*.jpg' },
      { field: 'size', operator: '>', value: '1024' },
    ]
    expect(serializeConditions(conditions)).toBe('name == "*.jpg" and size > 1024')
  })

  test('不完整条件被过滤', () => {
    const conditions: Condition[] = [
      { field: 'name', operator: '==', value: '*.txt' },
      { field: '', operator: '==', value: 'x' }, // 缺 field
      { field: 'size', operator: '', value: '100' }, // 缺 operator
      { field: 'type', operator: '==', value: '' }, // 缺 value
    ]
    expect(serializeConditions(conditions)).toBe('name == "*.txt"')
  })

  test('所有条件都不完整返回空字符串', () => {
    const conditions: Condition[] = [{ field: '', operator: '', value: '' }]
    expect(serializeConditions(conditions)).toBe('')
  })
})

describe('serializeConditionGroups', () => {
  test('单组不加括号', () => {
    const groups: ConditionGroup[] = [{ conditions: [{ field: 'name', operator: '==', value: '*.txt' }] }]
    expect(serializeConditionGroups(groups)).toBe('name == "*.txt"')
  })

  test('多组用 or 连接并加括号', () => {
    const groups: ConditionGroup[] = [
      {
        conditions: [
          { field: 'name', operator: '==', value: '*.txt' },
          { field: 'size', operator: '>', value: '100' },
        ],
      },
      { conditions: [{ field: 'type', operator: '==', value: 'dir' }] },
    ]
    expect(serializeConditionGroups(groups)).toBe('(name == "*.txt" and size > 100) or (type == dir)')
  })

  test('空组被过滤', () => {
    const groups: ConditionGroup[] = [
      { conditions: [] },
      { conditions: [{ field: 'name', operator: '==', value: '*.txt' }] },
    ]
    expect(serializeConditionGroups(groups)).toBe('name == "*.txt"')
  })

  test('全空返回空字符串', () => {
    expect(serializeConditionGroups([])).toBe('')
    expect(serializeConditionGroups([{ conditions: [] }])).toBe('')
  })
})

describe('deserializeConditionGroups', () => {
  test('单组无括号', () => {
    const result = deserializeConditionGroups('name == "*.txt"')
    expect(result).toEqual([{ conditions: [{ field: 'name', operator: '==', value: '*.txt' }] }])
  })

  test('单组多条件用 and 连接', () => {
    const result = deserializeConditionGroups('name == "*.jpg" and size > 1024')
    expect(result).toEqual([
      {
        conditions: [
          { field: 'name', operator: '==', value: '*.jpg' },
          { field: 'size', operator: '>', value: '1024' },
        ],
      },
    ])
  })

  test('多组用 or 连接', () => {
    const result = deserializeConditionGroups('(name == "*.txt") or (type == dir)')
    expect(result).toEqual([
      { conditions: [{ field: 'name', operator: '==', value: '*.txt' }] },
      { conditions: [{ field: 'type', operator: '==', value: 'dir' }] },
    ])
  })

  test('操作符按长度降序匹配：== 不被 = 误匹配', () => {
    const result = deserializeConditionGroups('name == "test"')
    expect(result[0].conditions[0].operator).toBe('==')
  })

  test('>= 和 <= 正确匹配', () => {
    const result = deserializeConditionGroups('size >= 100 and size <= 9999')
    expect(result[0].conditions[0]).toEqual({ field: 'size', operator: '>=', value: '100' })
    expect(result[0].conditions[1]).toEqual({ field: 'size', operator: '<=', value: '9999' })
  })

  test('=~ 和 !~ 正则操作符', () => {
    const result = deserializeConditionGroups('name =~ "*.log" and path !~ "/tmp/*"')
    expect(result[0].conditions[0]).toEqual({ field: 'name', operator: '=~', value: '*.log' })
    expect(result[0].conditions[1]).toEqual({ field: 'path', operator: '!~', value: '/tmp/*' })
  })

  test('空字符串返回空数组', () => {
    expect(deserializeConditionGroups('')).toEqual([])
    expect(deserializeConditionGroups('  ')).toEqual([])
  })
})

describe('往返一致性 (roundtrip)', () => {
  const cases = [
    'name == "*.txt"',
    'size > 1024',
    'type == file',
    'modified < 3d',
    'name == "*.jpg" and size > 100',
    '(name == "*.txt" and size > 100) or (type == dir)',
  ]

  for (const expr of cases) {
    test(`roundtrip: ${expr}`, () => {
      const groups = deserializeConditionGroups(expr)
      const result = serializeConditionGroups(groups)
      expect(result).toBe(expr)
    })
  }
})
