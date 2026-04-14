import { describe, it, expect } from 'vitest'
import {
  serializeConditions,
  serializeConditionGroups,
  type Condition,
  type ConditionGroup,
} from './condition-serializer'

// ==================== 单条件序列化 ====================

describe('serializeConditions: 单条件', () => {
  // --- name 字段 (glob, 加引号) ---
  it('name == glob pattern', () => {
    const result = serializeConditions([{ field: 'name', operator: '==', value: '*.txt' }])
    expect(result).toBe('name == "*.txt"')
  })

  it('name != glob pattern', () => {
    const result = serializeConditions([{ field: 'name', operator: '!=', value: 'temp_*' }])
    expect(result).toBe('name != "temp_*"')
  })

  // --- extension 字段 (glob, 加引号) ---
  it('extension == value', () => {
    const result = serializeConditions([{ field: 'extension', operator: '==', value: 'rs' }])
    expect(result).toBe('extension == "rs"')
  })

  it('extension != wildcard', () => {
    const result = serializeConditions([{ field: 'extension', operator: '!=', value: 'log' }])
    expect(result).toBe('extension != "log"')
  })

  // --- path 字段 (glob, 加引号) ---
  it('path == with double wildcard', () => {
    const result = serializeConditions([{ field: 'path', operator: '==', value: 'src/**' }])
    expect(result).toBe('path == "src/**"')
  })

  it('path != pattern', () => {
    const result = serializeConditions([{ field: 'path', operator: '!=', value: 'tmp/*' }])
    expect(result).toBe('path != "tmp/*"')
  })

  // --- size 字段 (bytes, 不加引号) ---
  it('size > number', () => {
    const result = serializeConditions([{ field: 'size', operator: '>', value: '1024' }])
    expect(result).toBe('size > 1024')
  })

  it('size <= number', () => {
    const result = serializeConditions([{ field: 'size', operator: '<=', value: '1048576' }])
    expect(result).toBe('size <= 1048576')
  })

  // --- type 字段 (enum, 不加引号) ---
  it('type == file', () => {
    const result = serializeConditions([{ field: 'type', operator: '==', value: 'file' }])
    expect(result).toBe('type == file')
  })

  it('type != symlink', () => {
    const result = serializeConditions([{ field: 'type', operator: '!=', value: 'symlink' }])
    expect(result).toBe('type != symlink')
  })

  it('type == dir', () => {
    const result = serializeConditions([{ field: 'type', operator: '==', value: 'dir' }])
    expect(result).toBe('type == dir')
  })

  // --- dir_date 字段 (date, 加引号) ---
  it('dir_date >= compact date', () => {
    const result = serializeConditions([{ field: 'dir_date', operator: '>=', value: '20240101' }])
    expect(result).toBe('dir_date >= "20240101"')
  })

  it('dir_date <= ISO date', () => {
    const result = serializeConditions([{ field: 'dir_date', operator: '<=', value: '2024-03-01' }])
    expect(result).toBe('dir_date <= "2024-03-01"')
  })
})

// ==================== modified 字段 (相对/绝对) ====================

describe('serializeConditions: modified 字段格式化', () => {
  // 相对时间 — 不加引号
  it('相对天数带 d 后缀: 3d', () => {
    const result = serializeConditions([{ field: 'modified', operator: '<', value: '3d' }])
    expect(result).toBe('modified < 3d')
  })

  it('相对天数纯数字: 30', () => {
    const result = serializeConditions([{ field: 'modified', operator: '>', value: '30' }])
    expect(result).toBe('modified > 30')
  })

  it('相对天数小数: 0.5', () => {
    const result = serializeConditions([{ field: 'modified', operator: '<', value: '0.5' }])
    expect(result).toBe('modified < 0.5')
  })

  it('相对天数小数带 d: 1.5d', () => {
    const result = serializeConditions([{ field: 'modified', operator: '>=', value: '1.5d' }])
    expect(result).toBe('modified >= 1.5d')
  })

  it('相对天数较大但 < 10000000: 365', () => {
    const result = serializeConditions([{ field: 'modified', operator: '<', value: '365' }])
    expect(result).toBe('modified < 365')
  })

  // 绝对日期 — 加引号
  it('绝对日期 ISO 格式: 2025-01-15', () => {
    const result = serializeConditions([{ field: 'modified', operator: '>', value: '2025-01-15' }])
    expect(result).toBe('modified > "2025-01-15"')
  })

  it('绝对日期紧凑 8 位: 20250115', () => {
    const result = serializeConditions([{ field: 'modified', operator: '>=', value: '20250115' }])
    expect(result).toBe('modified >= "20250115"')
  })

  it('绝对日期 ISO 带时间: 2025-01-15T12:00:00', () => {
    const result = serializeConditions([{ field: 'modified', operator: '<=', value: '2025-01-15T12:00:00' }])
    expect(result).toBe('modified <= "2025-01-15T12:00:00"')
  })

  it('边界值 10000000 视为绝对日期', () => {
    const result = serializeConditions([{ field: 'modified', operator: '<', value: '10000000' }])
    expect(result).toBe('modified < "10000000"')
  })

  it('边界值 9999999 视为相对天数', () => {
    const result = serializeConditions([{ field: 'modified', operator: '<', value: '9999999' }])
    expect(result).toBe('modified < 9999999')
  })
})

// ==================== 多条件组合 (AND) ====================

describe('serializeConditions: 多条件 AND 组合', () => {
  it('name + size 组合', () => {
    const conditions: Condition[] = [
      { field: 'name', operator: '==', value: '*.txt' },
      { field: 'size', operator: '>', value: '1024' },
    ]
    expect(serializeConditions(conditions)).toBe('name == "*.txt" and size > 1024')
  })

  it('path + modified + type 三条件组合', () => {
    const conditions: Condition[] = [
      { field: 'path', operator: '==', value: 'src/**' },
      { field: 'modified', operator: '<', value: '30' },
      { field: 'type', operator: '==', value: 'file' },
    ]
    expect(serializeConditions(conditions)).toBe('path == "src/**" and modified < 30 and type == file')
  })

  it('extension + size + name 三条件组合', () => {
    const conditions: Condition[] = [
      { field: 'extension', operator: '!=', value: 'log' },
      { field: 'size', operator: '<=', value: '1048576' },
      { field: 'name', operator: '==', value: 'data_*' },
    ]
    expect(serializeConditions(conditions)).toBe('extension != "log" and size <= 1048576 and name == "data_*"')
  })

  it('dir_date 范围组合', () => {
    const conditions: Condition[] = [
      { field: 'dir_date', operator: '>=', value: '20240101' },
      { field: 'dir_date', operator: '<=', value: '20240331' },
    ]
    expect(serializeConditions(conditions)).toBe('dir_date >= "20240101" and dir_date <= "20240331"')
  })

  it('modified 范围 + extension 组合', () => {
    const conditions: Condition[] = [
      { field: 'modified', operator: '>=', value: '2025-01-01' },
      { field: 'modified', operator: '<', value: '2025-03-01' },
      { field: 'extension', operator: '==', value: 'rs' },
    ]
    expect(serializeConditions(conditions)).toBe(
      'modified >= "2025-01-01" and modified < "2025-03-01" and extension == "rs"',
    )
  })
})

// ==================== 不完整条件过滤 ====================

describe('serializeConditions: 不完整条件过滤', () => {
  it('空数组返回空字符串', () => {
    expect(serializeConditions([])).toBe('')
  })

  it('缺少 field 的条件被过滤', () => {
    const conditions: Condition[] = [
      { field: '', operator: '==', value: '*.txt' },
      { field: 'name', operator: '==', value: '*.rs' },
    ]
    expect(serializeConditions(conditions)).toBe('name == "*.rs"')
  })

  it('缺少 operator 的条件被过滤', () => {
    const conditions: Condition[] = [
      { field: 'name', operator: '', value: '*.txt' },
      { field: 'size', operator: '>', value: '100' },
    ]
    expect(serializeConditions(conditions)).toBe('size > 100')
  })

  it('缺少 value 的条件被过滤', () => {
    const conditions: Condition[] = [
      { field: 'name', operator: '==', value: '' },
      { field: 'type', operator: '==', value: 'file' },
    ]
    expect(serializeConditions(conditions)).toBe('type == file')
  })

  it('全部不完整返回空字符串', () => {
    const conditions: Condition[] = [
      { field: '', operator: '', value: '' },
      { field: 'name', operator: '', value: '' },
      { field: '', operator: '==', value: '*.txt' },
    ]
    expect(serializeConditions(conditions)).toBe('')
  })

  it('不完整条件夹杂在有效条件之间', () => {
    const conditions: Condition[] = [
      { field: 'name', operator: '==', value: '*.txt' },
      { field: 'size', operator: '', value: '' },
      { field: 'type', operator: '==', value: 'dir' },
    ]
    expect(serializeConditions(conditions)).toBe('name == "*.txt" and type == dir')
  })
})

// ==================== 与后端 parse_filter_expression 兼容性 ====================

describe('serializeConditions: 后端兼容性验证', () => {
  it('典型 include 表达式: 文件名 + 文件类型', () => {
    const conditions: Condition[] = [
      { field: 'name', operator: '==', value: '*.rs' },
      { field: 'type', operator: '==', value: 'file' },
    ]
    // 后端可解析: name == "*.rs" and type == file
    expect(serializeConditions(conditions)).toBe('name == "*.rs" and type == file')
  })

  it('典型 exclude 表达式: 大文件 + 临时目录', () => {
    const conditions: Condition[] = [
      { field: 'size', operator: '>', value: '104857600' },
      { field: 'path', operator: '==', value: '**/tmp/**' },
    ]
    // 后端可解析: size > 104857600 and path == "**/tmp/**"
    expect(serializeConditions(conditions)).toBe('size > 104857600 and path == "**/tmp/**"')
  })

  it('典型 exclude 表达式: 排除旧文件 + 日志', () => {
    const conditions: Condition[] = [
      { field: 'modified', operator: '<', value: '365' },
      { field: 'extension', operator: '==', value: 'log' },
    ]
    expect(serializeConditions(conditions)).toBe('modified < 365 and extension == "log"')
  })
})

// ==================== OR 条件组序列化 ====================

describe('serializeConditionGroups: 基本组合', () => {
  it('空组数组返回空字符串', () => {
    expect(serializeConditionGroups([])).toBe('')
  })

  it('单组单条件 — 无括号', () => {
    const groups: ConditionGroup[] = [{ conditions: [{ field: 'name', operator: '==', value: '*.txt' }] }]
    expect(serializeConditionGroups(groups)).toBe('name == "*.txt"')
  })

  it('单组多条件 — AND 连接，无括号', () => {
    const groups: ConditionGroup[] = [
      {
        conditions: [
          { field: 'name', operator: '==', value: '*.txt' },
          { field: 'size', operator: '>', value: '1024' },
        ],
      },
    ]
    expect(serializeConditionGroups(groups)).toBe('name == "*.txt" and size > 1024')
  })

  it('多组各单条件 — OR 连接，加括号', () => {
    const groups: ConditionGroup[] = [
      { conditions: [{ field: 'name', operator: '==', value: '*.txt' }] },
      { conditions: [{ field: 'size', operator: '>', value: '1024' }] },
    ]
    expect(serializeConditionGroups(groups)).toBe('(name == "*.txt") or (size > 1024)')
  })

  it('多组各多条件 — 组内 AND + 组间 OR', () => {
    const groups: ConditionGroup[] = [
      {
        conditions: [
          { field: 'name', operator: '==', value: '*.txt' },
          { field: 'type', operator: '==', value: 'file' },
        ],
      },
      {
        conditions: [
          { field: 'path', operator: '==', value: 'src/**' },
          { field: 'size', operator: '>', value: '100' },
        ],
      },
    ]
    expect(serializeConditionGroups(groups)).toBe(
      '(name == "*.txt" and type == file) or (path == "src/**" and size > 100)',
    )
  })

  it('三组 OR 连接', () => {
    const groups: ConditionGroup[] = [
      { conditions: [{ field: 'extension', operator: '==', value: 'rs' }] },
      { conditions: [{ field: 'extension', operator: '==', value: 'toml' }] },
      { conditions: [{ field: 'extension', operator: '==', value: 'md' }] },
    ]
    expect(serializeConditionGroups(groups)).toBe('(extension == "rs") or (extension == "toml") or (extension == "md")')
  })
})

describe('serializeConditionGroups: 不完整条件过滤', () => {
  it('组内不完整条件被过滤', () => {
    const groups: ConditionGroup[] = [
      {
        conditions: [
          { field: 'name', operator: '==', value: '*.txt' },
          { field: '', operator: '', value: '' },
        ],
      },
    ]
    expect(serializeConditionGroups(groups)).toBe('name == "*.txt"')
  })

  it('全部条件不完整的组被跳过', () => {
    const groups: ConditionGroup[] = [
      { conditions: [{ field: '', operator: '', value: '' }] },
      { conditions: [{ field: 'name', operator: '==', value: '*.rs' }] },
    ]
    expect(serializeConditionGroups(groups)).toBe('name == "*.rs"')
  })

  it('单有效组 + 空组 — 退化为单组无括号', () => {
    const groups: ConditionGroup[] = [
      {
        conditions: [
          { field: 'name', operator: '==', value: '*.txt' },
          { field: 'size', operator: '>', value: '1024' },
        ],
      },
      { conditions: [{ field: '', operator: '', value: '' }] },
      { conditions: [] },
    ]
    expect(serializeConditionGroups(groups)).toBe('name == "*.txt" and size > 1024')
  })

  it('全部组为空返回空字符串', () => {
    const groups: ConditionGroup[] = [{ conditions: [] }, { conditions: [{ field: '', operator: '', value: '' }] }]
    expect(serializeConditionGroups(groups)).toBe('')
  })
})

describe('serializeConditionGroups: 后端兼容性', () => {
  it('典型多组 include: 文件名 OR 路径匹配', () => {
    const groups: ConditionGroup[] = [
      {
        conditions: [
          { field: 'name', operator: '==', value: '*.rs' },
          { field: 'type', operator: '==', value: 'file' },
        ],
      },
      { conditions: [{ field: 'path', operator: '==', value: 'docs/**' }] },
    ]
    expect(serializeConditionGroups(groups)).toBe('(name == "*.rs" and type == file) or (path == "docs/**")')
  })

  it('典型多组 exclude: 大文件 OR 临时目录 OR 日志', () => {
    const groups: ConditionGroup[] = [
      { conditions: [{ field: 'size', operator: '>', value: '104857600' }] },
      { conditions: [{ field: 'path', operator: '==', value: '**/tmp/**' }] },
      { conditions: [{ field: 'extension', operator: '==', value: 'log' }] },
    ]
    expect(serializeConditionGroups(groups)).toBe('(size > 104857600) or (path == "**/tmp/**") or (extension == "log")')
  })
})
