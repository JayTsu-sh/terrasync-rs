# Terrasync License 使用指南

## 概述

Terrasync 使用离线 license 授权系统。License 文件（JSON 格式）包含被授权人信息、有效期和机器绑定信息。License 有**激活有效期**，签发后必须在规定时间内完成激活，过期后 license 文件将无法再被激活。

---

## 管理员指南（签发 License）

### 1. 构建生成工具

```bash
cargo build -p licensing --features generate --release
# 生成的二进制: target/release/license-gen
```

### 2. 生成密钥对（首次使用）

```bash
./license-gen keygen --output ./keys/
```

输出：
- `keys/private.key` — 私钥（**妥善保管，不要泄露**）
- `keys/public.key` — 公钥
- 终端会输出 Rust 代码片段，需要复制到 `licensing/src/keys.rs` 并重新编译 terrasync

### 3. 签发 License

#### 单机限时（最常见）

```bash
./license-gen generate \
    --key ./keys/private.key \
    --licensee "客户名称" \
    --days 30 \
    --output license.json
```

#### 单机永久

```bash
./license-gen generate \
    --key ./keys/private.key \
    --licensee "客户名称" \
    --permanent \
    --output license.json
```

#### 多机限时（3 台机器，90 天）

```bash
./license-gen generate \
    --key ./keys/private.key \
    --licensee "客户名称" \
    --days 90 \
    --max-machines 3 \
    --output license.json
```

#### 不限机器，限时

```bash
./license-gen generate \
    --key ./keys/private.key \
    --licensee "客户名称" \
    --days 90 \
    --max-machines 0 \
    --output license.json
```

#### 自定义激活有效期

License 有一个**激活有效期**（`activation_window_days`），默认 7 天。签发后必须在此期限内完成激活，过期后 license 文件永久作废，无法再激活。

**安全建议**：根据实际部署节奏尽量缩短激活有效期，压缩 license 文件被非法复制后的可利用时间窗口。

```bash
# 激活有效期设为 1 天
./license-gen generate \
    --key ./keys/private.key \
    --licensee "客户名称" \
    --days 365 \
    --activation-window 1 \
    --output license.json
```

| 激活有效期 | 适用场景 |
|-----------|---------|
| 7 天（默认） | 邮寄/物流分发，部署周期长 |
| 1 天 | 当日部署，远程交付 |
| 1 小时 | 即时部署，现场交付 |

### 4. 查看 License 信息

```bash
./license-gen info license.json
```

输出示例：
```
=== License Information ===
  License ID: 550e8400-e29b-41d4-a716-446655440000
  Licensee: 客户名称
  Issued at: 2026-03-12T10:00:00+00:00
  Expires at: 2026-04-11T10:00:00+00:00
  Max machines: 1
  Activation window: 1 days
  Status: NOT ACTIVATED
```

### 5. License 类型组合

| 模式 | max_machines | expires_at | 说明 |
|------|-------------|------------|------|
| 单机限时 | 1（默认） | 指定天数 | 绑定一台机器，到期失效 |
| 单机永久 | 1 | --permanent | 永久绑定一台机器 |
| 多机限时 | N (>1) | 指定天数 | 允许在 N 台机器上激活 |
| 多机永久 | N (>1) | --permanent | N 台机器永久使用 |
| 不限机器限时 | 0 | 指定天数 | 不做机器绑定，仅限时 |
| 不限机器永久 | 0 | --permanent | 无限制（内部/测试） |

### 6. 密钥轮换

1. 运行 `license-gen keygen --output ./new-keys/`
2. 将输出的 Rust 代码片段更新到 `licensing/src/keys.rs`
3. 重新编译 terrasync
4. 旧密钥签发的 license 将无法通过新版本的验证
5. 需要用新密钥重新签发所有 license

---

## 客户指南（使用 License）

### 1. 获取 License

从管理员处获取 `license.json` 文件。

### 2. 部署 License

将 `license.json` 放置在以下位置之一（按优先级排序）：

1. **CLI 参数指定**：`terrasync --license /path/to/license.json scan ...`
2. **配置文件指定**：在 TOML 配置文件中设置 `[license] path = "/path/to/license.json"`
3. **可执行文件同目录**：将 `license.json` 放在 terrasync 二进制文件旁边
4. **当前工作目录**：将 `license.json` 放在运行命令的目录下

### 3. 激活 License

首次使用前，必须在目标机器上执行激活：

```bash
terrasync activate --license /path/to/license.json
```

成功输出：
```
License activated successfully.
```

**重要**：
- License 有**激活有效期**，签发后必须在规定时间内完成激活。如看到 `Activation window expired` 提示，说明激活有效期已过，需联系管理员重新签发。
- 激活后 `license.json` 文件会被更新（写入机器绑定信息），**请勿手动修改此文件**。

### 4. 正常使用

激活后，正常使用 terrasync 命令即可：

```bash
terrasync scan --id my_scan /path/to/dir
terrasync sync --id my_sync /src /dest
```

程序启动时会自动验证 license。

---

## 常见问题

### "License file not found"

License 文件未找到。请检查：
- 文件是否存在于指定路径
- 是否通过 `--license` 参数、配置文件或默认位置正确指定

### "License not activated"

License 尚未在当前机器上激活。请执行：
```bash
terrasync activate --license /path/to/license.json
```

### "Activation window expired"

激活有效期已过。License 签发后有一个激活有效期（默认 7 天），在此期限内必须完成激活。过期后该 license 文件无法再被激活，需联系管理员重新签发。

### "License verification failed"

License 验证失败。可能的原因包括但不限于：
- License 已过期
- 机器硬件发生了更换（网卡、硬盘等）
- License 文件被损坏或篡改
- 系统时间异常

请联系管理员获取新的 license。
