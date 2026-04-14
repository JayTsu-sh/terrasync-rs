# License 系统设计文档

## 概述

terrasync 采用离线 license 授权机制，通过 Ed25519 签名 + HMAC 绑定 + 机器指纹实现软件授权控制。License 文件为自包含的 JSON 格式，无需联网验证。

### 部署约束

- 完全离线，无激活服务器
- 管理员分发统一的 binary + license.json 到目标机器
- 目标机器自行激活

---

## License 生命周期

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  签发    │ ──→ │  分发    │ ──→ │  激活    │ ──→ │  验证    │
│ license- │     │ 管理员   │     │ 目标机器 │     │ 每次启动 │
│ gen      │     │ 发放文件 │     │ activate │     │ verify   │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
                                       │                │
                                       ▼                ▼
                                  ┌──────────┐    ┌──────────┐
                                  │ 激活窗口 │    │ License  │
                                  │ 过期则   │    │ 过期则   │
                                  │ 不可激活 │    │ 不可运行 │
                                  └──────────┘    └──────────┘
```

### 1. 签发（管理员操作）

```bash
# 生成密钥对（仅首次）
license-gen keygen --output ./keys

# 签发 license
license-gen generate \
    --key keys/private.key \
    --licensee "客户名称" \
    --days 365 \
    --max-machines 1 \
    --activation-window 1 \
    --output license.json
```

参数说明：
- `--days N`：有效期（天），省略则永久
- `--max-machines N`：最大绑定机器数（0 = 不限）
- `--activation-window N`：**激活有效期**（天），签发后必须在此期限内完成激活，过期后 license 文件作废，无法再激活

### 2. 激活（目标机器操作）

```bash
terrasync activate --license license.json
```

激活流程：
1. 验证 Ed25519 签名
2. 检查**激活有效期**：`当前时间 - issued_at > activation_window_days` → 报错 `Activation window expired`
3. 采集机器指纹（machine-id + MAC + 磁盘序列号 → SHA-256）
4. 创建机器绑定（fingerprint + HMAC 保护）
5. 初始化 license 时钟
6. 写入本地隐蔽哨兵文件
7. 原子写回 license.json

**激活有效期**是对 license 分发安全的关键控制：窗口越短，license 文件被复制后可利用的时间越短。建议根据部署节奏设置：

| 窗口期 | 适用场景 |
|--------|---------|
| 7 天（默认） | 邮寄/物流分发，部署周期长 |
| 1 天 | 当日部署，远程交付 |
| 1 小时 | 即时部署，现场交付 |

### 3. 验证（每次启动自动执行）

terrasync 启动时自动调用 `verify_license()`：
1. Ed25519 签名验证
2. License 时钟检查（防止系统时钟回拨）
3. HMAC 绑定完整性验证
4. 有效期检查（使用 license 时钟而非系统时钟）
5. 机器指纹匹配
6. 哨兵文件一致性检查
7. 更新 license 时钟 + 哨兵

---

## License 文件格式

```json
{
  "payload": {
    "license_id": "UUID-v4",
    "licensee": "客户名称",
    "issued_at": "2026-03-01T00:00:00Z",
    "expires_at": "2027-03-01T00:00:00Z",
    "activation_window_days": 1,
    "max_machines": 1
  },
  "signature": "Ed25519签名(Base64)",
  "activation": {
    "machines": [
      {
        "machine_fingerprint": "SHA-256 hex",
        "activated_at": "2026-03-01T12:00:00Z",
        "last_verified": "2026-03-20T10:00:00Z"
      }
    ],
    "binding_hmac": "HMAC-SHA256 hex"
  },
  "license_clock": "2026-03-20T10:00:00Z"
}
```

- `payload`：Ed25519 签名保护，不可篡改
- `activation`：HMAC 保护机器绑定完整性
- `license_clock`：单调递增时钟，纳入 HMAC 保护范围

---

## 安全防御机制

### 防御层次

```
第 1 层：Ed25519 签名        → 保护 license 条款不可篡改
第 2 层：HMAC 机器绑定        → 保护激活数据完整性
第 3 层：License 时钟         → 防止系统时钟回拨
第 4 层：本地哨兵文件         → 防止 license 文件还原/跨机复制
第 5 层：远程存储时间校验      → 防止 VM 快照回滚（sync 场景）
第 6 层：磁盘序列号指纹       → 缓解 VM 克隆攻击
第 7 层：激活窗口期           → 限制 license 文件被复制后的可利用时间
第 8 层：错误信息模糊化       → 阻止攻击者通过错误消息调试攻击手段
```

### 可防御的攻击

#### 攻击 1：回拨系统时钟续命 — 完全防御

**攻击方式**：License 过期后，将系统时钟回拨到过期前，重新运行程序。

**防御机制**：License 时钟（`license_clock`）。每次 `verify_license()` 成功后，将当前时间持久化到 license 文件和哨兵文件。下次启动时比较系统时钟与持久化值：

- 系统时钟 >= 持久化值 → 正常（进程停止期间时间自然流逝）
- 系统时钟 < 持久化值 → 时钟被回拨 → 阻止运行

有效期检查使用 license 时钟值而非 `Utc::now()`，确保即使系统时钟被回拨，过期判定仍然基于真实时间。

#### 攻击 2：还原旧 license 文件 — 完全防御

**攻击方式**：用早期备份的 license.json 覆盖当前文件，使 `license_clock` 回退。

**防御机制**：哨兵文件交叉比对。license 时钟同时写入 license 文件和本地隐蔽哨兵文件。验证时比对两者：

- 哨兵 clock > license 文件 clock → license 文件被还原 → 阻止运行
- 哨兵 clock <= license 文件 clock → 正常

哨兵文件存储在隐蔽系统路径，攻击者需要知道路径才能同步还原。

#### 攻击 3：复制已激活 license 到其他机器 — 完全防御

**攻击方式**：将已激活的 license.json 复制到另一台机器。

**防御机制**：机器指纹绑定 + 哨兵检查。

- 另一台机器的指纹不同 → `MachineBindingMismatch` → 阻止运行
- 另一台机器无哨兵文件或哨兵中指纹不匹配 → `SentinelMismatch` → 阻止运行

#### 攻击 4：VM 快照回滚 — 完全防御（远程存储 sync 场景）

**攻击方式**：在 VM 上使用 license，过期后回滚快照到过期前。快照同时恢复系统时钟、license 文件、哨兵文件，本地所有状态一致，本地防御无法检测。

**防御机制**：远程存储时间校验。sync 操作时在目标远程存储（NFS/S3）写入临时文件，读取其 mtime（由存储服务端设置，不受 VM 快照影响），与 `license_clock` 比较差值：

- 差值在 5 分钟内 → 正常
- 差值超过 5 分钟 → 本地时钟被篡改 → 阻止运行

**限制**：仅对远程存储有效。sync 到本地路径时 mtime 由本地系统时钟设置，无校验意义，跳过。

#### 攻击 5：VM 克隆 — 显著缓解

**攻击方式**：克隆已激活的 VM，克隆体指纹与原机完全一致。

**防御机制**：机器指纹中加入磁盘序列号。大多数 hypervisor（VMware、Hyper-V、KVM）克隆 VM 时会为虚拟磁盘生成新的序列号，导致克隆体指纹变化 → 验证失败。

**限制**：`dd` 裸盘克隆或部分 hypervisor（如 Proxmox `qm clone --full`）可能保留磁盘序列号，此时无法防御。

### 可缓解但无法完全防御的攻击

#### 攻击 6：复制未激活 license 多机激活 — 部分缓解

**攻击方式**：在激活前将 license.json 复制到多台机器，各自独立激活。

**根因**：离线 DRM 根本限制 — 所有机器起始状态完全相同，不存在中心化计数。

**缓解措施**：
- **激活窗口期**：缩短 `activation_window_days`，压缩攻击者的可利用时间窗口。窗口过期后所有未激活副本作废。
- **激活哨兵**：已激活的 license 文件不能被简单复制到其他机器使用。
- **远程存储时间校验**：sync 时利用远程存储的可信时间源，可检测时钟回拨绕过激活窗口的尝试。

**已知缺口 — 回拨系统时钟绕过激活窗口**：未激活的 license 文件没有 `license_clock`（激活时才初始化），也没有哨兵文件（激活时才创建），不存在任何本地时间锚点。攻击者可以在激活窗口过期后回拨系统时钟到 `issued_at` 附近，使窗口检查 `now - issued_at > window` 通过 → 激活成功。此缺口在 sync 阶段可被远程存储时间校验捕获（存储端时间证明窗口已过），但激活动作本身无法防御。

---

## 错误信息安全

所有安全相关的验证失败（时钟回拨、指纹不匹配、HMAC 校验失败、哨兵不一致等）统一输出相同的模糊错误消息：

```
License verification failed
```

不暴露具体失败原因，不区分错误类型。`tracing` 日志同样只输出 `"License verification failed"`。管理员本身也是潜在攻击者，详细日志等于攻击调试指南。

**例外**：`Activation window expired` 明确报出签发时间和窗口天数（这些信息在签名 payload 中本就可见，合法用户需要此信息联系管理员获取新 license）。

---

## 机器指纹组成

| 平台 | 组成 |
|------|------|
| Linux | `/etc/machine-id` + `/sys/class/dmi/id/product_uuid` + 首个物理网卡 MAC + 首个磁盘序列号 → SHA-256 |
| Windows | `MachineGuid`（注册表） + 首个物理网卡 MAC + 首个磁盘序列号 → SHA-256 |

采集失败的组件使用固定 fallback 值（`"no-product-uuid"`、`"no-mac-address"`、`"no-disk-serial"`），确保指纹始终可生成。

---

## 攻击防御总结矩阵

| # | 攻击 | 难度 | 防御效果 | 防御机制 |
|---|------|------|---------|---------|
| 1 | 回拨系统时钟续命 | 低 | **完全防御** | License 时钟 |
| 2 | 还原旧 license 文件 | 低 | **完全防御** | 哨兵交叉比对 |
| 3 | 复制已激活 license 到其他机器 | 低 | **完全防御** | 指纹绑定 + 哨兵 |
| 4 | VM 快照回滚 | 低 | **完全防御（远程存储 sync）/ 部分（本地）** | 远程存储时间校验 |
| 5 | VM 克隆 | 低 | **显著缓解** | 磁盘序列号指纹 |
| 6 | 复制未激活 license 多机激活 | 零 | **部分缓解** | 激活窗口期 + 哨兵（回拨时钟可绕过激活窗口，sync 阶段可被远程存储时间校验捕获） |

运维指南和使用说明见 `docs/license-guide.md`。
