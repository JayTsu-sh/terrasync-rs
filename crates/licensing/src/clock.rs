// 标准库
// 无

// 外部 crate
use chrono::{DateTime, Utc};

// 内部模块
use crate::error::{LicenseError, Result};

/// 单调递增的 License 时钟
///
/// 保证返回的值永远不小于之前持久化的值。
/// 如果检测到系统时钟被回拨（系统时钟 < 持久化值），直接报错阻止运行。
#[derive(Debug, Clone)]
pub struct LicenseClock {
    value: DateTime<Utc>,
}

impl LicenseClock {
    /// 从持久化值恢复，与系统时间比较
    ///
    /// - 系统时钟 >= 持久化值 → 正常，对齐到系统时钟
    /// - 系统时钟 < 持久化值 → 时钟回拨，返回 `ClockRegression` 错误
    pub fn from_persisted(persisted: DateTime<Utc>) -> Result<Self> {
        let system_now = Utc::now();
        if system_now < persisted {
            return Err(LicenseError::ClockRegression);
        }
        Ok(Self { value: system_now })
    }

    /// 首次创建（无持久化值时）
    pub fn initialize() -> Self {
        Self { value: Utc::now() }
    }

    /// 获取当前时钟值
    pub fn now(&self) -> DateTime<Utc> {
        self.value
    }

    /// 推进时钟到系统当前时间
    ///
    /// 仅在进程运行期间调用。如果系统时钟被回拨，返回错误。
    pub fn advance(&mut self) -> Result<DateTime<Utc>> {
        let system_now = Utc::now();
        if system_now < self.value {
            return Err(LicenseError::ClockRegression);
        }
        self.value = system_now;
        Ok(self.value)
    }
}
