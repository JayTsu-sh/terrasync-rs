use std::fmt;
use std::sync::Arc;

use data_mover::storage::RecoveryIdentity;
use data_mover::transfer::RecoveryRegistrar;

use crate::error::{DatabaseError, Result};

const MAX_ATTEMPT_ID_BYTES: usize = 256;

/// Caller-persisted ordering identity for one transfer attempt.
///
/// `order` must increase when the caller intentionally starts a replacement attempt. Reopening
/// the same attempt uses the same pair, which lets the database return the original claim token
/// after a process restart. `id` deterministically breaks ties between competing callers that
/// accidentally use the same order.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct RecoveryAttemptId {
    pub(crate) order: u64,
    pub(crate) id: String,
}

impl RecoveryAttemptId {
    /// Creates a bounded attempt identity.
    ///
    /// # Errors
    /// Blank, NUL-containing, or oversized identities are rejected.
    pub fn new(order: u64, id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if id.trim().is_empty() || id.contains('\0') || id.len() > MAX_ATTEMPT_ID_BYTES {
            return Err(DatabaseError::ConfigError(
                "recovery attempt id must be non-blank and bounded".to_string(),
            ));
        }
        Ok(Self { order, id })
    }
}

impl fmt::Debug for RecoveryAttemptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryAttemptId")
            .field("order", &self.order)
            .field("id", &self.id)
            .finish()
    }
}

/// Durable recovery inputs for one entry and the currently-owned attempt.
pub struct RecoveryAttemptRegistration {
    pub(crate) identity: Option<RecoveryIdentity>,
    pub(crate) claim: [u8; 32],
    pub(crate) registrar: Arc<dyn RecoveryRegistrar>,
}

impl RecoveryAttemptRegistration {
    #[must_use]
    pub const fn identity(&self) -> Option<&RecoveryIdentity> {
        self.identity.as_ref()
    }

    #[must_use]
    pub const fn claim(&self) -> [u8; 32] {
        self.claim
    }

    #[must_use]
    pub fn registrar(&self) -> Arc<dyn RecoveryRegistrar> {
        Arc::clone(&self.registrar)
    }
}
