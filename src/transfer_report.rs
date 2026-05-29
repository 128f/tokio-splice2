//! Bytes transmitted by a `splice(2)` operation.

use std::io;

#[derive(Debug)]
/// Bytes transmitted throughout the `splice(2)` operation, regardless of any
/// errors.
pub struct TransferReport {
    /// The number of bytes that have been transferred from a to b
    pub tx: usize,

    /// The number of bytes that have been transferred from b to a.
    pub rx: usize,

    /// The error that occurred during the `splice(2)` operation, if any.
    pub error: Option<io::Error>,
}

impl TransferReport {
    #[must_use]
    #[inline]
    /// Merges two `TransferReport` instances.
    pub fn merge(self, other: Self) -> Self {
        Self {
            tx: self.tx + other.tx,
            rx: self.rx + other.rx,
            error: self.error.or(other.error),
        }
    }

    #[must_use]
    #[inline]
    /// Returns the total number of bytes transmitted in both directions.
    pub const fn sum(&self) -> usize {
        self.tx.saturating_add(self.rx)
    }

    #[inline]
    /// Turns the `TransferReport` into an `io::Result<TransferReport>`.
    ///
    /// ## Errors
    ///
    /// Extracts the error from the `TransferReport` if it exists.
    pub fn into_result(self) -> io::Result<Self> {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok(Self {
                error: None,
                ..self
            })
        }
    }
}
