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
