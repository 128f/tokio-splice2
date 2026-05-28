//! Helpers for `SpliceCtx` construction.

use std::io;

#[derive(Debug)]
pub(crate) enum Offset {
    None,
    /// Read offset set.
    In(u64),
    /// Write offset set.
    Out(u64),
}

impl Offset {
    #[inline]
    pub(crate) fn off_in(&mut self) -> Option<&mut u64> {
        match self {
            Offset::In(off) => Some(off),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn off_out(&mut self) -> Option<&mut u64> {
        match self {
            Offset::Out(off) => Some(off),
            _ => None,
        }
    }

    pub(crate) fn calc_size_to_splice(
        f_len: u64,
        f_offset_start: Option<u64>,
        f_offset_end: Option<u64>,
    ) -> io::Result<u64> {
        let start = f_offset_start.unwrap_or(0);
        let end = f_offset_end.unwrap_or(f_len);
        if start > end || end > f_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid offset range",
            ));
        }
        Ok(end - start)
    }
}
