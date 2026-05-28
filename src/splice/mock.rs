//! Test-only [`Splicer`] that plays back scripted byte counts instead of
//! invoking the real `splice(2)` syscall.

#![allow(clippy::panic, reason = "test-only assertions")]

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsFd, BorrowedFd};

use super::Splicer;

/// Scripted [`Splicer`]. Each call pops the next entry from the appropriate
/// queue and returns it; the FDs, offsets, and requested length are ignored.
#[derive(Debug, Default)]
pub(crate) struct MockSplicer {
    splice_in: VecDeque<io::Result<usize>>,
    splice_out: VecDeque<io::Result<usize>>,
}

impl MockSplicer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn script_in<I: IntoIterator<Item = io::Result<usize>>>(mut self, it: I) -> Self {
        self.splice_in.extend(it);
        self
    }

    pub(crate) fn script_out<I: IntoIterator<Item = io::Result<usize>>>(mut self, it: I) -> Self {
        self.splice_out.extend(it);
        self
    }
}

impl Splicer for MockSplicer {
    fn splice_in<R: AsFd>(
        &mut self,
        _r: &R,
        _off_in: Option<&mut u64>,
        _pipe_w: BorrowedFd<'_>,
        _max_len: usize,
    ) -> io::Result<usize> {
        self.splice_in
            .pop_front()
            .expect("MockSplicer: ran out of scripted splice_in results")
    }

    fn splice_out<W: AsFd>(
        &mut self,
        _pipe_r: BorrowedFd<'_>,
        _w: &W,
        _off_out: Option<&mut u64>,
        _max_len: usize,
    ) -> io::Result<usize> {
        self.splice_out
            .pop_front()
            .expect("MockSplicer: ran out of scripted splice_out results")
    }
}
