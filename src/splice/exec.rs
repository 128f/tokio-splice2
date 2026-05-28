//! The [`Splicer`] trait abstracts the two splice directions, and [`Live`] is
//! the production implementation that calls the real `splice(2)` syscall.

use std::io;
use std::os::fd::{AsFd, BorrowedFd};

use rustix::pipe::{splice, SpliceFlags};

/// Performs the two splice directions: source → pipe, and pipe → destination.
///
/// The trait carries no state — [`SpliceCtx`](super::SpliceCtx) owns the pipe,
/// counters, and offsets, and calls into the `Splicer` to move bytes. The
/// trait exists so tests can substitute an implementation that doesn't invoke
/// the real `splice(2)` syscall.
pub trait Splicer {
    /// Move bytes from `r` into the pipe via `pipe_w`.
    ///
    /// Returns the number of bytes transferred. `Ok(0)` indicates the source
    /// has no more bytes to offer (EOF, or the configured target length has
    /// been reached).
    fn splice_in<R: AsFd>(
        &mut self,
        r: &R,
        off_in: Option<&mut u64>,
        pipe_w: BorrowedFd<'_>,
        max_len: usize,
    ) -> io::Result<usize>;

    /// Move bytes from the pipe via `pipe_r` into `w`.
    ///
    /// Returns the number of bytes transferred. The caller guarantees the
    /// pipe is non-empty; an `Ok(0)` return is therefore a broken invariant.
    fn splice_out<W: AsFd>(
        &mut self,
        pipe_r: BorrowedFd<'_>,
        w: &W,
        off_out: Option<&mut u64>,
        max_len: usize,
    ) -> io::Result<usize>;
}

/// Production [`Splicer`]: calls the real `splice(2)` syscall.
#[derive(Default, Debug)]
pub struct Live;

impl Splicer for Live {
    #[inline]
    fn splice_in<R: AsFd>(
        &mut self,
        r: &R,
        off_in: Option<&mut u64>,
        pipe_w: BorrowedFd<'_>,
        max_len: usize,
    ) -> io::Result<usize> {
        splice(
            r.as_fd(),
            off_in,
            pipe_w,
            None,
            max_len,
            SpliceFlags::NONBLOCK,
        )
        .map_err(Into::into)
    }

    #[inline]
    fn splice_out<W: AsFd>(
        &mut self,
        pipe_r: BorrowedFd<'_>,
        w: &W,
        off_out: Option<&mut u64>,
        max_len: usize,
    ) -> io::Result<usize> {
        splice(
            pipe_r,
            None,
            w.as_fd(),
            off_out,
            max_len,
            SpliceFlags::NONBLOCK,
        )
        .map_err(Into::into)
    }
}
