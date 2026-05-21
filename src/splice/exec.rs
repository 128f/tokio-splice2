//! Core `splice(2)` operations against a [`Splicer`].

use std::io;
use std::os::fd::AsFd;

use rustix::pipe::{splice, SpliceFlags};

use super::Splicer;

/// Move data from a socket (or file) into the pipe.
///
/// Precondition: the pipe is empty — either initial state, or
/// [`try_splice_to_dest`] has drained it. The pipe is therefore ready for
/// writing, so an EAGAIN from `splice` must come from the source not being
/// ready for reading.
///
/// Returns the number of bytes spliced into the pipe. `Ok(0)` means no more
/// bytes will ever come from the source — either the target byte count has
/// been reached, or the source hit EOF. The caller is responsible for closing
/// the pipe write side in that case.
pub(crate) fn try_splice_from_source<R: AsFd, W>(
    s: &mut Splicer<R, W>,
    r: &R,
) -> io::Result<usize> {
    let pipe_write_side_fd = s
        .pipe
        .write_side_fd()
        .expect("Caller must check is_finished() before calling");

    let bytes_remaining = s.size_to_splice - s.bytes_read;
    if bytes_remaining == 0 {
        return Ok(0);
    }

    splice(
        r.as_fd(),
        s.offset.off_in(),
        pipe_write_side_fd,
        None,
        bytes_remaining,
        SpliceFlags::NONBLOCK,
    )
    .map_err(Into::into)
}

/// Move data from the pipe to a socket (or file).
///
/// Single non-blocking splice attempt; returns EAGAIN if the destination is
/// not ready.
///
/// Returns the number of bytes spliced out of the pipe. `Ok(0)` means the
/// pipe was already drained (`bytes_read == bytes_written`); a zero return
/// from `splice` itself is a broken invariant and panics.
pub(crate) fn try_splice_to_destination<R, W: AsFd>(
    s: &mut Splicer<R, W>,
    w: &W,
) -> io::Result<usize> {
    let pipe_read_side_fd = s
        .pipe
        .read_side_fd()
        .expect("Caller must check is_finished() before calling");

    let bytes_remaining = s.bytes_read - s.bytes_written;
    if bytes_remaining == 0 {
        return Ok(0);
    }

    match splice(
        pipe_read_side_fd,
        None,
        w.as_fd(),
        s.offset.off_out(),
        bytes_remaining,
        SpliceFlags::NONBLOCK,
    ) {
        Ok(0) => panic!("splice should not return 0 when bytes_remaining > 0"),
        Ok(n) => Ok(n),
        Err(e) => Err(e.into()),
    }
}
