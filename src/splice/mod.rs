//! `splice(2)` operation state and primitives.

pub(crate) mod exec;
#[cfg(test)]
pub(crate) mod mock;
mod util;

pub use self::exec::{Live, Splicer};

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::os::fd::AsFd;
use std::{fmt, io};

use self::util::Offset;
use crate::io::{IsFile, IsNotFile};
use crate::pipe::Pipe;

/// State for a `splice(2)` operation: the pipe used as the kernel-side buffer,
/// the byte counters, and any file offset.
pub struct SpliceCtx<R, W, S = Live> {
    /// Performs the actual `splice(2)` calls. Defaults to [`Live`].
    splicer: S,
    /// The `off_in` when splicing from `R` to the pipe, or the `off_out` when
    /// splicing from the pipe to `W`.
    offset: Offset,
    /// Target length to read from `R` then write to `W`.
    ///
    /// Default is `isize::MAX`, which means read as much as possible.
    size_to_splice: usize,

    /// Pipe used to splice data.
    pipe: Pipe,

    /// Bytes that have been read from `R` into pipe write side.
    bytes_read: usize,
    /// Bytes that have been written to `W` from pipe read side.
    bytes_written: usize,

    r: PhantomData<R>,
    w: PhantomData<W>,
}

impl<R, W, S> fmt::Debug for SpliceCtx<R, W, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpliceCtx")
            .field("offset", &self.offset)
            .field("size_to_splice", &self.size_to_splice)
            .field("pipe", &self.pipe)
            .field("bytes_read", &self.bytes_read)
            .field("bytes_written", &self.bytes_written)
            .finish()
    }
}

impl<R, W> SpliceCtx<R, W, Live> {
    #[inline]
    fn new_inner() -> io::Result<Self> {
        Ok(Self {
            splicer: Live,
            offset: Offset::None,
            size_to_splice: isize::MAX as usize,
            pipe: Pipe::new()?,
            bytes_read: 0,
            bytes_written: 0,
            r: PhantomData,
            w: PhantomData,
        })
    }

    #[inline]
    /// Prepare a new `SpliceCtx` instance.
    ///
    /// Can be used only when `R` and `W` are not files.
    ///
    /// ## Errors
    ///
    /// * Create pipe failed.
    pub fn new() -> io::Result<Self>
    where
        R: IsNotFile,
        W: IsNotFile,
    {
        Self::new_inner()
    }

    #[inline]
    /// Prepare a new `SpliceCtx` instance.
    ///
    /// Can be used only when `R` is a file.
    ///
    /// ## Arguments
    ///
    /// * `f_len` - File length.
    /// * `f_offset_start` - File offset start. Set to `None` to read from the
    ///   beginning.
    /// * `f_offset_end` - File offset end. Set to `None` to read to the end.
    ///
    /// ## Errors
    ///
    /// * Invalid offset.
    /// * Create pipe failed.
    pub fn with_input_file(
        f_len: u64,
        f_offset_start: Option<u64>,
        f_offset_end: Option<u64>,
    ) -> io::Result<Self>
    where
        R: IsFile,
        W: IsNotFile,
    {
        Ok(SpliceCtx {
            offset: Offset::In(f_offset_start.unwrap_or(0)),
            size_to_splice: Offset::calc_size_to_splice(f_len, f_offset_start, f_offset_end)?
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file size too large"))?,
            ..Self::new_inner()?
        })
    }

    #[inline]
    /// Prepare a new `SpliceCtx` instance.
    ///
    /// Can be used only when `W` is a file.
    ///
    /// ## Arguments
    ///
    /// * `f_len` - File length.
    /// * `f_offset_start` - File offset start. Set to `None` to write from the
    ///   beginning.
    /// * `f_offset_end` - File offset end. Set to `None` to write to the end.
    ///
    /// ## Errors
    ///
    /// * Invalid offset.
    /// * Create pipe failed.
    pub fn with_output_file(
        f_len: u64,
        f_offset_start: Option<u64>,
        f_offset_end: Option<u64>,
    ) -> io::Result<Self>
    where
        R: IsNotFile,
        W: IsFile,
    {
        Ok(SpliceCtx {
            offset: Offset::Out(f_offset_start.unwrap_or(0)),
            size_to_splice: Offset::calc_size_to_splice(f_len, f_offset_start, f_offset_end)?
                .try_into()
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "splice range too large")
                })?,
            ..Self::new_inner()?
        })
    }
}

#[cfg(test)]
impl<R, W, S> SpliceCtx<R, W, S> {
    /// Test-only constructor that lets a test wire in its own [`Splicer`]
    /// implementation. Always takes the `IsNotFile`/`IsNotFile` shape.
    pub(crate) fn new_with_splicer(splicer: S) -> io::Result<Self> {
        Ok(Self {
            splicer,
            offset: Offset::None,
            size_to_splice: isize::MAX as usize,
            pipe: Pipe::new()?,
            bytes_read: 0,
            bytes_written: 0,
            r: PhantomData,
            w: PhantomData,
        })
    }
}

impl<R, W, S> SpliceCtx<R, W, S> {
    #[must_use]
    #[inline]
    /// Set the target number of bytes to copy from `R` to `W`.
    ///
    /// If `R` or `W` is a file, use [`with_input_file`](Self::with_input_file)
    /// or [`with_output_file`](Self::with_output_file) instead.
    pub fn with_target_len(self, size_to_splice: usize) -> Self
    where
        R: IsNotFile,
        W: IsNotFile,
    {
        Self {
            size_to_splice,
            ..self
        }
    }

    #[inline]
    /// Set the pipe size in bytes.
    ///
    /// See [`Pipe`]'s top level docs for more details.
    ///
    /// ## Errors
    ///
    /// * Set pipe size failed.
    ///
    /// For more details, see [`fcntl(2)`].
    ///
    /// [`fcntl(2)`]: https://man7.org/linux/man-pages/man2/fcntl.2.html.
    pub fn with_pipe_size(mut self, pipe_size: usize) -> io::Result<Self> {
        self.pipe = self.pipe.with_pipe_size(pipe_size)?;
        Ok(self)
    }
}

impl<R, W, S> SpliceCtx<R, W, S> {
    #[must_use]
    #[inline]
    /// Returns bytes that have been read from `R`.
    pub const fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    #[must_use]
    #[inline]
    /// Returns bytes that have been written to `W`.
    pub const fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Returns whether the internal pipe currently holds bytes that have not
    /// yet been written to `W`.
    #[inline]
    pub const fn pipe_has_data(&self) -> bool {
        self.bytes_read > self.bytes_written
    }

    /// Returns whether the internal pipe is full and cannot accept more bytes
    /// from `R` until some are drained to `W`.
    #[inline]
    pub const fn pipe_is_full(&self) -> bool {
        self.bytes_read - self.bytes_written >= self.pipe.size().get()
    }

    #[must_use]
    #[inline]
    /// Returns the pipe size in bytes.
    pub const fn pipe_size(&self) -> NonZeroUsize {
        self.pipe.size()
    }

    #[inline]
    pub(crate) fn is_source_done(&self) -> bool {
        self.pipe.is_fill_done()
    }

    #[inline]
    #[allow(dead_code)]
    /// Returns if both sides of the pipe are done.
    pub(crate) const fn is_finished(&self) -> bool {
        self.pipe.is_fill_done() && self.bytes_read == self.bytes_written
    }

}

/// The outcome of a single splice attempt between a socket and the internal pipe.
#[derive(Debug)]
pub enum SpliceOutcome {
    /// The socket is exhausted and will close
    Closed,
    /// We didn't actually move any bytes
    NoProgress,
    /// We moved some bytes
    BytesWritten(usize),
}

impl<R: AsFd, W, S: Splicer> SpliceCtx<R, W, S> {
    #[inline]
    pub(crate) fn try_splice_from_source(&mut self, r: &R) -> io::Result<SpliceOutcome> {
        let bytes_remaining = self.size_to_splice - self.bytes_read;
        if bytes_remaining == 0 {
            // we have reached the target; source is done filling the pipe
            self.pipe.set_fill_done();
            return Ok(SpliceOutcome::Closed);
        }
        let pipe_w = self
            .pipe
            .fill_fd()
            .expect("Caller must check is_finished() before calling")
            .as_fd();
        let n = self
            .splicer
            .splice_in(r, self.offset.off_in(), pipe_w, bytes_remaining)?;

        if n > 0 {
            self.bytes_read += n;
            return Ok(SpliceOutcome::BytesWritten(n));
        }
        // 0 means source EOF
        self.pipe.set_fill_done();
        return Ok(SpliceOutcome::Closed);
    }
}

impl<R, W: AsFd, S: Splicer> SpliceCtx<R, W, S> {
    #[inline]
    pub(crate) fn try_splice_to_dest(&mut self, w: &W) -> io::Result<SpliceOutcome> {
        let bytes_remaining = self.bytes_read - self.bytes_written;
        if bytes_remaining == 0 {
            if self.is_source_done() {
                // Source EOF'd and we've drained the pipe, so we're done.
                self.pipe.set_drain_done();
                return Ok(SpliceOutcome::Closed);
            }
            // signal that we made no progress
            return Ok(SpliceOutcome::NoProgress);
        }
        let pipe_r = self
            .pipe
            .drain_fd()
            .expect("Caller must check is_finished() before calling")
            .as_fd();
        let n = self
            .splicer
            .splice_out(pipe_r, w, self.offset.off_out(), bytes_remaining)?;
        if n > 0 {
            self.bytes_written += n;
            return Ok(SpliceOutcome::BytesWritten(n));
        }
        return Ok(SpliceOutcome::NoProgress);
    }
}
