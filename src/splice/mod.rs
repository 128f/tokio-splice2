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
use crate::traffic::TrafficResult;

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

    #[must_use]
    #[inline]
    /// Returns the pipe size in bytes.
    pub const fn pipe_size(&self) -> NonZeroUsize {
        self.pipe.size()
    }

    #[inline]
    /// Returns if both sides of the pipe are done.
    pub(crate) const fn is_finished(&self) -> bool {
        self.pipe.is_write_side_done() && self.bytes_read == self.bytes_written
    }

    #[must_use]
    #[inline]
    /// Returns the traffic result (client TX one).
    pub const fn traffic_client_tx(&self, error: Option<io::Error>) -> TrafficResult {
        TrafficResult {
            tx: self.bytes_written,
            rx: 0,
            error,
        }
    }

    #[must_use]
    #[inline]
    /// Returns the traffic result (client RX one).
    pub const fn traffic_client_rx(&self, error: Option<io::Error>) -> TrafficResult {
        TrafficResult {
            tx: 0,
            rx: self.bytes_read,
            error,
        }
    }
}

impl<R: AsFd, W, S: Splicer> SpliceCtx<R, W, S> {
    #[inline]
    pub(crate) fn try_splice_from_source(&mut self, r: &R) -> io::Result<usize> {
        let bytes_remaining = self.size_to_splice - self.bytes_read;
        if bytes_remaining == 0 {
            self.pipe.set_write_side_done();
            return Ok(0);
        }
        let pipe_w = self
            .pipe
            .write_side_fd()
            .expect("Caller must check is_finished() before calling")
            .as_fd();
        let n = self
            .splicer
            .splice_in(r, self.offset.off_in(), pipe_w, bytes_remaining)?;
        if n == 0 {
            self.pipe.set_write_side_done();
        } else {
            self.bytes_read += n;
        }
        Ok(n)
    }
}

impl<R, W: AsFd, S: Splicer> SpliceCtx<R, W, S> {
    #[inline]
    pub(crate) fn try_splice_to_dest(&mut self, w: &W) -> io::Result<usize> {
        let bytes_remaining = self.bytes_read - self.bytes_written;
        if bytes_remaining == 0 {
            return Ok(0);
        }
        let pipe_r = self
            .pipe
            .read_side_fd()
            .expect("Caller must check is_finished() before calling")
            .as_fd();
        let n = self
            .splicer
            .splice_out(pipe_r, w, self.offset.off_out(), bytes_remaining)?;
        if n == 0 {
            panic!("splice should not return 0 when bytes_remaining > 0");
        }
        self.bytes_written += n;
        Ok(n)
    }
}
