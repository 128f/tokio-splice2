//! `splice(2)` operation state and primitives.

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::os::fd::AsFd;
use std::{fmt, io};

use rustix::pipe::{splice, SpliceFlags};

use crate::io::{IsFile, IsNotFile};
use crate::pipe::Pipe;
use crate::traffic::TrafficResult;
use crate::utils::Offset;

/// State for a `splice(2)` operation: the pipe used as the kernel-side buffer,
/// the byte counters, and any file offset.
pub struct Splicer<R, W> {
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

impl<R, W> fmt::Debug for Splicer<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Splicer")
            .field("offset", &self.offset)
            .field("size_to_splice", &self.size_to_splice)
            .field("pipe", &self.pipe)
            .field("bytes_read", &self.bytes_read)
            .field("bytes_written", &self.bytes_written)
            .finish()
    }
}

impl<R, W> Splicer<R, W> {
    #[inline]
    fn new_inner() -> io::Result<Self> {
        Ok(Self {
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
    /// Prepare a new `Splicer` instance.
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
    /// Prepare a new `Splicer` instance.
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
        Ok(Splicer {
            offset: Offset::In(Some(f_offset_start.unwrap_or(0))),
            size_to_splice: Offset::calc_size_to_splice(f_len, f_offset_start, f_offset_end)?
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file size too large"))?,
            ..Self::new_inner()?
        })
    }

    #[inline]
    /// Prepare a new `Splicer` instance.
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
        Ok(Splicer {
            offset: Offset::Out(Some(f_offset_start.unwrap_or(0))),
            size_to_splice: Offset::calc_size_to_splice(f_len, f_offset_start, f_offset_end)?
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file size too large"))?,
            ..Self::new_inner()?
        })
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
        self.pipe.set_pipe_size(pipe_size)?;
        Ok(self)
    }
}

impl<R, W> Splicer<R, W> {
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

impl<R, W> Splicer<R, W> {
    /// `try_splice_from_source` moves data from a socket (or file) into the pipe.
    ///
    /// Precondition: when called, the pipe is empty. It is either in its initial
    /// state, or `try_splice_to_dest` has emptied it previously.
    ///
    /// Given this, the pipe is ready for writing, so if splice returns EAGAIN
    /// it must be because the source is not ready for reading.
    ///
    /// Closes the pipe write side when the target byte count has been reached.
    pub(crate) fn try_splice_from_source(&mut self, r: &impl AsFd) -> io::Result<()> {
        let Some(pipe_write_side_fd) = self.pipe.write_side_fd() else {
            return Ok(());
        };

        let Some(size_rest_to_splice) = self
            .size_to_splice
            .checked_sub(self.bytes_read)
            .and_then(NonZeroUsize::new)
        else {
            self.pipe.set_write_side_done();

            return Ok(());
        };

        match splice(
            r.as_fd(),
            self.offset.off_in(),
            pipe_write_side_fd,
            None,
            size_rest_to_splice.get(),
            SpliceFlags::NONBLOCK,
        ) {
            Ok(0) => {
                self.pipe.set_write_side_done();
                Ok(())
            }
            Ok(n) => {
                self.bytes_read += n;
                Ok(())
            }
            Err(e) => Err(io::Error::from_raw_os_error(e.raw_os_error())),
        }
    }

    /// `try_splice_to_dest` moves data from the pipe to a socket (or file).
    ///
    /// Performs a single non-blocking splice attempt. Returns EAGAIN if the
    /// destination is not ready.
    pub(crate) fn try_splice_to_dest(&mut self, w: &impl AsFd) -> io::Result<()> {
        let Some(pipe_read_side_fd) = self.pipe.read_side_fd() else {
            return Ok(());
        };

        let Some(size_need_to_be_written) = self
            .bytes_read
            .checked_sub(self.bytes_written)
            .and_then(NonZeroUsize::new)
        else {
            return Ok(());
        };

        match splice(
            pipe_read_side_fd,
            None,
            w.as_fd(),
            self.offset.off_out(),
            size_need_to_be_written.get(),
            SpliceFlags::NONBLOCK,
        ) {
            Ok(0) => Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                self.bytes_written += n;
                Ok(())
            }
            Err(e) => Err(io::Error::from_raw_os_error(e.raw_os_error())),
        }
    }
}
