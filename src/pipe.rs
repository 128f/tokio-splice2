//! Linux Pipe - [`pipe(7)`].
//!
//! The default pipe capacity depends on the system default, but it is
//! usually `65536` for those whose page size is `4096`. See [`pipe(7)`]
//! for more details.
//!
//! Customize the pipe size is supported, see [`fcntl(2)`] for details and
//! notices.
//!
//! [`pipe(7)`]: https://man7.org/linux/man-pages/man7/pipe.7.html
//! [`fcntl(2)`]: https://man7.org/linux/man-pages/man2/fcntl.2.html

// FIXME: Pipe pool?

// #[cfg(feature = "feat-pipe-pool")]
// pub(crate) mod pool;

use std::io;
use std::num::NonZeroUsize;

use rustix::fd::OwnedFd;
use rustix::pipe::{fcntl_getpipe_size, fcntl_setpipe_size, pipe_with, PipeFlags};

#[allow(unsafe_code)]
/// `MAXIMUM_PIPE_SIZE` is the maximum amount of data we asks
/// the kernel to move in a single call to `splice(2)`.
///
/// We use 1MB as `splice(2)` writes data through a pipe, and 1MB is the default
/// maximum pipe buffer size, which is determined by
/// `/proc/sys/fs/pipe-max-size`.
///
/// Running applications under unprivileged user may have the pages usage
/// limited. See [`pipe(7)`] for details.
///
/// [`pipe(7)`]: https://man7.org/linux/man-pages/man7/pipe.7.html
pub const MAXIMUM_PIPE_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(1 << 20) };

#[allow(unsafe_code)]
/// `DEFAULT_PIPE_SIZE` is the default pipe size when pipe size is not known.
pub const DEFAULT_PIPE_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(1 << 16) };

#[derive(Debug)]
/// Linux Pipe.
pub struct Pipe {
    /// File descriptor bytes leave the pipe through (read end). `None` once
    /// the drain side is done.
    drain_fd: Option<OwnedFd>,

    /// File descriptor bytes enter the pipe through (write end). `None` once
    /// the fill side is done.
    fill_fd: Option<OwnedFd>,

    /// Pipe size in bytes.
    size: NonZeroUsize,
}

impl Pipe {
    /// Create a pipe, with flags `O_NONBLOCK` and `O_CLOEXEC`.
    ///
    /// The default pipe size is set to `MAXIMUM_PIPE_SIZE` bytes.
    ///
    /// ## Errors
    ///
    /// * If the pipe creation of setting pipe size fails, an `io::Error` is
    ///   returned.
    pub fn new() -> io::Result<Self> {
        pipe_with(PipeFlags::NONBLOCK | PipeFlags::CLOEXEC)
            .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))
            .and_then(|(drain_fd, fill_fd)| {
                // Splice will loop writing MAXIMUM_PIPE_SIZE bytes from the source to the pipe,
                // and then write those bytes from the pipe to the destination.
                // Set the pipe buffer size to MAXIMUM_PIPE_SIZE to optimize that.
                // Ignore errors here, as a smaller buffer size will work,
                // although it will require more system calls.
                let size = match fcntl_setpipe_size(&drain_fd, MAXIMUM_PIPE_SIZE.get()) {
                    Ok(size) => NonZeroUsize::new(size),
                    Err(_) => NonZeroUsize::new(fcntl_getpipe_size(&drain_fd)?),
                }
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        "failed to set pipe size, using default size",
                    )
                })?;

                Ok(Self {
                    drain_fd: Some(drain_fd),
                    fill_fd: Some(fill_fd),
                    size,
                })
            })
    }

    /// Set the pipe size, consuming the pipe and returning it with the new size.
    ///
    /// ## Errors
    ///
    /// See [`fcntl(2)`].
    ///
    /// [`fcntl(2)`]: https://man7.org/linux/man-pages/man2/fcntl.2.html.
    pub fn with_pipe_size(mut self, pipe_size: usize) -> io::Result<Self> {
        let Some(fill_fd) = self.fill_fd.as_ref() else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "fill-side file descriptor is not available",
            ));
        };

        let new_size = fcntl_setpipe_size(fill_fd, pipe_size)
            .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))?;
        self.size = NonZeroUsize::new(new_size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "fcntl returned zero pipe size"))?;
        Ok(self)
    }

    #[inline]
    pub(crate) fn set_drain_done(&mut self) {
        self.drain_fd = None;
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) const fn is_drain_done(&self) -> bool {
        self.drain_fd.is_none()
    }

    #[inline]
    pub(crate) const fn fill_fd(&self) -> Option<&OwnedFd> {
        self.fill_fd.as_ref()
    }

    #[must_use]
    #[inline]
    pub(crate) const fn is_fill_done(&self) -> bool {
        self.fill_fd.is_none()
    }

    #[inline]
    pub(crate) fn set_fill_done(&mut self) {
        self.fill_fd = None;
    }

    #[inline]
    pub(crate) const fn drain_fd(&self) -> Option<&OwnedFd> {
        self.drain_fd.as_ref()
    }

    #[must_use]
    #[inline]
    /// Returns the size of the pipe, in bytes.
    pub const fn size(&self) -> NonZeroUsize {
        self.size
    }
}
