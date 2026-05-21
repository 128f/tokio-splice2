//! `splice(2)` I/O implementation.

use std::future::poll_fn;
use std::os::fd::AsFd;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{io, ops};

use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, Interest};
use tokio::net::{TcpStream, UnixStream};

use crate::splice::Splicer;
use crate::traffic::TrafficResult;

#[derive(Debug)]
/// Zero-copy unidirectional I/O with `splice(2)`.
///
/// For bidirectional I/O version, see [`SpliceBidiIo`].
///
/// Notice: see the [module-level documentation](crate) for known limitations.
pub struct SpliceIo<R, W> {
    splicer: Splicer<R, W>,
    state: TransferState,
}

impl<R, W> ops::Deref for SpliceIo<R, W> {
    type Target = Splicer<R, W>;

    fn deref(&self) -> &Self::Target {
        &self.splicer
    }
}

impl<R, W> From<Splicer<R, W>> for SpliceIo<R, W> {
    fn from(splicer: Splicer<R, W>) -> Self {
        SpliceIo {
            splicer,
            state: TransferState::Fill,
        }
    }
}

#[derive(Debug)]
enum TransferState {
    /// Moving data from `R` into the pipe.
    Fill,

    /// Moving data from the pipe to `W`.
    Drain,

    /// Flushing buffered data of `W`.
    Flushing,

    /// We have terminated and will attempt to gracefully shutdown,
    /// if there was an error it will be returned
    Finished { error: Option<io::Error> },
}

impl TransferState {
    fn finished() -> Self {
        Self::Finished { error: None }
    }
    fn faulted(error: io::Error) -> Self {
        Self::Finished { error: Some(error) }
    }
}

impl<R, W> SpliceIo<R, W>
where
    R: AsyncReadFd,
    W: AsyncWriteFd,
{
    /// Performs zero-copy data transfer from reader `R` to writer `W` using the
    /// splice syscall.
    ///
    /// This is a convenient `async fn` version of
    /// [`SpliceIo::poll_execute`].
    pub async fn execute(mut self, r: &mut R, w: &mut W) -> TrafficResult {
        let error = poll_fn(|cx| self.poll_execute(cx, r, w)).await.err();

        self.splicer.traffic_client_tx(error)
    }

    #[cfg_attr(
        any(
            feature = "feat-tracing-trace",
            all(debug_assertions, feature = "feat-tracing")
        ),
        tracing::instrument(level = "TRACE", skip(self, cx, r, w), ret)
    )]
    /// Performs zero-copy data transfer from reader `R` to writer `W` using the
    /// splice syscall.
    ///
    /// This is the `poll`-based asynchronous version.
    ///
    /// # Notes
    ///
    /// This is an advanced API that should only be used if you fully understand
    /// its behavior. When using this API:
    ///
    /// - The [`SpliceIo`] instance MUST NOT be reused after completion.
    /// - The caller MAY manually extracts [`TrafficResult`] from the context.
    pub fn poll_execute(
        &mut self,
        cx: &mut Context<'_>,
        r: &mut R,
        w: &mut W,
    ) -> Poll<io::Result<()>> {
        loop {
            crate::enter_tracing_span!(
                "loop",
                splicer = ?self.splicer,
                state = ?self.state,
            );

            if let TransferState::Finished { error } = &mut self.state {
                // Best effort to shutdown the writer.
                ready!(Pin::new(&mut *w).poll_shutdown(cx))?;
                break Poll::Ready(match error.take() {
                    Some(e) => Err(e),
                    None => Ok(()),
                });
            }

            self.state = match self.state {
                TransferState::Fill => {
                    // check if we're ready to read
                    ready!(r.poll_read_ready(cx))?;
                    // try to read, if EAGAIN then loop back and wait again
                    // side-effect: try_io_read will clear the readiness state if it returns EAGAIN, so we won't busy loop
                    match r.try_io_read(|| self.splicer.try_splice_from_source(&*r)) {
                        Ok(()) => TransferState::Drain,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => TransferState::Fill,
                        Err(e) => TransferState::faulted(e),
                    }
                }
                TransferState::Drain => {
                    // check if we're ready to write
                    ready!(w.poll_write_ready(cx))?;
                    // try to write, if EAGAIN then loop back and wait again
                    // side-effect: try_io_write will clear the readiness state if it returns EAGAIN, so we won't busy loop
                    match w.try_io_write(|| self.splicer.try_splice_to_dest(&*w)) {
                        Ok(_) if self.splicer.is_finished() => TransferState::finished(),
                        Ok(_) => TransferState::Flushing,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => TransferState::Drain,
                        Err(e) => TransferState::faulted(e),
                    }
                }
                TransferState::Flushing => match Pin::new(&mut *w).poll_flush(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(_)) => TransferState::Fill,
                    Poll::Ready(Err(e)) => TransferState::faulted(e),
                },
                // we guard and return above this match
                TransferState::Finished { .. } => unreachable!(),
            };
        }
    }
}

#[derive(Debug)]
/// Bidirectional splice I/O, combining two `SpliceIo` instances.
pub struct SpliceBidiIo<SL, SR> {
    /// Splice I/O instance, from `SL` to `SR`.
    pub io_sl2sr: SpliceIo<SL, SR>,

    /// Splice I/O instance, from `SR` to `SL`.
    pub io_sr2sl: SpliceIo<SR, SL>,
}

impl<SL, SR> SpliceBidiIo<SL, SR>
where
    SL: AsyncReadFd + AsyncWriteFd + IsNotFile,
    SR: AsyncReadFd + AsyncWriteFd + IsNotFile,
{
    /// Performs zero-copy data transfer between `SL` and `SR` using the
    /// splice syscall.
    ///
    /// This is a convenient `async fn` version of
    /// [`SpliceBidiIo::poll_execute`].
    pub async fn execute(mut self, sl: &mut SL, sr: &mut SR) -> TrafficResult
    where
        SL: Unpin,
        SR: Unpin,
    {
        let error = poll_fn(|cx| self.poll_execute(cx, sl, sr)).await.err();

        self.io_sl2sr
            .splicer
            .traffic_client_tx(error)
            .merge(self.io_sr2sl.splicer.traffic_client_rx(None))
    }

    #[cfg_attr(
        any(
            feature = "feat-tracing-trace",
            all(debug_assertions, feature = "feat-tracing")
        ),
        tracing::instrument(
            level = "TRACE",
            name = "SpliceBidiIo::poll_execute",
            skip(self, cx, sl, sr),
            ret
        )
    )]
    /// Performs zero-copy data transfer between `SL` and `SR` using the
    /// splice syscall.
    ///
    /// This is the `poll`-based asynchronous version.
    ///
    /// # Notes
    ///
    /// This is an advanced API that should only be used if you fully understand
    /// its behavior. When using this API:
    ///
    /// - The [`SpliceBidiIo`] instance MUST NOT be reused after completion.
    /// - The caller MAY manually extracts [`TrafficResult`] from the context.
    pub fn poll_execute(
        &mut self,
        cx: &mut Context<'_>,
        sl: &mut SL,
        sr: &mut SR,
    ) -> Poll<io::Result<()>> {
        let io_sl2sr_ret = self.io_sl2sr.poll_execute(cx, sl, sr);
        let io_sr2sl_ret = self.io_sr2sl.poll_execute(cx, sr, sl);
        match (io_sl2sr_ret, io_sr2sl_ret) {
            (Poll::Pending, _) | (_, Poll::Pending) => Poll::Pending,
            (Poll::Ready(Ok(())), Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
            (Poll::Ready(Err(e)), _) | (_, Poll::Ready(Err(e))) => Poll::Ready(Err(e)),
        }
    }
}

// === traits ===

/// Marker trait: indicate a file.
///
/// Since the compiler complains *conflicting implementations* when we try to
/// implement `IsFile` for `T: ops::Deref<U>` when U: `IsFile`, you have to
/// implement this marker trait for your wrapper type over a file.
pub trait IsFile {}

impl<T> IsFile for &mut T where T: IsFile {}
impl<T> IsFile for Pin<&mut T> where T: IsFile {}

/// Marker trait: indicate not a file.
///
/// We have to introduce this because Rust does not allow the syntax `!IsFile`
/// (at least only limited to some builtin marker traits like `Send`),
pub trait IsNotFile {}

impl<T> IsNotFile for &mut T where T: IsNotFile {}
impl<T> IsNotFile for Pin<&mut T> where T: IsNotFile {}

/// Marker trait: indicates an async-readable file descriptor.
///
/// This trait extends both `AsyncRead` and `AsFd`, providing the necessary
/// methods for async reading operations with splice.
pub trait AsyncReadFd: AsyncRead + AsFd + Unpin {
    #[doc(hidden)]
    fn poll_read_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    #[doc(hidden)]
    fn try_io_read<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R>;
}

impl<T: AsyncReadFd> AsyncReadFd for &mut T {
    #[inline]
    fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_read_ready(cx)
    }

    #[inline]
    fn try_io_read<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        (**self).try_io_read(f)
    }
}

/// Marker trait: indicates an async-writable file descriptor.
///
/// This trait extends both `AsyncWrite` and `AsFd`, providing the necessary
/// methods for async writing operations with splice.
pub trait AsyncWriteFd: AsyncWrite + AsFd + Unpin {
    #[doc(hidden)]
    fn poll_write_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    #[doc(hidden)]
    fn try_io_write<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R>;
}

impl<T: AsyncWriteFd> AsyncWriteFd for &mut T {
    #[inline]
    fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_write_ready(cx)
    }

    #[inline]
    fn try_io_write<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        (**self).try_io_write(f)
    }
}

macro_rules! impl_async_fd {
    ($($ty:ty),+) => {
        $(
            impl AsyncReadFd for $ty {
                #[inline]
                fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                    self.poll_read_ready(cx)
                }

                #[inline]
                fn try_io_read<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
                    self.try_io(Interest::READABLE, f)
                }
            }

            impl AsyncWriteFd for $ty {
                #[inline]
                fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                    self.poll_write_ready(cx)
                }

                #[inline]
                fn try_io_write<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
                    self.try_io(Interest::WRITABLE, f)
                }
            }

            impl IsNotFile for $ty {}
        )+
    };
    (FILE: $($ty:ty),+) => {
        $(
            impl AsyncReadFd for $ty {
                #[inline]
                fn poll_read_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                    Poll::Ready(Ok(()))
                }

                #[inline]
                fn try_io_read<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
                    f()
                }
            }

            impl AsyncWriteFd for $ty {
                #[inline]
                fn poll_write_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                    Poll::Ready(Ok(()))
                }

                #[inline]
                fn try_io_write<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
                    f()
                }
            }

            impl IsFile for $ty {}
        )+
    };
}

impl_async_fd!(TcpStream, UnixStream);
impl_async_fd!(FILE: File);
