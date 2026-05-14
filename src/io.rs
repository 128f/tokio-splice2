//! `splice(2)` I/O implementation.

use std::future::poll_fn;
use std::os::fd::AsFd;
use std::pin::{pin, Pin};
use std::task::{ready, Context, Poll};
use std::{io, ops};

use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, Interest};
use tokio::net::{TcpStream, UnixStream};

use crate::context::SpliceIoCtx;
use crate::traffic::TrafficResult;

#[pin_project::pin_project]
#[derive(Debug)]
/// Zero-copy unidirectional I/O with `splice(2)`.
///
/// For bidirectional I/O version, see [`SpliceBidiIo`].
///
/// Notice: see the [module-level documentation](crate) for known limitations.
pub struct SpliceIo<R, W> {
    /// Context for the splice I/O operation.
    ///
    /// See [`SpliceIoCtx`] for more details.
    ctx: SpliceIoCtx<R, W>,

    #[pin]
    state: TransferState,
}

impl<R, W> ops::Deref for SpliceIo<R, W> {
    type Target = SpliceIoCtx<R, W>;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl<R, W> From<SpliceIoCtx<R, W>> for SpliceIo<R, W> {
    fn from(ctx: SpliceIoCtx<R, W>) -> Self {
        SpliceIo {
            ctx,
            state: TransferState::FromSource,
        }
    }
}

#[derive(Debug)]
#[pin_project::pin_project(project = TransferStateProj)]
enum TransferState {
    /// Moving data from `R` into the pipe.
    FromSource,

    /// Moving data from the pipe to `W`.
    ToDest,

    /// Flushing buffered data of `W`.
    Flushing,

    /// Transfer is finished, `W` is shutting down.
    Terminating,

    /// An error occurred during the transfer.
    Faulted { error: Option<io::Error> },

    /// Transfer is finished.
    Finished,
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
    pub async fn execute(self, r: &mut R, w: &mut W) -> TrafficResult
    where
        R: Unpin,
        W: Unpin,
    {
        let mut this = pin!(self);
        let mut r = Pin::new(r);
        let mut w = Pin::new(w);

        let error = poll_fn(|cx| this.as_mut().poll_execute(cx, r.as_mut(), w.as_mut()))
            .await
            .err();

        this.ctx.traffic_client_tx(error)
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
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut r: Pin<&mut R>,
        mut w: Pin<&mut W>,
    ) -> Poll<io::Result<()>> {
        macro_rules! ready_or_cleanup {
            ($e:expr, $state:expr) => {
                match $e {
                    Poll::Ready(Ok(t)) => t,
                    Poll::Ready(Err(e)) => {
                        $state.set(TransferState::Faulted { error: Some(e) });
                        continue;
                    }
                    Poll::Pending => {
                        break Poll::Pending;
                    }
                }
            };
        }

        loop {
            crate::enter_tracing_span!(
                "loop",
                ctx = ?self.ctx,
                state = ?self.state,
            );

            let mut this = self.as_mut().project();

            match this.state.as_mut().project() {
                TransferStateProj::FromSource => {
                    let _ = ready_or_cleanup!(
                        this.ctx.poll_splice_from_source(cx, r.as_mut()),
                        this.state.as_mut()
                    );

                    this.state.set(TransferState::ToDest);
                }
                TransferStateProj::ToDest => {
                    ready_or_cleanup!(
                        this.ctx.poll_splice_to_dest(cx, w.as_mut()),
                        this.state.as_mut()
                    );

                    if this.ctx.is_finished() {
                        // All done, flush and shutdown `W`.
                        this.state.set(TransferState::Terminating);
                    } else {
                        // Flush `W` after writing to dest.
                        this.state.set(TransferState::Flushing);
                    }
                }
                TransferStateProj::Flushing => {
                    ready_or_cleanup!(w.as_mut().poll_flush(cx), this.state.as_mut());

                    this.state.set(TransferState::FromSource);
                }
                TransferStateProj::Terminating => {
                    ready_or_cleanup!(w.as_mut().poll_shutdown(cx), this.state.as_mut());

                    this.state.set(TransferState::Finished);
                }
                TransferStateProj::Faulted { error } => {
                    if error.is_some() {
                        // Best effort to shutdown the writer.
                        ready!(w.as_mut().poll_shutdown(cx))?;
                    }

                    let Some(error) = error.take() else {
                        break Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "`poll_execute()` called after error returned",
                        )));
                    };

                    break Poll::Ready(Err(error));
                }
                TransferStateProj::Finished => {
                    break Poll::Ready(Ok(()));
                }
            }
        }
    }
}

#[pin_project::pin_project]
#[derive(Debug)]
/// Bidirectional splice I/O, combining two `SpliceIo` instances.
pub struct SpliceBidiIo<SL, SR> {
    #[pin]
    /// Splice I/O instance, from `SL` to `SR`.
    pub io_sl2sr: SpliceIo<SL, SR>,

    #[pin]
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
    pub async fn execute(self, sl: &mut SL, sr: &mut SR) -> TrafficResult
    where
        SL: Unpin,
        SR: Unpin,
    {
        let mut this = pin!(self);
        let mut sl = Pin::new(sl);
        let mut sr = Pin::new(sr);

        let error = poll_fn(|cx| this.as_mut().poll_execute(cx, sl.as_mut(), sr.as_mut()))
            .await
            .err();

        // After copy done, we can return the traffic result.
        this.io_sl2sr
            .ctx
            .traffic_client_tx(error)
            .merge(this.io_sr2sl.ctx.traffic_client_rx(None))
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
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut sl: Pin<&mut SL>,
        mut sr: Pin<&mut SR>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.project();

        let io_sl2sr_ret = this
            .io_sl2sr
            .as_mut()
            .poll_execute(cx, sl.as_mut(), sr.as_mut());
        let io_sr2sl_ret = this
            .io_sr2sl
            .as_mut()
            .poll_execute(cx, sr.as_mut(), sl.as_mut());

        #[cfg(not(feature = "feat-brutal-shutdown"))]
        {
            match (io_sl2sr_ret, io_sr2sl_ret) {
                (Poll::Pending, _) | (_, Poll::Pending) => Poll::Pending,
                (Poll::Ready(Ok(())), Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
                (Poll::Ready(Err(e)), _) | (_, Poll::Ready(Err(e))) => Poll::Ready(Err(e)),
            }
        }

        #[cfg(feature = "feat-brutal-shutdown")]
        {
            match (io_sl2sr_ret, io_sr2sl_ret) {
                (Poll::Pending, Poll::Pending) => Poll::Pending,
                (Poll::Ready(Err(e)), _) | (_, Poll::Ready(Err(e))) => Poll::Ready(Err(e)),
                // Once received `FIN`, close the other side immediately.
                (Poll::Ready(Ok(())), _) | (_, Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
            }
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
pub trait AsyncReadFd: AsyncRead + AsFd {
    #[doc(hidden)]
    fn poll_read_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    #[doc(hidden)]
    fn try_io_read<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R>;
}

impl<T: AsyncReadFd + Unpin> AsyncReadFd for &mut T {
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
pub trait AsyncWriteFd: AsyncWrite + AsFd {
    #[doc(hidden)]
    fn poll_write_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    #[doc(hidden)]
    fn try_io_write<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R>;
}

impl<T: AsyncWriteFd + Unpin> AsyncWriteFd for &mut T {
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
