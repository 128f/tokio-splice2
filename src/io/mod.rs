//! `splice(2)` I/O implementation.

mod fd;

pub use fd::{AsyncReadFd, AsyncWriteFd, IsFile, IsNotFile};

use std::future::poll_fn;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{io, ops};

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
                        Ok(_) => TransferState::Drain,
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
