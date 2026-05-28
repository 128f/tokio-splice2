//! `splice(2)` I/O implementation.

mod fd;
#[cfg(test)]
pub(crate) mod mock;

pub use fd::{AsyncReadFd, AsyncWriteFd, IsFile, IsNotFile};

use std::future::poll_fn;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{fmt, io, ops};

use crate::splice::{Live, SpliceCtx, Splicer};
use crate::traffic::TrafficResult;

/// Zero-copy unidirectional I/O with `splice(2)`.
///
/// For bidirectional I/O version, see [`SpliceBidiIo`].
///
/// Notice: see the [module-level documentation](crate) for known limitations.
pub struct SpliceIo<R, W, S = Live> {
    splicer: SpliceCtx<R, W, S>,
    state: TransferState,
}

impl<R, W, S> fmt::Debug for SpliceIo<R, W, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpliceIo")
            .field("splicer", &self.splicer)
            .field("state", &self.state)
            .finish()
    }
}

impl<R, W, S> ops::Deref for SpliceIo<R, W, S> {
    type Target = SpliceCtx<R, W, S>;

    fn deref(&self) -> &Self::Target {
        &self.splicer
    }
}

#[cfg(test)]
impl<R, W, S> SpliceIo<R, W, S> {
    /// Test-only: short name of the current [`TransferState`] discriminant.
    pub(crate) fn state_name(&self) -> &'static str {
        match self.state {
            TransferState::Fill => "Fill",
            TransferState::Drain => "Drain",
            TransferState::Flushing => "Flushing",
            TransferState::Finished { .. } => "Finished",
        }
    }
}

impl<R, W, S> From<SpliceCtx<R, W, S>> for SpliceIo<R, W, S> {
    fn from(splicer: SpliceCtx<R, W, S>) -> Self {
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

impl<R, W, S> SpliceIo<R, W, S>
where
    R: AsyncReadFd,
    W: AsyncWriteFd,
    S: Splicer,
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

/// Bidirectional splice I/O, combining two `SpliceIo` instances.
pub struct SpliceBidiIo<SL, SR, S1 = Live, S2 = Live> {
    /// Splice I/O instance, from `SL` to `SR`.
    pub io_sl2sr: SpliceIo<SL, SR, S1>,

    /// Splice I/O instance, from `SR` to `SL`.
    pub io_sr2sl: SpliceIo<SR, SL, S2>,
}

impl<SL, SR, S1, S2> fmt::Debug for SpliceBidiIo<SL, SR, S1, S2> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpliceBidiIo")
            .field("io_sl2sr", &self.io_sl2sr)
            .field("io_sr2sl", &self.io_sr2sl)
            .finish()
    }
}

impl<SL, SR, S1, S2> SpliceBidiIo<SL, SR, S1, S2>
where
    SL: AsyncReadFd + AsyncWriteFd + IsNotFile,
    SR: AsyncReadFd + AsyncWriteFd + IsNotFile,
    S1: Splicer,
    S2: Splicer,
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

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use super::mock::{MockFd, TryIo};
    use super::{SpliceIo, TransferState};
    use crate::splice::mock::MockSplicer;
    use crate::splice::SpliceCtx;

    fn noop_cx() -> Context<'static> {
        Context::from_waker(std::task::Waker::noop())
    }

    /// Partial write: reader supplies 8000 bytes, writer drains only 3000 in
    /// the first round, then both FDs go idle. 5000 bytes are still buffered
    /// in the pipe, so the state machine must park on the writer (Drain) —
    /// parking on the reader would deadlock once the source has no more to
    /// give.
    #[test]
    fn partial_write_parks_on_writer() {
        // 8k bytes go into the pipe, 3k bytes go out ->
        // 5k bytes are sitting in the pipe, waiting to be drained
        let splicer = MockSplicer::new()
            .script_in([Ok(8000)])
            .script_out([Ok(3000)]);

        // cause the reader to report it's ready,
        // then call the splicer, consuming the first 8k bytes
        let mut reader = MockFd::new()
            .unwrap()
            .script_read_ready([Poll::Ready(Ok(()))])
            .script_try_io_read([TryIo::CallInner]);

        // cause the reader to report it's ready,
        // then call the splicer, writing the 3k bytes
        // the pipe should now contain the 5k bytes
        let mut writer = MockFd::new()
            .unwrap()
            .script_write_ready([Poll::Ready(Ok(()))])
            .script_try_io_write([TryIo::CallInner]);

        // run a SpliceIo state machine with the above scripted events
        let ctx: SpliceCtx<MockFd, MockFd, MockSplicer> =
            SpliceCtx::new_with_splicer(splicer).unwrap();
        let mut io: SpliceIo<MockFd, MockFd, MockSplicer> = ctx.into();

        // poll with the dummy waker
        // state machine should begin spinning
        // and the task will end up parked on the writer,
        // since the pipe is not empty
        let mut cx = noop_cx();
        let poll = io.poll_execute(&mut cx, &mut reader, &mut writer);

        assert!(matches!(poll, Poll::Pending));
        assert_eq!(io.bytes_read(), 8000);
        assert_eq!(io.bytes_written(), 3000);
        assert_eq!(io.state_name(), "Drain");
    }

    /// Clean drain in one poll: full splice in, full splice out, EOF.
    #[test]
    fn clean_drain_finishes() {
        // read 1000 bytes into the pipe, then EOF (0 bytes) ->
        // then write the 1000 bytes out in one shot
        let splicer = MockSplicer::new()
            .script_in([Ok(1000), Ok(0)])
            .script_out([Ok(1000)]);

        // reader reports ready, once for read and once for EOF
        // then allow the splice to go through twice
        let mut reader = MockFd::new()
            .unwrap()
            .script_read_ready([Poll::Ready(Ok(())), Poll::Ready(Ok(()))])
            .script_try_io_read([TryIo::CallInner, TryIo::CallInner]);

        // Report ready for read twice,
        // and allow two splice operations,
        // the second splice operation will not actually happen,
        // since the meter will report all bytes have been read
        let mut writer = MockFd::new()
            .unwrap()
            .script_write_ready([Poll::Ready(Ok(())), Poll::Ready(Ok(()))])
            .script_try_io_write([TryIo::CallInner, TryIo::CallInner]);

        // script and confirm we reach a Finished state
        // due to the EOF, and the meter reports all bytes read/written correctly
        let ctx: SpliceCtx<MockFd, MockFd, MockSplicer> =
            SpliceCtx::new_with_splicer(splicer).unwrap();
        let mut io: SpliceIo<MockFd, MockFd, MockSplicer> = ctx.into();

        let mut cx = noop_cx();
        let poll = io.poll_execute(&mut cx, &mut reader, &mut writer);

        assert!(matches!(poll, Poll::Ready(Ok(()))));
        assert_eq!(io.bytes_read(), 1000);
        assert_eq!(io.bytes_written(), 1000);
        assert!(matches!(io.state, TransferState::Finished { error: None }));
    }
}
