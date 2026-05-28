//! Test-only [`AsyncReadFd`]/[`AsyncWriteFd`] that plays back scripted readiness
//! and `try_io` outcomes. Used to drive [`SpliceIo`](super::SpliceIo)'s state
//! machine deterministically against a [`MockSplicer`](crate::splice::mock::MockSplicer).

#![allow(clippy::panic, reason = "test-only assertions")]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{AsyncReadFd, AsyncWriteFd, IsNotFile};

/// Outcome to script for a single `try_io_*` call.
#[derive(Debug)]
#[allow(dead_code, reason = "test API: variants used by future tests")]
pub(crate) enum TryIo {
    /// Invoke the inner closure (which will hit the [`MockSplicer`]).
    CallInner,
    /// Short-circuit with `EWOULDBLOCK`; the closure is not called.
    WouldBlock,
    /// Short-circuit with a specific error kind.
    Err(io::ErrorKind),
}

/// Scripted FD. Holds queues for both directions; only the relevant ones get
/// consumed depending on whether the harness uses it as reader, writer, or both.
///
/// Empty readiness queues return [`Poll::Pending`] (the FD is "idle"); empty
/// `try_io` queues panic, since `try_io_*` is only ever called after readiness
/// returned `Ready`.
#[derive(Debug)]
pub(crate) struct MockFd {
    /// Dummy `OwnedFd` so `AsFd` returns *some* valid descriptor; never read
    /// from or written to by the harness because the backend is also mocked.
    fd: OwnedFd,
    read_ready: RefCell<VecDeque<Poll<io::Result<()>>>>,
    write_ready: RefCell<VecDeque<Poll<io::Result<()>>>>,
    try_io_read: RefCell<VecDeque<TryIo>>,
    try_io_write: RefCell<VecDeque<TryIo>>,
}

impl MockFd {
    pub(crate) fn new() -> io::Result<Self> {
        let fd: OwnedFd = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")?
            .into();
        Ok(Self {
            fd,
            read_ready: RefCell::new(VecDeque::new()),
            write_ready: RefCell::new(VecDeque::new()),
            try_io_read: RefCell::new(VecDeque::new()),
            try_io_write: RefCell::new(VecDeque::new()),
        })
    }

    pub(crate) fn script_read_ready<I>(self, it: I) -> Self
    where
        I: IntoIterator<Item = Poll<io::Result<()>>>,
    {
        self.read_ready.borrow_mut().extend(it);
        self
    }

    pub(crate) fn script_write_ready<I>(self, it: I) -> Self
    where
        I: IntoIterator<Item = Poll<io::Result<()>>>,
    {
        self.write_ready.borrow_mut().extend(it);
        self
    }

    pub(crate) fn script_try_io_read<I: IntoIterator<Item = TryIo>>(self, it: I) -> Self {
        self.try_io_read.borrow_mut().extend(it);
        self
    }

    pub(crate) fn script_try_io_write<I: IntoIterator<Item = TryIo>>(self, it: I) -> Self {
        self.try_io_write.borrow_mut().extend(it);
        self
    }
}

impl AsFd for MockFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl IsNotFile for MockFd {}

// AsyncRead/AsyncWrite stubs: the state machine never calls poll_read/poll_write
// directly. poll_flush/poll_shutdown ARE called (Flushing / Finished states);
// both return Ready(Ok(())).
impl AsyncRead for MockFd {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        panic!("MockFd::poll_read should not be called by the splice state machine")
    }
}

impl AsyncWrite for MockFd {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        panic!("MockFd::poll_write should not be called by the splice state machine")
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncReadFd for MockFd {
    fn poll_read_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.read_ready
            .borrow_mut()
            .pop_front()
            .unwrap_or(Poll::Pending)
    }

    fn try_io_read<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        match self
            .try_io_read
            .borrow_mut()
            .pop_front()
            .expect("MockFd: ran out of scripted try_io_read")
        {
            TryIo::CallInner => f(),
            TryIo::WouldBlock => Err(io::ErrorKind::WouldBlock.into()),
            TryIo::Err(k) => Err(k.into()),
        }
    }
}

impl AsyncWriteFd for MockFd {
    fn poll_write_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write_ready
            .borrow_mut()
            .pop_front()
            .unwrap_or(Poll::Pending)
    }

    fn try_io_write<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        match self
            .try_io_write
            .borrow_mut()
            .pop_front()
            .expect("MockFd: ran out of scripted try_io_write")
        {
            TryIo::CallInner => f(),
            TryIo::WouldBlock => Err(io::ErrorKind::WouldBlock.into()),
            TryIo::Err(k) => Err(k.into()),
        }
    }
}
