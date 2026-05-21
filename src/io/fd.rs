//! File-descriptor classification traits + impls for the concrete I/O types
//! supported by [`SpliceIo`](super::SpliceIo).

use std::io;
use std::os::fd::AsFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, Interest};
use tokio::net::{TcpStream, UnixStream};

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
