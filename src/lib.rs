#![doc = include_str!("../README.md")]
#![cfg_attr(debug_assertions, allow(clippy::unreachable))]

pub mod splice;
pub mod io;
pub mod pipe;
pub mod traffic;

pub use splice::SpliceCtx;
pub use io::{AsyncReadFd, AsyncWriteFd, IsFile, IsNotFile, SpliceBidiIo, SpliceIo};

#[inline]
/// Copies data from `r` to `w` using `splice(2)`.
///
/// See [`SpliceCtx::new`] and [`SpliceIo::execute`] for more details; see
/// the [crate-level documentation](crate) for known limitations.
///
/// ## Errors
///
/// * Create pipe failed.
pub async fn copy<R, W>(r: &mut R, w: &mut W) -> std::io::Result<traffic::TrafficResult>
where
    R: io::AsyncReadFd + IsNotFile + Unpin,
    W: io::AsyncWriteFd + IsNotFile + Unpin,
{
    Ok(io::SpliceIo::from(splice::SpliceCtx::new()?)
        .execute(r, w)
        .await)
}

#[inline]
/// Copies data from file `r` to `w` using `splice(2)`.
///
/// See [`SpliceCtx::with_input_file`] for more details; see the
/// [crate-level documentation](crate) for known limitations.
///
/// ## Errors
///
/// * Create pipe failed.
/// * Invalid file length or offset.
pub async fn sendfile<R, W>(
    r: &mut R,
    w: &mut W,
    f_len: u64,
    f_offset_start: Option<u64>,
    f_offset_end: Option<u64>,
) -> std::io::Result<traffic::TrafficResult>
where
    R: io::AsyncReadFd + IsFile + Unpin,
    W: io::AsyncWriteFd + IsNotFile + Unpin,
{
    Ok(io::SpliceIo::from(splice::SpliceCtx::with_input_file(
        f_len,
        f_offset_start,
        f_offset_end,
    )?)
    .execute(r, w)
    .await)
}

#[inline]
/// Copies data in both directions between `sl` and `sr`.
///
/// This function returns a future that will read from both streams, writing any
/// data read to the opposing stream. This happens in both directions
/// concurrently.
///
/// See [`SpliceCtx::new`] and [`SpliceBidiIo::execute`] for more details;
/// see the [crate-level documentation](crate) for known limitations.
///
/// ## Errors
///
/// * Create pipe failed.
pub async fn copy_bidirectional<A, B>(
    sl: &mut A,
    sr: &mut B,
) -> std::io::Result<traffic::TrafficResult>
where
    A: io::AsyncReadFd + io::AsyncWriteFd + IsNotFile + Unpin,
    B: io::AsyncReadFd + io::AsyncWriteFd + IsNotFile + Unpin,
{
    Ok(io::SpliceBidiIo {
        io_sl2sr: splice::SpliceCtx::new()?.into(),
        io_sr2sl: splice::SpliceCtx::new()?.into(),
    }
    .execute(sl, sr)
    .await)
}

// === Tracing macros for logging ===

#[allow(unused)]
macro_rules! trace {
    ($($tt:tt)*) => {{
        #[cfg(any(feature = "feat-tracing-trace", all(debug_assertions, feature = "feat-tracing")))]
        tracing::trace!($($tt)*);
    }};
}

#[allow(unused)]
macro_rules! debug {
    ($($tt:tt)*) => {{
        #[cfg(feature = "feat-tracing")]
        tracing::debug!($($tt)*);
    }};
}

#[allow(unused)]
macro_rules! info {
    ($($tt:tt)*) => {{
        #[cfg(feature = "feat-tracing")]
        tracing::info!($($tt)*);
    }};
}

#[allow(unused)]
// Avoid name conflicts with `warn` in the standard library.
macro_rules! warning {
    ($($tt:tt)*) => {{
        #[cfg(feature = "feat-tracing")]
        tracing::warn!($($tt)*);
    }};
}

#[allow(unused)]
macro_rules! error {
    ($($tt:tt)*) => {{
        #[cfg(feature = "feat-tracing")]
        tracing::error!($($tt)*);
    }};
}

#[allow(unused)]
macro_rules! enter_tracing_span {
    ($($tt:tt)*) => {
        #[cfg(any(
            feature = "feat-tracing-trace",
            all(debug_assertions, feature = "feat-tracing")
        ))]
        let _span = tracing::span!(
            tracing::Level::TRACE,
            $($tt)*
        )
        .entered();
    };
}

#[allow(unused)]
pub(crate) use {debug, enter_tracing_span, error, info, trace, warning};
