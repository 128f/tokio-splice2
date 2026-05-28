//! Shared test helpers.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use splicer::traffic::TrafficResult;
use splicer::{SpliceIo, SpliceCtx};

pub(crate) const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Defines a `#[tokio::test]` whose body is wrapped in `tokio::time::timeout`.
/// Without an explicit `Duration`, uses [`TEST_TIMEOUT`].
///
/// ```ignore
/// timed_test!(my_test, { /* async body */ });
/// timed_test!(my_test, Duration::from_secs(2), { /* async body */ });
/// ```
#[macro_export]
macro_rules! timed_test {
    ($name:ident, $body:block) => {
        $crate::timed_test!($name, $crate::util::TEST_TIMEOUT, $body);
    };
    ($name:ident, $timeout:expr, $body:block) => {
        #[tokio::test]
        async fn $name() {
            tokio::time::timeout($timeout, async $body)
                .await
                .expect(concat!("test ", stringify!($name), " timed out"));
        }
    };
}

/// A pair of loopback listeners with a splicer wired between them.
///
/// The caller supplies a `source` closure that drives the upstream side
/// (typically writing some payload and shutting down). The downstream side
/// is always read to EOF.
pub(crate) struct ListenerPair {
    upstream: TcpListener,
    downstream: TcpListener,
    pub(crate) upstream_addr: SocketAddr,
    pub(crate) downstream_addr: SocketAddr,
}

pub(crate) struct RunOutput<T> {
    pub(crate) splice: TrafficResult,
    pub(crate) received: Vec<u8>,
    pub(crate) source: T,
}

impl ListenerPair {
    pub(crate) async fn new() -> Self {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let downstream_addr = downstream.local_addr().unwrap();
        Self {
            upstream,
            downstream,
            upstream_addr,
            downstream_addr,
        }
    }

    pub(crate) async fn run<F, Fut, T>(
        self,
        splicer: SpliceCtx<TcpStream, TcpStream>,
        source: F,
    ) -> RunOutput<T>
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send,
        T: Send + 'static,
    {
        let upstream_addr = self.upstream_addr;
        let downstream_addr = self.downstream_addr;

        let source_task = tokio::spawn(async move {
            let s = TcpStream::connect(upstream_addr).await.unwrap();
            source(s).await
        });

        let sink_task = tokio::spawn(async move {
            let mut s = TcpStream::connect(downstream_addr).await.unwrap();
            let mut buf = Vec::new();
            s.read_to_end(&mut buf).await.unwrap();
            buf
        });

        let (r_res, w_res) = tokio::join!(self.upstream.accept(), self.downstream.accept());
        let (mut r, _) = r_res.unwrap();
        let (mut w, _) = w_res.unwrap();

        let splice = SpliceIo::from(splicer).execute(&mut r, &mut w).await;

        // Close both sides so a still-writing source observes EPIPE and exits.
        // Harmless for sources that already shut down on their own.
        drop(r);
        drop(w);

        let source = source_task.await.unwrap();
        let received = sink_task.await.unwrap();

        RunOutput {
            splice,
            received,
            source,
        }
    }
}
