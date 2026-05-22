//! Resource-leak regression test: long-running splice loops must not
//! accumulate file descriptors.
//!
//! Tokio opens fds lazily (reactor epoll, eventfd, worker threads, DNS, …),
//! so a naive before/after snapshot will false-positive on warmup. We warm
//! the runtime, then assert growth does not scale with iteration count: a
//! real per-iteration leak would make Δ(10k) ≈ 10 × Δ(1k).

use std::fs;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const PAYLOAD: &[u8] = b"hello";

fn fd_count() -> usize {
    fs::read_dir("/proc/self/fd").unwrap().count()
}

async fn copy_iteration() {
    let (mut a, mut b) = UnixStream::pair().unwrap();
    let (mut c, mut d) = UnixStream::pair().unwrap();

    let writer = tokio::spawn(async move {
        a.write_all(PAYLOAD).await.unwrap();
        a.shutdown().await.unwrap();
    });

    let reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        d.read_to_end(&mut buf).await.unwrap();
        buf
    });

    let result = splicer::copy(&mut b, &mut c).await.unwrap();

    writer.await.unwrap();
    let received = reader.await.unwrap();

    assert_eq!(result.tx, PAYLOAD.len());
    assert_eq!(received.as_slice(), PAYLOAD);
}

#[tokio::test]
async fn test_does_not_leak_pipe_fds() {
    tokio::time::timeout(Duration::from_secs(60), async {
        // Warm up so reactor / DNS / worker-thread fds are already accounted for.
        for _ in 0..100 {
            copy_iteration().await;
        }

        let after_warmup = fd_count();

        for _ in 0..1_000 {
            copy_iteration().await;
        }
        let after_1k = fd_count();

        for _ in 0..10_000 {
            copy_iteration().await;
        }
        let after_10k = fd_count();

        let growth_1k = after_1k.saturating_sub(after_warmup);
        let growth_10k = after_10k.saturating_sub(after_1k);

        eprintln!(
            "fds: warmup={after_warmup}, +1k={after_1k} (Δ{growth_1k}), +10k={after_10k} (Δ{growth_10k})"
        );

        // A real per-iteration leak would make growth_10k ≈ 10 × growth_1k.
        // Allow a small constant for late-allocated runtime fds; flag any
        // growth that scales with iteration count.
        assert!(
            growth_10k <= 10,
            "fd count grew by {growth_10k} over 10k iters (1k iters grew by {growth_1k}) — likely a leak"
        );
    })
    .await
    .expect("test timed out");
}
