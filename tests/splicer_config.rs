//! Tests for the `Splicer` builder API and runtime counters.
//!
//! Covers `with_pipe_size`, `with_target_len`, and the `bytes_read` /
//! `bytes_written` accessors.

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use splicer::Splicer;

mod util;
use util::ListenerPair;

timed_test!(test_splicer_with_pipe_size, {
    // 8192 = two 4 KiB pages, so the kernel won't round it up on any
    // supported arch — we can assert the returned size exactly.
    let splicer: Splicer<TcpStream, TcpStream> =
        Splicer::new().unwrap().with_pipe_size(8192).unwrap();
    assert_eq!(splicer.pipe_size().get(), 8192);

    // 64 KiB is 8× the pipe capacity, so the splice loop has to make
    // multiple fill/drain rounds rather than fitting the payload in one.
    let payload = vec![0xAB_u8; 64 * 1024];
    let expected = payload.clone();

    let out = ListenerPair::new()
        .await
        .run(splicer, |mut s| async move {
            s.write_all(&payload).await.unwrap();
            s.shutdown().await.unwrap();
        })
        .await;

    assert!(
        out.splice.error.is_none(),
        "splice errored: {:?}",
        out.splice.error
    );
    assert_eq!(out.splice.tx, expected.len());
    assert_eq!(out.received, expected);
});

timed_test!(test_splicer_with_pipe_size_below_page_size, {
    let splicer: Splicer<TcpStream, TcpStream> = Splicer::new().unwrap().with_pipe_size(1).unwrap();
    let actual = splicer.pipe_size().get();

    assert!(actual >= 4096, "expected page-rounded size, got {actual}");
    assert_ne!(actual, 1, "pipe_size must report kernel-rounded value");
    assert!(
        actual.is_power_of_two(),
        "pipe size {actual} should be page-aligned"
    );
});

timed_test!(test_splicer_with_pipe_size_above_max, {
    // /proc/sys/fs/pipe-max-size caps unprivileged callers; CAP_SYS_RESOURCE
    // (e.g. root in a default docker container) can exceed it. To make the
    // assertion meaningful regardless of privilege, request a size so large
    // the kernel cannot satisfy it at all — `usize::MAX` is guaranteed to
    // overflow `F_SETPIPE_SZ`'s `int` argument or exhaust memory.
    let result = Splicer::<TcpStream, TcpStream>::new()
        .unwrap()
        .with_pipe_size(usize::MAX);

    assert!(
        result.is_err(),
        "expected Err for usize::MAX pipe size, got Ok({})",
        result.map(|s| s.pipe_size().get()).unwrap_or(0)
    );
});

timed_test!(test_splicer_with_target_len_short_circuit, {
    const TARGET: usize = 16 * 1024;
    // Source will try to push 64× more than the target. If `with_target_len`
    // works, the splicer stops at TARGET regardless of how much is available.
    const PEER_BOUND: usize = 1024 * 1024;

    let splicer: Splicer<TcpStream, TcpStream> = Splicer::new().unwrap().with_target_len(TARGET);

    let out = ListenerPair::new()
        .await
        .run(splicer, |mut s| async move {
            let chunk = vec![0xAB_u8; 4096];
            let mut sent = 0;
            while sent < PEER_BOUND {
                if s.write_all(&chunk).await.is_err() {
                    break;
                }
                sent += chunk.len();
            }
            sent
        })
        .await;

    assert!(
        out.splice.error.is_none(),
        "splice errored: {:?}",
        out.splice.error
    );
    assert_eq!(out.splice.tx, TARGET, "splicer should stop at target_len");
    assert_eq!(
        out.received.len(),
        TARGET,
        "sink should receive exactly target_len"
    );
    assert!(
        out.source >= TARGET,
        "source must have supplied >= target_len, got {}",
        out.source
    );
});

timed_test!(test_splicer_with_target_len_source_eof_first, {
    const TARGET: usize = 16 * 1024;
    const SOURCE_BYTES: usize = 4 * 1024;

    let splicer: Splicer<TcpStream, TcpStream> = Splicer::new().unwrap().with_target_len(TARGET);

    let out = ListenerPair::new()
        .await
        .run(splicer, |mut s| async move {
            s.write_all(&vec![0xCD_u8; SOURCE_BYTES]).await.unwrap();
            s.shutdown().await.unwrap();
        })
        .await;

    assert!(
        out.splice.error.is_none(),
        "splice errored: {:?}",
        out.splice.error
    );
    assert_eq!(
        out.splice.tx, SOURCE_BYTES,
        "splicer should report bytes actually moved"
    );
    assert!(
        out.splice.tx < TARGET,
        "tx ({}) should be < target ({})",
        out.splice.tx,
        TARGET
    );
    assert_eq!(out.received.len(), SOURCE_BYTES);
});
