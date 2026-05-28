# splicer

Async [`splice(2)`] copy primitives for Tokio on Linux.

## What it does

Three async entry points, all returning a [`TrafficResult`](src/traffic.rs)
with bytes-in / bytes-out and the terminating error (if any):

```rust,no_run
use tokio::fs::File;
use tokio::net::TcpStream;

async fn example() -> std::io::Result<()> {
    let mut a = TcpStream::connect("127.0.0.1:0").await?;
    let mut b = TcpStream::connect("127.0.0.1:0").await?;

    // Stream-to-stream, one direction.
    splicer::copy(&mut a, &mut b).await?;

    // Like `tokio::io::copy_bidirectional`, but on splice(2).
    splicer::copy_bidirectional(&mut a, &mut b).await?;

    // sendfile-style: file -> socket, with optional byte range.
    let mut file = File::open("/dev/null").await?;
    splicer::sendfile(&mut file, &mut a, 0, None, None).await?;
    Ok(())
}
```

`R` / `W` must be FDs the kernel will splice — TCP, Unix sockets, and (for
the file entry points) `tokio::fs::File`. The [`AsyncReadFd`] / [`IsFile`]
traits in [`src/io/fd.rs`](src/io/fd.rs) gatekeep this at compile time.

For finer control — custom pipe size, byte caps, reusing the pipe across
calls — drop down to [`SpliceCtx`](src/splice/mod.rs) and
[`SpliceIo`](src/io/mod.rs) directly.

## Layering

- [`src/splice/`](src/splice/) — `SpliceCtx<R, W>`: pure synchronous splice
  mechanics. Owns the pipe, offset, and byte counters. No `Poll` / `Pin` /
  `Context`. Testable without a runtime.
- [`src/io/`](src/io/) — `SpliceIo` and `SpliceBidiIo`: the async state
  machine wrapping `SpliceCtx` and driving the underlying FDs through Tokio's
  reactor.
- [`src/pipe.rs`](src/pipe.rs), [`src/traffic.rs`](src/traffic.rs) — pipe
  RAII and traffic accounting.

## Building and testing

The crate is Linux-only. Local dev goes through the [`Justfile`](Justfile),
which wraps `cargo` in a `rust:1-bookworm` docker container:

```sh
just build           # cargo build --all-features
just test            # cargo test  --all-features
just clippy          # cargo clippy --all-features --all-targets -- -D warnings
just fmt             # cargo fmt (runs on host)
```

## Features

- `feat-tracing` — emit `tracing` events from the splice loop.
- `feat-tracing-trace` — also enable `TRACE`-level events in release builds.

## Caveats inherited from splice(2)

These are properties of the syscall, not the wrapper, and they all still
apply:

- **Page-cache aliasing on file input.** `splice` from a file shares
  references to page-cache pages, much like `mmap`. If the file is modified
  while bytes are still queued in the destination's kernel buffer, the peer
  may see the new contents. The crate takes `&mut R` as a best-effort guard;
  it can't prevent another process from rewriting the file. See
  [lwn.net/Articles/923237] and [rust#116451].
- **Bytes returned ≠ bytes on the wire.** A successful `splice` only means
  the bytes hit the destination FD's kernel buffer. The bidi path issues a
  `poll_flush` at end-of-stream, but a misbehaving `AsyncWrite` impl can
  still defer the actual flush.
- **Small / chatty transfers can lose to read+write.** Per-call overhead and
  pipe-buffer sizing matter; splice isn't a free win on every workload.
- **UDP isn't covered.** `splice` doesn't help; use `sendmmsg` / `recvmmsg`
  or XDP.

## License

MIT OR Apache-2.0, same as upstream. See [LICENSE](LICENSE).

Derived from [hanyu-dev/tokio-splice2] (Apache-2.0 / MIT), itself derived
from [Hanaasagi/tokio-splice]. Thanks to both.

[`splice(2)`]: https://man7.org/linux/man-pages/man2/splice.2.html
[`AsyncReadFd`]: src/io/fd.rs
[`IsFile`]: src/io/fd.rs
[hanyu-dev/tokio-splice2]: https://github.com/hanyu-dev/tokio-splice2
[Hanaasagi/tokio-splice]: https://github.com/Hanaasagi/tokio-splice
[lwn.net/Articles/923237]: https://lwn.net/Articles/923237/
[rust#116451]: https://github.com/rust-lang/rust/issues/116451
