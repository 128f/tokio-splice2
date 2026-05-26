docker := 'docker run --rm -v "$PWD":/work -w /work rust:1-bookworm'
perf_dir := 'target/perf'

default:
    @just --list

build *args:
    {{docker}} cargo build --all-features {{args}}

test *args:
    {{docker}} cargo test --all-features {{args}}

check *args:
    {{docker}} cargo check --all-features {{args}}

clippy *args:
    {{docker}} cargo clippy --all-features --all-targets {{args}} -- -D warnings

fmt:
    cargo fmt

fmt-check:
    cargo fmt --check

shell:
    {{docker}} bash

# === Perf testing (see examples/README.md) ===

# Build the splicer-based Rust proxy into target/perf/proxy-rust
build-perf-rust:
    {{docker}} cargo build --release --all-features --example proxy
    mkdir -p {{perf_dir}}
    cp target/release/examples/proxy {{perf_dir}}/proxy-rust

# Build the Go proxy into target/perf/proxy-go
build-perf-go:
    mkdir -p {{perf_dir}}
    cd examples && go build -o ../{{perf_dir}}/proxy-go proxy.go

# Build both perf proxy binaries
build-perf: build-perf-rust build-perf-go
