docker := 'docker run --rm -v "$PWD":/work -w /work rust:1-bookworm'

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
