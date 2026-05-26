# Perf testing

Today's date is: May 27, 2026

## Goal

Goal: understand what baseline performance looks like, and ensure this fork has not regressed performance.

## Methodology

We perform each 30s test 3 times in a row and write everything to the log.

Then we wait 30s in between.

## Artifacts being tested

We want to test several artifacts, and they are tested in this order according to the above method:

* baseline - iperf to iperf
  - Run `iperf3 -s -p 5201` and `iperf3 -c 127.0.0.1 -p 5201 -t 30` for a theoretical maximum throughput.

* golang - Run the simple GoLang example in `examples`

* splicer - Run the proxy server in examples/proxy.rs

* tokio-splice2 - Run upstream's version of examples/proxy.rs to see if we regressed


## Versions

```
> cargo --version
cargo 1.95.0 (f2d3ce0bd 2026-03-21)
> cargo +nightly --version
cargo 1.98.0-nightly (4d1f98451 2026-05-15)
> go version
go version go1.26.1-X:nodwarf5 linux/amd64
```

## Isolating CPUs

CPU used: `AMD Ryzen 9 7950X (32) @ 5.883G`.

We have set:

`isolcpus=4-15,20-31 nohz_full=4-15,20-31 rcu_nocbs=4-15,20-31`

boot flags to isolate logical cpus, and confirmed with

`cat /sys/devices/system/cpu/isolated`

## Running the servers

There is a just target for building the proxy servers: `just build-perf`.

They end up in the `target/perf` directory.

## Basic Plan

Below commands are outlined for just proxy-go and proxy-rust.
Initially, 16 threads were used, but we found that it had a negative performance impact, since our iperf3 test only uses one long-lived connection.

```
# go version
export PROXY_CMD="./target/perf/proxy-go"
#
export PROXY_CMD="./target/perf/proxy-rust"

# start iperf server
taskset -c 4,20      iperf3 -s -p 5201
# start proxy
taskset -c 6,22  $PROXY_CMD 5200 5201
# start iperf client (and begin test)
taskset -c 5,21      iperf3 -c 127.0.0.1 -p 5200 -t 30
# optionally watch cpu
taskset -c 14,30     pidstat -u 1 -p $(pgrep splicer) > cpu.log
```