# Perf testing

## Isolating CPUs

CPU used: `AMD Ryzen 9 7950X (32) @ 5.883G`.

We have set:

`isolcpus=4-15,20-31 nohz_full=4-15,20-31 rcu_nocbs=4-15,20-31`

boot flags to isolate logical cpus, and confirmed with

`cat /sys/devices/system/cpu/isolated`

## Running the servers

There is a just target for building the perf servers, `just build-perf`. They end up in the `target/perf` directory.

## Basic Plan

```
# choose one
PROXY_CMD="env GOMAXPROCS=16 ./proxy-go"
#PROXY_CMD="env TOKIO_WORKER_THREADS=16 ./proxy-rust"
# start iperf server
taskset -c 4,20      iperf3 -s -p 5201
# start proxy
taskset -c 6-13,22-29  ./$PROXY_CMD 5200 5201
# start iperf client (and begin test)
taskset -c 5,21      iperf3 -c 127.0.0.1 -p 5200 -t 30
# optionally watch cpu
taskset -c 14,30     pidstat -u 1 -p $(pgrep splicer) > cpu.log
```