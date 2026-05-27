#!/usr/bin/env python3
"""Perf harness: runs the README's tests, randomly interleaved.

Usage: ./run.py [repeats]   (repeats = how many times each artifact runs; default 3)
Config via env: DURATION, COOLDOWN, LOG_DIR, LOG_TAG, PROXY_GO, PROXY_RUST, PROXY_UPSTREAM.

Logs (LOG_TAG defaults to today's date):
  perf-<tag>.log             harness events (start/markers/cooldown/done)
  perf-<tag>-<artifact>.log  iperf client output for that artifact
  cpu-<tag>-<artifact>.log   pidstat samples for that artifact's proxy
"""
import os
import random
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

# ---- config ----
REPEATS = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("REPEATS", 3))
DURATION = int(os.environ.get("DURATION", 30))   # seconds per iperf run
COOLDOWN = int(os.environ.get("COOLDOWN", 30))   # seconds between tests
LOG_DIR = os.environ.get("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
TAG = os.environ.get("LOG_TAG", f"{datetime.now():%Y-%m-%d}")
LOG = os.path.join(LOG_DIR, f"perf-{TAG}.log")   # harness events; per-artifact data lives alongside

PROXY_GO = os.environ.get("PROXY_GO", "./target/perf/proxy-go")
PROXY_RUST = os.environ.get("PROXY_RUST", "./target/perf/proxy-rust")
PROXY_UPSTREAM = os.environ.get("PROXY_UPSTREAM", "./target/perf/proxy-upstream")

# CPU pinning + ports, straight from the README
IPERF_SRV_CPUS = "4,20"
IPERF_CLI_CPUS = "5,21"
PROXY_CPUS = "6,22"
CPU_MON_CPUS = "14,30"   # where pidstat itself runs (isolated, off the test path)
IPERF_PORT = 5201   # iperf server
PROXY_PORT = 5200   # proxy front door

ARTIFACTS = ["baseline", "golang", "splicer", "tokio-splice2"]

HAVE_PIDSTAT = shutil.which("pidstat") is not None


def log(msg):
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def perf_log(artifact):
    return os.path.join(LOG_DIR, f"perf-{TAG}-{artifact}.log")


def cpu_log(artifact):
    return os.path.join(LOG_DIR, f"cpu-{TAG}-{artifact}.log")


def spawn(cmd, env=None):
    """Start a background process in its own group so we can kill it cleanly."""
    return subprocess.Popen(
        cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def kill(procs):
    for p in procs:
        if p.poll() is None:
            os.killpg(p.pid, signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)


def start_cpu_monitor(artifact, pid):
    """pidstat the proxy for the test's duration, output -> cpu_log(artifact).

    Runs DURATION 1s samples then prints an Average line and exits on its own,
    so it lines up with the iperf client run. Pinned to CPU_MON_CPUS so the
    monitor never competes with the proxy under test.
    """
    if not HAVE_PIDSTAT:
        return None, None
    f = open(cpu_log(artifact), "a")
    f.write(f"\n=== {datetime.now(timezone.utc).isoformat()} {artifact} pid={pid} ===\n")
    f.flush()
    proc = subprocess.Popen(
        ["taskset", "-c", CPU_MON_CPUS, "pidstat", "-u", "1", str(DURATION), "-p", str(pid)],
        stdout=f, stderr=subprocess.STDOUT, start_new_session=True,
    )
    return proc, f


def run_test(artifact):
    """Start backends, run the client (logged), tear backends down."""
    procs = [spawn(["taskset", "-c", IPERF_SRV_CPUS, "iperf3", "-s", "-p", str(IPERF_PORT)])]
    proxy = None
    port = PROXY_PORT
    if artifact == "baseline":
        port = IPERF_PORT
    elif artifact == "golang":
        proxy = spawn(["taskset", "-c", PROXY_CPUS, PROXY_GO, str(PROXY_PORT), str(IPERF_PORT)])
    elif artifact == "splicer":
        proxy = spawn(["taskset", "-c", PROXY_CPUS, PROXY_RUST, str(PROXY_PORT), str(IPERF_PORT)])
    elif artifact == "tokio-splice2":
        env = {**os.environ,
               "EXAMPLE_LISTEN_ADDR": f"0.0.0.0:{PROXY_PORT}",
               "EXAMPLE_REMOTE_ADDR": f"127.0.0.1:{IPERF_PORT}"}
        proxy = spawn(["taskset", "-c", PROXY_CPUS, PROXY_UPSTREAM], env=env)
    if proxy is not None:
        procs.append(proxy)

    cpu_proc = cpu_file = None
    try:
        time.sleep(1)  # let server + proxy bind
        if proxy is not None and proxy.poll() is None:
            cpu_proc, cpu_file = start_cpu_monitor(artifact, proxy.pid)
        subprocess.run(
            ["taskset", "-c", IPERF_CLI_CPUS, "iperf3", "-c", "127.0.0.1",
             "-p", str(port), "-t", str(DURATION), "--logfile", perf_log(artifact)],
        )
    finally:
        if cpu_proc is not None and cpu_proc.poll() is None:
            try:
                cpu_proc.wait(timeout=5)  # let it print its Average line
            except subprocess.TimeoutExpired:
                os.killpg(cpu_proc.pid, signal.SIGTERM)
        if cpu_file is not None:
            cpu_file.close()
        kill(procs)


def main():
    queue = [a for a in ARTIFACTS for _ in range(REPEATS)]
    random.shuffle(queue)

    log(f"starting: {len(queue)} tests, {DURATION}s each, {COOLDOWN}s cooldown -> {LOG}")
    log(f"logs: per-artifact iperf -> perf-{TAG}-<artifact>.log in {LOG_DIR}")
    if HAVE_PIDSTAT:
        log(f"cpu: pidstat on proxy (cpus {CPU_MON_CPUS}) -> cpu-{TAG}-<artifact>.log")
    else:
        log("cpu: pidstat not found on PATH, skipping CPU measurement")
    for i, artifact in enumerate(queue):
        log(f"=== test {i + 1}/{len(queue)}: {artifact} ===")
        run_test(artifact)
        if i + 1 < len(queue):
            log(f"--- cooldown {COOLDOWN}s ---")
            time.sleep(COOLDOWN)
    log("done")


if __name__ == "__main__":
    main()
