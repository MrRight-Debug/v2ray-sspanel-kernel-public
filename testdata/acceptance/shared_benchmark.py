#!/usr/bin/env python3
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


STAGE = Path("/opt/v2ray-sspanel-shared-smoke")
BINARIES = STAGE / "binaries"
FIXTURES = STAGE / "fixtures"
EVIDENCE = STAGE / "evidence"
MOCK_PORT = 23000
ECHO_PORT = 19090
DURATION = 30
RUN_OFFSET = (int(time.time()) // 60 % 60) * 100

SCENARIOS = [
    ("old-v5.53-1", "v2ray-sspanel-old-76940c86", "shared-server-1.json", "shared-client-1.json", 1, False),
    ("old-v5.53-4", "v2ray-sspanel-old-76940c86", "shared-server-4.json", "shared-client-4.json", 4, False),
    ("pre-shared-v5.56-1", "v2ray-sspanel-pre-shared-d776c9cc", "shared-server-1.json", "shared-client-1.json", 1, False),
    ("pre-shared-v5.56-4", "v2ray-sspanel-pre-shared-d776c9cc", "shared-server-4.json", "shared-client-4.json", 4, False),
    ("shared-v5.56-1", "v2ray-sspanel-shared", "shared-server-1.json", "shared-client-1.json", 1, False),
    ("shared-v5.56-4", "v2ray-sspanel-shared", "shared-server-4.json", "shared-client-4.json", 4, False),
    ("shared-profile-v5.56-1", "v2ray-sspanel-shared-pprof", "shared-server-1.json", "shared-client-1.json", 1, True),
    ("shared-profile-v5.56-4", "v2ray-sspanel-shared-pprof", "shared-server-4.json", "shared-client-4.json", 4, True),
]


def wait_port(port, process=None, timeout=12):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"process exited before port {port}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {port} did not open")


def read_int(path):
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def proc_status(pid):
    values = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            values[key] = value.strip()
    return values


def fd_count(pid):
    return len(list(Path(f"/proc/{pid}/fd").iterdir()))


def connections(pid):
    result = subprocess.run(
        ["ss", "-Htnp", "state", "established"],
        text=True,
        capture_output=True,
        check=False,
    )
    marker = f"pid={pid},"
    return sum(1 for line in result.stdout.splitlines() if marker in line)


def read_cgroup(cgroup):
    root = Path("/sys/fs/cgroup") / cgroup.lstrip("/")
    cpu_usage = None
    try:
        for line in (root / "cpu.stat").read_text().splitlines():
            if line.startswith("usage_usec "):
                cpu_usage = int(line.split()[1])
                break
    except OSError:
        pass
    return {
        "memory_current": read_int(root / "memory.current"),
        "memory_peak": read_int(root / "memory.peak"),
        "memory_swap_current": read_int(root / "memory.swap.current"),
        "cpu_usage_usec": cpu_usage,
    }


def go_metrics(enabled, port):
    if not enabled:
        return None
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/debug/go-metrics", timeout=5) as response:
        return json.loads(response.read().decode())


def panel_summary(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/summary", timeout=5) as response:
        return json.loads(response.read().decode())


def traffic_worker(stop_event, socks_ports, echo_port):
    index = 0
    while not stop_event.is_set():
        port = socks_ports[index % len(socks_ports)]
        index += 1
        subprocess.run(
            [
                "curl", "-fsS", "--max-time", "5", "--socks5-hostname",
                f"127.0.0.1:{port}", f"http://127.0.0.1:{echo_port}/",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=7,
        )


def run_scenario(
    name,
    binary_name,
    server_config,
    client_config,
    node_count,
    profile,
    scenario_index,
    extra_env=None,
):
    binary = BINARIES / binary_name
    panel_port = 23000 + RUN_OFFSET + scenario_index
    echo_port = 19000 + RUN_OFFSET + scenario_index
    server_base = 24100 + RUN_OFFSET + scenario_index * 10
    socks_base = 26100 + RUN_OFFSET + scenario_index * 10
    metrics_port = 25000 + RUN_OFFSET + scenario_index
    server_ports = [server_base + index for index in range(node_count)]
    socks_ports = [socks_base + index for index in range(node_count)]
    server_data = json.loads((FIXTURES / server_config).read_text())
    for index, inbound in enumerate(server_data["inbounds"]):
        inbound["port"] = server_ports[index]
        inbound["settings"]["sspanel"]["url"] = f"http://127.0.0.1:{panel_port}"
    client_data = json.loads((FIXTURES / client_config).read_text())
    for index, inbound in enumerate(client_data["inbounds"]):
        inbound["port"] = socks_ports[index]
    for index, outbound in enumerate(client_data["outbounds"]):
        outbound["settings"]["vnext"][0]["port"] = server_ports[index]
    server_cfg = Path(f"/tmp/{name}-server-{os.getpid()}.json")
    client_cfg = Path(f"/tmp/{name}-client-{os.getpid()}.json")
    server_cfg.write_text(json.dumps(server_data))
    client_cfg.write_text(json.dumps(client_data))
    unit = f"codex-shared-bench-{name.replace('_', '-')}-{int(time.time())}".lower()
    processes = []
    executor = None
    stop_event = threading.Event()
    try:
        panel = subprocess.Popen(
            [sys.executable, str(FIXTURES / "shared_panel_mock.py"), "127.0.0.1", str(panel_port), "300"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        processes.append(panel)
        wait_port(panel_port, panel)
        echo = subprocess.Popen(
            [sys.executable, str(FIXTURES / "shared_slow_echo.py"), "127.0.0.1", str(echo_port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        processes.append(echo)
        wait_port(echo_port, echo)

        service_cmd = [
            "systemd-run", f"--unit={unit}", "--collect", "--no-block",
            "--property=MemoryAccounting=yes", "--property=TasksAccounting=yes",
            "--setenv=V2RAY_VMESS_AEAD_FORCED=false",
        ]
        for key, value in (extra_env or {}).items():
            service_cmd.append(f"--setenv={key}={value}")
        if profile:
            service_cmd.append(f"--setenv=V2RAY_DEBUG_METRICS_ADDR=127.0.0.1:{metrics_port}")
        service_cmd.extend([str(binary), "run", "-config", str(server_cfg)])
        subprocess.run(service_cmd, check=True, capture_output=True, text=True, timeout=10)

        for port in server_ports:
            wait_port(port, timeout=15)
        if profile:
            wait_port(metrics_port, timeout=15)

        pid = int(subprocess.check_output(
            ["systemctl", "show", "-p", "MainPID", "--value", unit],
            text=True,
        ).strip())
        cgroup = subprocess.check_output(
            ["systemctl", "show", "-p", "ControlGroup", "--value", unit],
            text=True,
        ).strip()
        if pid <= 0 or not cgroup:
            raise RuntimeError("transient service did not expose PID/cgroup")

        client = subprocess.Popen(
            [
                "env", "-u", "HTTP_PROXY", "-u", "HTTPS_PROXY", "-u", "ALL_PROXY",
                "-u", "http_proxy", "-u", "https_proxy", "-u", "all_proxy",
                "V2RAY_VMESS_AEAD_FORCED=false",
                str(BINARIES / "v2ray-test-client-v5.56.0"), "run", "-config", str(client_cfg),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        processes.append(client)
        for port in socks_ports:
            wait_port(port, client)

        time.sleep(8)
        executor = ThreadPoolExecutor(max_workers=len(socks_ports) * 4)
        for _ in range(len(socks_ports) * 4):
            executor.submit(traffic_worker, stop_event, socks_ports, echo_port)

        samples = []
        started = time.time()
        for step in range(0, DURATION + 1, 5):
            elapsed = time.time() - started
            if elapsed < step:
                time.sleep(step - elapsed)
            status = proc_status(pid)
            samples.append({
                "elapsed": step,
                "rss_kib": int(status.get("VmRSS", "0 kB").split()[0]),
                "vm_hwm_kib": int(status.get("VmHWM", "0 kB").split()[0]),
                "threads": int(status.get("Threads", "0")),
                "fds": fd_count(pid),
                "connections": connections(pid),
                "cgroup": read_cgroup(cgroup),
                "go": go_metrics(profile, metrics_port),
            })

        stop_event.set()
        if executor is not None:
            executor.shutdown(wait=True)
            executor = None
        summary = panel_summary(panel_port)
        logs = subprocess.run(
            ["journalctl", "-u", unit, "--no-pager", "-o", "cat"],
            text=True,
            capture_output=True,
            check=False,
        ).stdout
        return {
            "scenario": name,
            "binary": binary_name,
            "server_config": server_config,
            "profile": profile,
            "pid": pid,
            "cgroup_path": cgroup,
            "samples": samples,
            "panel": summary,
            "panic": logs.lower().count("panic"),
            "fatal": logs.lower().count("fatal"),
            "sspanel_failure": logs.lower().count("sspanel: failed") + logs.lower().count("failed to sync users"),
        }
    finally:
        stop_event.set()
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        subprocess.run(["systemctl", "stop", unit], check=False, capture_output=True, text=True)
        subprocess.run(["systemctl", "reset-failed", unit], check=False, capture_output=True, text=True)
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for config in (server_cfg, client_cfg):
            if config.exists():
                config.unlink()
        time.sleep(0.5)


def main():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    results = []
    for scenario_index, scenario in enumerate(SCENARIOS):
        print(f"scenario={scenario[0]}", flush=True)
        results.append(run_scenario(*scenario, scenario_index))
        with (EVIDENCE / "benchmark-results.partial.json").open("w", encoding="utf-8") as handle:
            json.dump(results, handle, ensure_ascii=False, indent=2)
    (EVIDENCE / "benchmark-results.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print("benchmark=PASS", flush=True)


if __name__ == "__main__":
    main()
