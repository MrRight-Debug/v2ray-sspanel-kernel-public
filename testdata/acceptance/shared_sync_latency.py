#!/usr/bin/env python3
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


STAGE = Path("/opt/v2ray-sspanel-shared-smoke")
FIXTURES = STAGE / "fixtures"
EVIDENCE = STAGE / "evidence"
RUN_OFFSET = (int(time.time()) // 60 % 40) * 100


def wait_port(port, process, timeout=12):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited before port {port}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} did not open")


def get_summary(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/summary", timeout=5) as response:
        return json.loads(response.read().decode())


def run_case(name, binary_name, node_count, index):
    panel_port = 23200 + RUN_OFFSET + index
    server_base = 24300 + RUN_OFFSET + index * 10
    server_ports = [server_base + offset for offset in range(node_count)]
    server_config_name = "shared-server-1.json" if node_count == 1 else "shared-server-4.json"
    data = json.loads((FIXTURES / server_config_name).read_text())
    for inbound, port in zip(data["inbounds"], server_ports):
        inbound["port"] = port
        inbound["settings"]["sspanel"]["url"] = f"http://127.0.0.1:{panel_port}"
    config = Path(f"/tmp/{name}-{os.getpid()}.json")
    config.write_text(json.dumps(data))
    panel = None
    server = None
    try:
        panel_start = time.time()
        panel = subprocess.Popen(
            [sys.executable, str(FIXTURES / "shared_panel_mock.py"), "127.0.0.1", str(panel_port), "300"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        wait_port(panel_port, panel)
        env = os.environ.copy()
        env["V2RAY_VMESS_AEAD_FORCED"] = "false"
        server = subprocess.Popen(
            [str(STAGE / "binaries" / binary_name), "run", "-config", str(config)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        for port in server_ports:
            wait_port(port, server)
        summary = None
        deadline = time.time() + 15
        while time.time() < deadline:
            summary = get_summary(panel_port)
            if summary.get("first_users_epoch") is not None:
                break
            if server.poll() is not None:
                raise RuntimeError("server exited before first user sync")
            time.sleep(0.05)
        if not summary or summary.get("first_users_epoch") is None:
            raise RuntimeError("first user sync did not occur")
        time.sleep(1)
        summary = get_summary(panel_port)
        return {
            "scenario": name,
            "binary": binary_name,
            "nodes": node_count,
            "first_nodeinfo_latency_ms": summary["first_nodeinfo_latency_ms"],
            "first_users_latency_ms": summary["first_users_latency_ms"],
            "users_requests_after_first": summary["users_requests"],
            "users_not_modified_after_first": summary["users_not_modified"],
            "nodeinfo_requests_after_first": summary["nodeinfo_by_node"],
            "wall_latency_ms": round((time.time() - panel_start) * 1000, 1),
        }
    finally:
        for process in (server, panel):
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        if config.exists():
            config.unlink()
        time.sleep(0.4)


def main():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    cases = [
        ("pre-shared-v5.56-1", "v2ray-sspanel-pre-shared-d776c9cc", 1),
        ("pre-shared-v5.56-4", "v2ray-sspanel-pre-shared-d776c9cc", 4),
        ("shared-v5.56-1", "v2ray-sspanel-shared", 1),
        ("shared-v5.56-4", "v2ray-sspanel-shared", 4),
    ]
    results = []
    for index, case in enumerate(cases):
        print(f"sync-latency={case[0]}", flush=True)
        results.append(run_case(*case, index))
    (EVIDENCE / "shared-sync-latency.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(results, ensure_ascii=False))


if __name__ == "__main__":
    main()
