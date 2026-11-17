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
SERVER = STAGE / "binaries" / "v2ray-sspanel-shared"
CLIENT = STAGE / "binaries" / "v2ray-test-client-v5.56.0"
RUN_OFFSET = (int(time.time()) // 60 % 40) * 100


def wait_port(port, host="127.0.0.1", process=None, timeout=12):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"process exited before port {port}")
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {port} did not open")


def summary(port, ipv6=False):
    host = "[::1]" if ipv6 else "127.0.0.1"
    with urllib.request.urlopen(f"http://{host}:{port}/summary", timeout=5) as response:
        return json.loads(response.read().decode())


def main():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    echo_v4 = 18000 + RUN_OFFSET
    echo_v6 = 18010 + RUN_OFFSET
    slow = 18020 + RUN_OFFSET
    panel_v4 = 22000 + RUN_OFFSET
    panel_v6 = 22010 + RUN_OFFSET
    server_base = 21000 + RUN_OFFSET
    socks_base = 11200 + RUN_OFFSET
    server_ports = [server_base + index for index in range(4)]
    socks_ports = [socks_base + index for index in range(4)]
    processes = []
    configs = []
    try:
        static_data = json.loads((FIXTURES / "static-server.json").read_text())
        for index in range(2):
            static_data["inbounds"][index]["port"] = server_ports[index]
        dynamic_v4 = json.loads((FIXTURES / "sspanel-server.json").read_text())
        dynamic_v4["inbounds"][0]["port"] = server_ports[2]
        dynamic_v4["inbounds"][0]["settings"]["sspanel"]["url"] = f"http://127.0.0.1:{panel_v4}"
        dynamic_v6 = json.loads((FIXTURES / "sspanel-server-ipv6.json").read_text())
        dynamic_v6["inbounds"][0]["port"] = server_ports[3]
        dynamic_v6["inbounds"][0]["settings"]["sspanel"]["url"] = f"http://[::1]:{panel_v6}"
        client_data = json.loads((FIXTURES / "client.json").read_text())
        for index in range(4):
            client_data["inbounds"][index]["port"] = socks_ports[index]
            client_data["outbounds"][index]["settings"]["vnext"][0]["port"] = server_ports[index]

        for name, data in (
            ("static", static_data),
            ("dynamic-v4", dynamic_v4),
            ("dynamic-v6", dynamic_v6),
            ("client", client_data),
        ):
            path = Path(f"/tmp/shared-compat-{name}-{os.getpid()}.json")
            path.write_text(json.dumps(data))
            configs.append(path)

        commands = [
            [sys.executable, "-m", "http.server", str(echo_v4), "--bind", "127.0.0.1"],
            [sys.executable, "-m", "http.server", str(echo_v6), "--bind", "::1"],
            [sys.executable, str(FIXTURES / "shared_slow_echo.py"), "127.0.0.1", str(slow)],
            [sys.executable, str(FIXTURES / "panel_mock.py"), "127.0.0.1", str(panel_v4)],
            [sys.executable, str(FIXTURES / "panel_mock.py"), "::1", str(panel_v6)],
        ]
        for command in commands:
            process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            processes.append(process)
        for port, host in (
            (echo_v4, "127.0.0.1"),
            (echo_v6, "::1"),
            (slow, "127.0.0.1"),
            (panel_v4, "127.0.0.1"),
            (panel_v6, "::1"),
        ):
            wait_port(port, host)

        env = os.environ.copy()
        env["V2RAY_VMESS_AEAD_FORCED"] = "false"
        for path in configs[:3]:
            process = subprocess.Popen(
                [str(SERVER), "run", "-config", str(path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            processes.append(process)
        for port, host in zip(server_ports, ("127.0.0.1", "::1", "127.0.0.1", "::1")):
            wait_port(port, host)
        client = subprocess.Popen(
            [str(CLIENT), "run", "-config", str(configs[3])],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        processes.append(client)
        for port in socks_ports:
            wait_port(port)

        def curl(socks_port, url):
            return subprocess.run(
                ["curl", "-g", "-fsS", "--max-time", "12", "--socks5-hostname", f"127.0.0.1:{socks_port}", url],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )

        results = {
            "alterid0_ipv4": curl(socks_ports[0], f"http://127.0.0.1:{echo_v4}/").returncode == 0,
            "alterid32_ipv6": curl(socks_ports[1], f"http://[::1]:{echo_v6}/").returncode == 0,
            "sspanel_v4": curl(socks_ports[2], f"http://127.0.0.1:{slow}/").returncode == 0,
            "sspanel_v6": curl(socks_ports[3], f"http://127.0.0.1:{slow}/").returncode == 0,
        }
        time.sleep(2)
        panel4 = summary(panel_v4)
        panel6 = summary(panel_v6, ipv6=True)
        results.update({
            "panel_v4": panel4,
            "panel_v6": panel6,
            "result": "PASS" if all(results[key] for key in ("alterid0_ipv4", "alterid32_ipv6", "sspanel_v4", "sspanel_v6")) else "FAIL",
        })
        (EVIDENCE / "shared-compat.json").write_text(
            json.dumps(results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(results, ensure_ascii=False))
        if results["result"] != "PASS":
            raise RuntimeError("shared compatibility smoke failed")
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for config in configs:
            if config.exists():
                config.unlink()


if __name__ == "__main__":
    main()
