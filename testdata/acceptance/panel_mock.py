#!/usr/bin/env python3
import json
import re
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


UUID = "4f8e1c60-2c4b-4b80-8f53-0d34ce5b8082"
state_lock = threading.Lock()
state = {
    "node_info_requests": 0,
    "user_requests": 0,
    "traffic_posts": 0,
    "aliveip_posts": 0,
    "traffic_bytes": 0,
    "alive_ips": [],
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def authorized(self):
        query = parse_qs(urlparse(self.path).query)
        return query.get("key", [""])[0] == "test-key"

    def send_json(self, payload, code=200):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/summary":
            with state_lock:
                self.send_json(dict(state))
            return
        if not self.authorized():
            self.send_json({"ret": 0, "msg": "unauthorized"}, 401)
            return
        node_match = re.fullmatch(r"/mod_mu/nodes/(\d+)/info", parsed.path)
        if node_match:
            with state_lock:
                state["node_info_requests"] += 1
            self.send_json({
                "ret": 1,
                "data": {
                    "node_speedlimit": 0,
                    "sort": 11,
                    "custom_config": {"alter_id": "32"},
                },
            })
            return
        if parsed.path == "/mod_mu/users":
            query = parse_qs(parsed.query)
            node_id = int(query.get("node_id", ["0"])[0])
            with state_lock:
                state["user_requests"] += 1
            self.send_json({
                "ret": 1,
                "data": [{"id": 40 + node_id, "uuid": UUID, "node_speedlimit": 0}],
            })
            return
        self.send_json({"ret": 0, "msg": "not found"}, 404)

    def do_POST(self):
        if not self.authorized():
            self.send_json({"ret": 0, "msg": "unauthorized"}, 401)
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        path = urlparse(self.path).path
        if path == "/mod_mu/users/traffic":
            total = sum(int(item.get("u", 0)) + int(item.get("d", 0)) for item in payload.get("data", []))
            with state_lock:
                state["traffic_posts"] += 1
                state["traffic_bytes"] += total
            self.send_json({"ret": 1, "data": "ok"})
            return
        if path == "/mod_mu/users/aliveip":
            ips = sorted({str(item.get("ip", "")) for item in payload.get("data", []) if item.get("ip")})
            with state_lock:
                state["aliveip_posts"] += 1
                state["alive_ips"] = ips
            self.send_json({"ret": 1, "data": "ok"})
            return
        self.send_json({"ret": 0, "msg": "not found"}, 404)


class PanelServer(ThreadingHTTPServer):
    address_family = socket.AF_INET


if __name__ == "__main__":
    bind_host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    bind_port = int(sys.argv[2]) if len(sys.argv) > 2 else 22000
    PanelServer.address_family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
    PanelServer((bind_host, bind_port), Handler).serve_forever()
