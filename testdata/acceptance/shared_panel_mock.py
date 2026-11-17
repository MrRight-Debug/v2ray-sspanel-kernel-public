#!/usr/bin/env python3
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import sys
import time


USER_COUNT = int(sys.argv[3]) if len(sys.argv) > 3 else 300
USERS = [
    {
        "id": index,
        "uuid": str(uuid.UUID(int=index)),
        "node_speedlimit": 0,
    }
    for index in range(1, USER_COUNT + 1)
]
USERS_BODY = json.dumps({"ret": 1, "data": USERS}, separators=(",", ":")).encode()
ETAG = '"shared-users-v1"'
STARTED_AT = time.time()
state = {
    "users_requests": 0,
    "users_not_modified": 0,
    "users_by_node": {},
    "nodeinfo_by_node": {},
    "traffic_posts": 0,
    "aliveip_posts": 0,
    "first_users_epoch": None,
    "last_users_epoch": None,
    "first_users_latency_ms": None,
    "first_nodeinfo_epoch": None,
    "first_nodeinfo_latency_ms": None,
}
state_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def send_json(self, payload, code=200, headers=None):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/summary":
            with state_lock:
                self.send_json(dict(state))
            return
        if parsed.path.startswith("/mod_mu/nodes/") and parsed.path.endswith("/info"):
            node_id = parsed.path.split("/")[3]
            now = time.time()
            with state_lock:
                if state["first_nodeinfo_epoch"] is None:
                    state["first_nodeinfo_epoch"] = now
                    state["first_nodeinfo_latency_ms"] = round((now - STARTED_AT) * 1000, 1)
                state["nodeinfo_by_node"][node_id] = state["nodeinfo_by_node"].get(node_id, 0) + 1
            self.send_json({
                "ret": 1,
                "data": {"sort": 11, "custom_config": {"alter_id": "32"}},
            })
            return
        if parsed.path == "/mod_mu/users":
            node_id = parse_qs(parsed.query).get("node_id", ["0"])[0]
            now = time.time()
            with state_lock:
                if state["first_users_epoch"] is None:
                    state["first_users_epoch"] = now
                    state["first_users_latency_ms"] = round((now - STARTED_AT) * 1000, 1)
                state["last_users_epoch"] = now
                state["users_requests"] += 1
                state["users_by_node"][node_id] = state["users_by_node"].get(node_id, 0) + 1
            if self.headers.get("If-None-Match") == ETAG:
                with state_lock:
                    state["users_not_modified"] += 1
                self.send_response(304)
                self.send_header("ETag", ETAG)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(USERS_BODY)))
            self.send_header("ETag", ETAG)
            self.end_headers()
            self.wfile.write(USERS_BODY)
            return
        self.send_json({"ret": 0, "msg": "not found"}, 404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        path = urlparse(self.path).path
        with state_lock:
            if path == "/mod_mu/users/traffic":
                state["traffic_posts"] += 1
            elif path == "/mod_mu/users/aliveip":
                state["aliveip_posts"] += 1
            else:
                self.send_json({"ret": 0, "msg": "not found"}, 404)
                return
        self.send_json({"ret": 1, "data": "ok"})


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 23000
    ThreadingHTTPServer((host, port), Handler).serve_forever()
