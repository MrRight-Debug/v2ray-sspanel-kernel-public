#!/usr/bin/env python3
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", "4")
        self.end_headers()
        self.wfile.write(b"a")
        self.wfile.flush()
        time.sleep(2)
        self.wfile.write(b"b")
        self.wfile.flush()
        time.sleep(2)
        self.wfile.write(b"cd")
        self.wfile.flush()


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 19092
    ThreadingHTTPServer((host, port), Handler).serve_forever()
