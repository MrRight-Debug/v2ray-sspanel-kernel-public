#!/usr/bin/env python3
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
    ThreadingHTTPServer(("127.0.0.1", 19092), Handler).serve_forever()
