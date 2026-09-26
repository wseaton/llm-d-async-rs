"""HTTP proxy in front of vLLM that breaks event streams mid-generation, the
way a model pod dying does: the nth stream is cut after CUT_AFTER[n] events
(a comma-separated list), later streams pass through. Everything else passes
through too. Stdlib only."""

import http.client
import json
import os
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

UPSTREAM = urlsplit(os.environ["UPSTREAM"])
HOST = UPSTREAM.hostname or sys.exit("UPSTREAM has no host")
PORT = UPSTREAM.port or 80
cut_points = [int(n) for n in os.environ.get("CUT_AFTER", "").split(",") if n.strip()]
lock = threading.Lock()


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        self.forward(None)

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.forward(body)

    def forward(self, body: bytes | None) -> None:
        conn = http.client.HTTPConnection(HOST, PORT, timeout=600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length")}
        conn.request(self.command, self.path, body=body, headers=headers)
        resp = conn.getresponse()
        streamed = resp.getheader("Content-Type", "").startswith("text/event-stream")

        cut_after = 0
        if streamed:
            with lock:
                if cut_points:
                    cut_after = cut_points.pop(0)
            sent = json.loads(body or b"{}")
            prompt = sent.get("prompt", sent.get("token_ids", sent.get("messages")))
            shape = f"{len(prompt)} prompt token ids" if isinstance(prompt, list) and all(isinstance(t, int) for t in prompt) else "messages or text"
            max_tokens = sent.get("max_tokens", (sent.get("sampling_params") or {}).get("max_tokens"))
            log(f"stream {self.path}: {shape}, max_tokens={max_tokens}, cut_after={cut_after or None}")

        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() not in ("content-length", "transfer-encoding", "connection"):
                self.send_header(k, v)
        if not streamed:
            data = resp.read()
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        events = 0
        event = b""
        while True:
            line = resp.readline()
            if not line:
                break
            event += line
            if line not in (b"\n", b"\r\n"):
                continue
            self.wfile.write(b"%x\r\n%s\r\n" % (len(event), event))
            self.wfile.flush()
            event = b""
            events += 1
            if cut_after and events >= cut_after:
                log(f"cutting the stream after {events} events")
                conn.close()
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                return
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()


if __name__ == "__main__":
    log(f"proxying to {UPSTREAM.geturl()}, cutting streams after {cut_points} events")
    ThreadingHTTPServer(("0.0.0.0", 8080), Proxy).serve_forever()
