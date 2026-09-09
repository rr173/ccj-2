"""A tiny receiver for manually testing retries and ordering.

Run outside Docker, for example:

    python3 scripts/mock_receiver.py --port 9000 --fail-times 3

On Linux containers can usually reach this host process via:

    http://172.17.0.1:9000/
"""

import argparse
import json
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

processed = set()
received = defaultdict(list)
counter = defaultdict(int)


class Handler(BaseHTTPRequestHandler):
    fail_times = 0

    def do_POST(self) -> None:  # noqa: N802 - stdlib API
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        key = self.headers.get("Idempotency-Key")
        destination = body.get("destination_id") or "unknown"
        counter[destination] += 1
        received[destination].append((body.get("destination_seq"), key))

        if key in processed:
            self._write(200, {"status": "duplicate_ignored", "key": key})
            return

        if self.fail_times and counter[destination] <= self.fail_times:
            self._write(500, {"status": "forced_failure", "attempt": counter[destination]})
            return

        processed.add(key)
        self._write(
            200,
            {
                "status": "processed",
                "key": key,
                "seq": body.get("destination_seq"),
                "received": received[destination],
            },
        )

    def log_message(self, fmt: str, *args) -> None:
        print(f"{time.strftime('%H:%M:%S')} {self.address_string()} {fmt % args}")

    def _write(self, status_code: int, payload: dict) -> None:
        data = json.dumps(payload).encode()
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--fail-times", type=int, default=0)
    args = parser.parse_args()
    Handler.fail_times = args.fail_times
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"mock receiver listening on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
