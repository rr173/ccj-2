"""A tiny receiver for manually testing retries, ordering and receipts.

Run outside Docker, for example:

    python3 scripts/mock_receiver.py --port 9000 --fail-times 3

To have it call the receipt API back after processing:

    python3 scripts/mock_receiver.py --port 9000 \
        --receipt-url http://localhost:8000/v1/receipts

Use --receipt-delay together with a small RECEIPT_TIMEOUT_SECONDS on the
server side to watch receipts arrive late (disposition "late").

On Linux containers can usually reach this host process via:

    http://172.17.0.1:9000/
"""

import argparse
import json
import threading
import time
import urllib.request
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

processed = set()
received = defaultdict(list)
counter = defaultdict(int)


def send_receipt(url: str, destination_id: str, dedupe_key: str, result: str, delay: float) -> None:
    if delay > 0:
        time.sleep(delay)
    body = json.dumps(
        {
            "destination_id": destination_id,
            "dedupe_key": dedupe_key,
            "result": result,
        }
    ).encode()
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read() or b"{}")
        print(
            f"receipt sent for {dedupe_key}: disposition={payload.get('disposition')}"
        )
    except Exception as exc:  # noqa: BLE001 - demo script, log and continue
        print(f"receipt for {dedupe_key} failed: {exc}")


def send_consent(ingest_url: str, event_id: str, destination_id: str, decision: str) -> None:
    url = (
        f"{ingest_url}/v1/events/{event_id}/consent"
        f"?destination_id={destination_id}"
    )
    request = urllib.request.Request(
        url,
        data=json.dumps({"decision": decision}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read() or b"{}")
        print(
            f"consent {decision} for event {event_id}: "
            f"disposition={payload.get('disposition')} "
            f"release_state={payload.get('release_state')}"
        )
    except Exception as exc:  # noqa: BLE001 - demo script, log and continue
        print(f"consent for event {event_id} failed: {exc}")


class Handler(BaseHTTPRequestHandler):
    fail_times = 0
    receipt_url: str | None = None
    receipt_result = "success"
    receipt_delay = 0.0
    response_delay = 0.0
    # When true, activation challenges are answered by echoing the challenge
    # straight back in the probe's 2xx response, which confirms the
    # destination immediately. When false, challenges get a 200 without an
    # echo so the handshake stays pending (use the confirm API manually).
    auto_confirm = True
    # Preview-consent gate ("预告 + 点头才给正文"): when an ingest URL is set,
    # an event_preview is answered by calling the consent endpoint. "approve"
    # releases this address's own body; "deny" refuses it; "none" leaves it
    # held so the body can be observed expiring/voiding.
    ingest_url: str | None = None
    preview_decision = "approve"

    def do_POST(self) -> None:  # noqa: N802 - stdlib API
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")

        # Activation handshake probe: it is not an event delivery — answer
        # with the challenge echo and never treat it as processed business.
        if self.headers.get("X-Message-Type") == "activation_challenge" or (
            body.get("type") == "activation_challenge"
        ):
            if self.response_delay > 0:
                time.sleep(self.response_delay)
            challenge = body.get("challenge")
            if self.auto_confirm and challenge:
                self._write(
                    200,
                    {
                        "type": "activation_response",
                        "destination_id": body.get("destination_id"),
                        "echo": challenge,
                    },
                )
            else:
                self._write(200, {"type": "activation_response", "received": True})
            return

        # Preview notice ("预告") of a gated event: it never carries the real
        # payload. Answer via the consent endpoint so the body is (or is not)
        # released for this address.
        is_preview = (
            self.headers.get("X-Message-Type") == "event_preview"
            or body.get("message_type") == "event_preview"
        )
        if is_preview:
            key = self.headers.get("Idempotency-Key")
            self._write(200, {"status": "preview_received", "key": key})
            if self.ingest_url and self.preview_decision in ("approve", "deny"):
                threading.Thread(
                    target=send_consent,
                    args=(
                        self.ingest_url,
                        body["event_id"],
                        body["destination_id"],
                        self.preview_decision,
                    ),
                    daemon=True,
                ).start()
            return

        key = self.headers.get("Idempotency-Key")
        destination = body.get("destination_id") or "unknown"
        counter[destination] += 1
        received[destination].append((body.get("destination_seq"), key))

        if self.response_delay > 0:
            time.sleep(self.response_delay)

        if key in processed:
            self._write(200, {"status": "duplicate_ignored", "key": key})
            self._maybe_receipt(destination, key)
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
        self._maybe_receipt(destination, key)

    def _maybe_receipt(self, destination: str, key: str | None) -> None:
        if not self.receipt_url or not key:
            return
        threading.Thread(
            target=send_receipt,
            args=(self.receipt_url, destination, key, self.receipt_result, self.receipt_delay),
            daemon=True,
        ).start()

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
    parser.add_argument("--receipt-url", default=None)
    parser.add_argument("--receipt-result", default="success", choices=["success", "failure"])
    parser.add_argument("--receipt-delay", type=float, default=0.0)
    parser.add_argument("--response-delay", type=float, default=0.0)
    parser.add_argument(
        "--no-auto-confirm",
        action="store_true",
        help="answer activation probes without echoing the challenge (stay pending)",
    )
    parser.add_argument(
        "--ingest-url",
        default=None,
        help="base URL of the ingest API; when set, previews are answered "
             "via the consent endpoint",
    )
    parser.add_argument(
        "--preview-decision",
        default="approve",
        choices=["approve", "deny", "none"],
        help="how to answer a gated event's preview (default approve; "
             "'none' leaves the body held so it can be watched expiring)",
    )
    args = parser.parse_args()
    Handler.fail_times = args.fail_times
    Handler.receipt_url = args.receipt_url
    Handler.receipt_result = args.receipt_result
    Handler.receipt_delay = args.receipt_delay
    Handler.response_delay = args.response_delay
    Handler.auto_confirm = not args.no_auto_confirm
    Handler.ingest_url = args.ingest_url
    Handler.preview_decision = args.preview_decision
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"mock receiver listening on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
