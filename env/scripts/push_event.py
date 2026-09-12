"""Sign and push one event as a registered external source.

Registration (one-time per source) gives you a source id and a secret; the
secret is shown only then (and on key rotation). Every event push must carry:

    X-Source-Id   the id returned at registration
    X-Signed-At   the send time as a Unix timestamp (seconds)
    X-Signature   hex HMAC-SHA256(secret, f"{X-Signed-At}." + raw_json_body)

Examples:

    # register a new source (prints id + secret)
    python3 scripts/push_event.py --register billing

    # push an event with that secret
    python3 scripts/push_event.py \\
        --source-id 3f1c... --secret '...' \\
        --event-type paid --dedupe-key order-1001 \\
        --payload '{"order_id":"1001"}' \
        --deliver-by 2026-09-10T12:00:00Z

    # simulate a replay (re-sign with an old send timestamp) -> 401 stale
    python3 scripts/push_event.py ... --signed-at $(( $(date +%s) - 600 ))

The secret is read from --secret or the EVENT_SOURCE_SECRET environment
variable so it does not have to land in the shell history.
"""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request


def http_request(url, method="GET", body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")


def sign(secret: str, signed_at: str, raw_body: bytes) -> str:
    message = signed_at.encode("ascii") + b"." + raw_body
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def register(base_url: str, name: str) -> int:
    status, payload = http_request(f"{base_url}/v1/sources", "POST", {"name": name})
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if status == 201:
        print(
            "\nSave the secret now — it is never shown again.\n"
            "Rotate it with: POST /v1/sources/{id}/rotate-key",
            file=sys.stderr,
        )
    return 0 if status == 201 else 1


def push(base_url: str, args: argparse.Namespace) -> int:
    secret = args.secret or os.environ.get("EVENT_SOURCE_SECRET")
    if not secret:
        print("error: provide --secret or EVENT_SOURCE_SECRET", file=sys.stderr)
        return 2
    payload = json.loads(args.payload)
    event = {
        "event_type": args.event_type,
        "dedupe_key": args.dedupe_key,
        "payload": payload,
    }
    if args.preview_payload is not None:
        # Only valid for a gated event type (one with a preview-consent
        # policy); the real content stays in payload and only goes out with
        # the body after this address nods.
        event["preview_payload"] = json.loads(args.preview_payload)
    if args.not_before is not None:
        event["not_before"] = args.not_before
    if args.deliver_by is not None:
        event["deliver_by"] = args.deliver_by
    raw_body = json.dumps(event).encode()
    signed_at = args.signed_at or str(int(time.time()))
    headers = {
        "X-Source-Id": args.source_id,
        "X-Signed-At": signed_at,
        "X-Signature": sign(secret, signed_at, raw_body),
    }
    request = urllib.request.Request(
        f"{base_url}/v1/events", data=raw_body, method="POST",
        headers={"Content-Type": "application/json", **headers},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            status, body = response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        status, body = exc.code, json.loads(exc.read() or b"null")
    print(json.dumps(body, indent=2, ensure_ascii=False))
    if status == 200 and body.get("duplicate"):
        print("note: duplicate resend — no new event was created", file=sys.stderr)
    if status == 201 and body.get("status") == "unrouted":
        print("note: accepted and stored, but no destination subscribes to this "
              "event type — nothing was sent out", file=sys.stderr)
    return 0 if status in (200, 201) else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.environ.get("INGEST_URL", "http://localhost:8000"))
    parser.add_argument("--register", metavar="NAME", help="register a new source and print its secret")
    parser.add_argument("--source-id")
    parser.add_argument("--secret", help="defaults to $EVENT_SOURCE_SECRET")
    parser.add_argument("--signed-at", help="override the Unix-seconds send time (testing replays)")
    parser.add_argument("--event-type")
    parser.add_argument("--dedupe-key")
    parser.add_argument("--payload", default="{}")
    parser.add_argument(
        "--preview-payload",
        default=None,
        help="short text for the preview of a gated event type (JSON)",
    )
    parser.add_argument("--not_before", "--not-before", dest="not_before", default=None)
    parser.add_argument("--deliver_by", "--deliver-by", dest="deliver_by", default=None)
    args = parser.parse_args()

    if args.register:
        raise SystemExit(register(args.base_url, args.register))
    if not (args.source_id and args.event_type and args.dedupe_key):
        parser.error("--source-id, --event-type and --dedupe-key are required to push")
    raise SystemExit(push(args.base_url, args))


if __name__ == "__main__":
    main()
