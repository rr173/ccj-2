"""End-to-end checks for event deliver-by cutoffs ("最晚送到").

Covers the guarantees:

- an event submitted with deliver_by snapshots the promise on every copy;
- copies still queued when the cutoff passes become terminal deadline_expired
  and are never written as delivered or confused with an unrouted event;
- changing the cutoff afterwards rewrites only still-undelivered copies;
- delivered copies are not recalled or rewritten;
- an event without deliver_by keeps the ordinary delivery lifecycle;
- one event trace shows, per address, which copy delivered and which missed
  the cutoff;
- a relay station missing the cutoff stops later stations without backfill.
"""
import hashlib
import hmac
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
from fastapi.testclient import TestClient

os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://event@/events?host=/tmp&port=55432"
)
os.environ["RETRY_BACKOFF_BASE_SECONDS"] = "0"
os.environ["RECEIPT_TIMEOUT_SECONDS"] = "5"
os.environ["CONFIRM_BACKOFF_BASE_SECONDS"] = "0"
os.environ["CONFIRM_POLL_INTERVAL_SECONDS"] = "0.1"
os.environ["RECONCILE_SWEEP_INTERVAL_SECONDS"] = "0.05"

from app.db import SessionLocal, build_engine  # noqa: E402
from app.models import init_db  # noqa: E402
from app import main as api  # noqa: E402
import app.worker as worker  # noqa: E402
from app import reconciler  # noqa: E402

failures: list[str] = []
RUN = str(int(time.time() * 1000))[-9:]
ETYPE = f"deliverby{RUN}"
RELAY_TYPE = f"deliverbyrelay{RUN}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    def __init__(self, name):
        self.name = name
        self.events: list[dict] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hook-{RUN}-{self.name}"

    def keys(self):
        return [e["dedupe_key"] for e in self.events]

    def _handler(self):
        state = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                if self.headers.get("X-Message-Type") == "activation_challenge":
                    self._write(200, {"echo": body["challenge"]})
                    return
                state.events.append(body)
                self._write(200, {"status": "ok"})

            def _write(self, code, payload):
                data = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        return Handler


init_db(build_engine())
client = TestClient(api.app)

source = client.post("/v1/sources", json={"name": f"deliver-by-test-{RUN}"})
check("source registered", source.status_code == 201, source.text)
SID = source.json()["id"]
SECRET = source.json()["secret"]

recv_a = MockReceiver("a")
recv_b = MockReceiver("b")


def register(receiver, event_types):
    response = client.post(
        "/v1/destinations",
        json={"url": receiver.url(), "event_types": event_types},
    )
    assert response.status_code == 201, response.text
    destination_id = response.json()["id"]
    http_client = httpx.Client(timeout=5)
    db = SessionLocal()
    deadline = time.time() + 5
    while time.time() < deadline:
        worker.process_confirmation_once(db, http_client)
        state = client.get(f"/v1/destinations/{destination_id}").json()
        if state["confirmation_state"] == "confirmed":
            break
        time.sleep(0.05)
    db.close()
    http_client.close()
    state = client.get(f"/v1/destinations/{destination_id}").json()
    assert state["confirmation_state"] == "confirmed", state
    return destination_id


DST_A = register(recv_a, [ETYPE, RELAY_TYPE])
DST_B = register(recv_b, [ETYPE, RELAY_TYPE])


def iso_at(seconds_from_now: float) -> str:
    return (
        datetime.now(timezone.utc) + timedelta(seconds=seconds_from_now)
    ).isoformat()


def push(key, *, event_type=ETYPE, deliver_by=None):
    event = {
        "event_type": event_type,
        "dedupe_key": key,
        "payload": {"n": key},
    }
    if deliver_by is not None:
        event["deliver_by"] = deliver_by
    raw = json.dumps(event).encode()
    ts = str(int(time.time()))
    signature = hmac.new(
        SECRET.encode(), ts.encode() + b"." + raw, hashlib.sha256
    ).hexdigest()
    return client.post(
        "/v1/events",
        content=raw,
        headers={
            "Content-Type": "application/json",
            "X-Source-Id": SID,
            "X-Signed-At": ts,
            "X-Signature": signature,
        },
    )


def trace(event_id):
    return client.get(f"/v1/events/{event_id}/trace").json()


def body_for(event_trace, destination_id):
    return next(
        delivery
        for delivery in event_trace["deliveries"]
        if delivery["destination_id"] == destination_id
        and delivery["phase"] == "body"
    )


# 1. No cutoff: normal delivery.
created = push(f"no-cutoff-{RUN}")
check("event without cutoff accepted", created.status_code == 201, created.text)
check("event without cutoff has null deliver_by", created.json()["deliver_by"] is None)
for _ in range(10):
    worker.process_once()
    time.sleep(0.01)
event = client.get(f"/v1/events/{created.json()['id']}").json()
check("event without cutoff delivers", event["status"] == "delivered", event)

# 2. Submitted with a cutoff already in the past: sweep/claim both enforce it.
created = push(f"past-cutoff-{RUN}", deliver_by=iso_at(-2))
check("past-cutoff event accepted", created.status_code == 201, created.text)
reconciler.sweep_once()
for _ in range(3):
    worker.process_once()
event_trace = trace(created.json()["id"])
statuses = {d["destination_id"]: d["status"] for d in event_trace["deliveries"]}
check(
    "past-cutoff copies are deadline_expired",
    statuses == {DST_A: "deadline_expired", DST_B: "deadline_expired"},
    statuses,
)
check(
    "past-cutoff event is deadline_expired",
    event_trace["event"]["status"] == "deadline_expired",
    event_trace["event"]["status"],
)
check(
    "past-cutoff copies were not sent",
    recv_a.keys().count(f"past-cutoff-{RUN}") == 0
    and recv_b.keys().count(f"past-cutoff-{RUN}") == 0,
)

# 3. Extend the deadline before the old cutoff: both still-pending copies use
# the new promise and can deliver.
created = push(f"extend-{RUN}", deliver_by=iso_at(60))
event_id = created.json()["id"]
update = client.post(
    f"/v1/events/{event_id}/deliver-by",
    json={"deliver_by": iso_at(120)},
)
check("cutoff extension accepted", update.status_code == 200, update.text)
check("cutoff extension touches two pending copies", update.json()["updated_count"] == 2)
for _ in range(10):
    worker.process_once()
event_trace = trace(event_id)
check(
    "extended-cutoff event delivers",
    event_trace["event"]["status"] == "delivered",
    event_trace["event"]["status"],
)

# 4. One address already delivered before an earlier changed cutoff; the later
# cutoff closes only the still-pending address and does not recall the first.
paused = client.post(
    f"/v1/destinations/{DST_B}/pause",
    json={"paused_from": datetime.now(timezone.utc).isoformat()},
)
check("destination B paused", paused.status_code == 200, paused.text)
created = push(f"partial-{RUN}", deliver_by=iso_at(60))
event_id = created.json()["id"]
for _ in range(10):
    worker.process_once()
# Tighten to a past cutoff after A is delivered; B is still waiting.
update = client.post(
    f"/v1/events/{event_id}/deliver-by",
    json={"deliver_by": iso_at(-1)},
)
check("late cutoff change accepted", update.status_code == 200, update.text)
check("late cutoff touches only undelivered B", update.json()["updated_count"] == 1)
reconciler.sweep_once()
event_trace = trace(event_id)
by_destination = {
    d["destination_id"]: d for d in event_trace["deliveries"] if d["phase"] == "body"
}
check("A remains delivered", by_destination[DST_A]["status"] == "delivered")
check("B is deadline_expired", by_destination[DST_B]["status"] == "deadline_expired")
check(
    "trace distinguishes delivered and cutoff-missed addresses",
    by_destination[DST_A]["delivered_at"] is not None
    and by_destination[DST_B]["delivered_at"] is None
    and by_destination[DST_B]["deliver_by_expired_at"] is not None,
)
check(
    "partial event is deadline_expired rather than delivered",
    event_trace["event"]["status"] == "deadline_expired",
)
resumed = client.post(f"/v1/destinations/{DST_B}/resume")
check("destination B resumed", resumed.status_code == 200, resumed.text)

# 5. A cutoff miss at relay station 1 stops the remaining station.
chain = client.put(
    f"/v1/event-types/{RELAY_TYPE}/relay-chain",
    json={"destination_ids": [DST_A, DST_B]},
)
check("relay chain defined", chain.status_code == 200, chain.text)
created = push(
    f"relay-cutoff-{RUN}", event_type=RELAY_TYPE, deliver_by=iso_at(-1)
)
check("relay cutoff event accepted", created.status_code == 201, created.text)
reconciler.sweep_once()
for _ in range(5):
    worker.process_once()
event_trace = trace(created.json()["id"])
stations = event_trace["event"].get("relay")
check("relay event halted", event_trace["event"]["status"] == "relay_halted")
check(
    "relay cutoff reason visible",
    stations and stations["stop_reason"] == "deliver_by_expired",
    stations,
)
check(
    "later relay station skipped",
    stations and stations["stations"][1]["status"] == "relay_skipped",
    stations,
)

if failures:
    print(f"\n{len(failures)} check(s) failed: {failures}")
    raise SystemExit(1)
print("\nall deliver-by checks passed")
