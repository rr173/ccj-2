"""End-to-end checks for operator-marked "not receiving" windows (pause/resume).

Covers the five guarantees of a per-destination receive pause:
- a window can be marked on one address (bounded, indefinite, future-start);
- copies due while the window is in effect wait in their original queue
  positions and never cut ahead of a copy currently in flight;
- reconciliation starts only when a copy is really sent out — never from
  submit time, even when the copy sat paused longer than the receipt timeout;
- the pause makes no attempts at all, so it can never be charged as
  consecutive failures or isolate the address;
- other destinations subscribed to the same event type keep draining.
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
from sqlalchemy import text

os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://event@/events?host=/tmp&port=55432"
)
os.environ["RETRY_BACKOFF_BASE_SECONDS"] = "0"
# Short receipt window: a copy paused longer than this would look "timed out"
# if the countdown wrongly started at submit time instead of at real send.
os.environ["RECEIPT_TIMEOUT_SECONDS"] = "3"
os.environ["CONFIRM_BACKOFF_BASE_SECONDS"] = "0"
os.environ["CONFIRM_POLL_INTERVAL_SECONDS"] = "0.1"

from app.db import SessionLocal, build_engine  # noqa: E402
from app.models import init_db  # noqa: E402
from app import main as api  # noqa: E402
import app.worker as worker  # noqa: E402
from app import reconciler  # noqa: E402
from app.config import settings  # noqa: E402

failures: list[str] = []
RUN = str(int(time.time() * 1000))[-9:]
ETYPE = f"pause{RUN}"


def K(name: str) -> str:
    return f"{RUN}-{name}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    """Records delivered events in order; can be told to fail every POST."""

    def __init__(self, fail_times=0):
        self.fail_times = fail_times
        self.events: list[dict] = []
        self.challenges: list[dict] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hook-{RUN}"

    def _handler(self):
        state = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                if self.headers.get("X-Message-Type") == "activation_challenge":
                    state.challenges.append(body)
                    self._write(200, {"echo": body["challenge"]})
                    return
                state.events.append(body)
                if state.fail_times > 0:
                    state.fail_times -= 1
                    self._write(500, {"error": "boom"})
                else:
                    self._write(200, {"status": "ok"})

            def _write(self, code, payload):
                data = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        return Handler


# --- scaffold ---------------------------------------------------------------

init_db(build_engine())
client = TestClient(api.app)

r = client.post("/v1/sources", json={"name": f"pause-test-{RUN}"})
check("source register 201", r.status_code == 201, r.status_code)
SID, SECRET = r.json()["id"], r.json()["secret"]


def push_event(key, event_type=ETYPE, payload=None):
    event = {"event_type": event_type, "dedupe_key": key, "payload": payload or {}}
    raw = json.dumps(event).encode()
    ts = str(int(time.time()))
    sig = hmac.new(SECRET.encode(), ts.encode() + b"." + raw, hashlib.sha256).hexdigest()
    return client.post(
        "/v1/events",
        content=raw,
        headers={
            "Content-Type": "application/json",
            "X-Source-Id": SID,
            "X-Signed-At": ts,
            "X-Signature": sig,
        },
    )


def register_confirmed(receiver: MockReceiver):
    r = client.post(
        "/v1/destinations",
        json={"url": receiver.url(), "event_types": [ETYPE]},
    )
    assert r.status_code == 201, r.text
    did = r.json()["id"]
    hc = httpx.Client(timeout=5)
    db = SessionLocal()
    deadline = time.time() + 5
    while time.time() < deadline:
        worker.process_confirmation_once(db, hc)
        if client.get(f"/v1/destinations/{did}").json()["confirmation_state"] == "confirmed":
            break
        time.sleep(0.1)
    db.close()
    hc.close()
    state = client.get(f"/v1/destinations/{did}").json()
    assert state["confirmation_state"] == "confirmed", state
    return did


def run_worker(times=20):
    for _ in range(times):
        worker.process_once()


def delivery_for(event_id, destination_id):
    return next(
        d
        for d in client.get(f"/v1/events/{event_id}/trace").json()["deliveries"]
        if d["destination_id"] == destination_id
    )


def destination_state(destination_id):
    return client.get(f"/v1/destinations/{destination_id}").json()


def ts(value):
    return datetime.fromisoformat(value)


def sleep_until(moment: datetime, margin=0.15):
    remaining = (moment - datetime.now(timezone.utc)).total_seconds() + margin
    if remaining > 0:
        time.sleep(remaining)


recv_a = MockReceiver()
recv_b = MockReceiver()
DA = register_confirmed(recv_a)
DB = register_confirmed(recv_b)

# ===========================================================================
# 0. A copy delivered BEFORE the pause keeps its normal reconcile lifecycle:
#    its receipt still applies while the address is paused.
# ===========================================================================
r = push_event(K("e0"))
e0 = r.json()["id"]
run_worker(10)
check("e0 delivered to A before the pause",
      [e["dedupe_key"] for e in recv_a.events] == [K("e0")],
      [e["dedupe_key"] for e in recv_a.events])

until = datetime.now(timezone.utc) + timedelta(seconds=2.5)
r = client.post(f"/v1/destinations/{DA}/pause",
                json={"paused_until": until.isoformat()})
check("pause 200", r.status_code == 200, r.status_code)
check("pause response shows active window",
      r.json()["paused"] is True and r.json()["paused_from"] is not None
      and ts(r.json()["paused_until"]) > datetime.now(timezone.utc),
      (r.json()["paused"], r.json()["paused_from"], r.json()["paused_until"]))

r = client.post(
    "/v1/receipts",
    json={"destination_id": DA, "dedupe_key": K("e0"), "result": "success"},
)
check("receipt for a pre-pause delivery still applies during the pause",
      r.json()["disposition"] == "applied", r.json())

# ===========================================================================
# 1. Bounded window: copies wait in queue; the same-type neighbour keeps
#    draining; the paused address is not touched at all.
# ===========================================================================
until = datetime.now(timezone.utc) + timedelta(seconds=4)
r = client.post(f"/v1/destinations/{DA}/pause",
                json={"paused_until": until.isoformat()})
check("re-pause replaces the window", r.status_code == 200, r.status_code)

r = push_event(K("e1"))
check("e1 accepted 201", r.status_code == 201, r.status_code)
e1 = r.json()["id"]
e1_created = ts(r.json()["created_at"])

run_worker(15)

# The other subscriber of the same type is unaffected (it also got e0 earlier).
check("same-type neighbour received e1 during the pause",
      [e["dedupe_key"] for e in recv_b.events][-1:] == [K("e1")],
      [e["dedupe_key"] for e in recv_b.events])
# The paused address got nothing: no attempt was made at all.
check("paused receiver got no delivery", len(recv_a.events) == 1, len(recv_a.events))
d1 = delivery_for(e1, DA)
check("paused copy waits pending with zero attempts",
      d1["status"] == "pending" and d1["attempts"] == 0,
      (d1["status"], d1["attempts"]))
check("no reconcile countdown while waiting",
      d1["reconcile_state"] == "none" and d1["reconcile_deadline"] is None,
      (d1["reconcile_state"], d1["reconcile_deadline"]))
e1_seq = d1["destination_seq"]
st = destination_state(DA)
check("pause is not charged as failures and does not isolate",
      st["failure_count"] == 0 and st["status"] == "active" and st["paused"] is True,
      (st["failure_count"], st["status"], st["paused"]))
ev = client.get(f"/v1/events/{e1}/trace").json()["event"]
check("event stays pending while one copy waits paused",
      ev["status"] == "pending" and ev["delivered_count"] == 1
      and ev["delivery_count"] == 2,
      (ev["status"], ev["delivered_count"], ev["delivery_count"]))

# Window ends: the waiting copy goes out in its original queue position.
sleep_until(until)
run_worker(10)
check("paused copy delivered after the window",
      [e["dedupe_key"] for e in recv_a.events] == [K("e0"), K("e1")],
      [e["dedupe_key"] for e in recv_a.events])
d1 = delivery_for(e1, DA)
check("copy kept its original queue position",
      d1["destination_seq"] == e1_seq and d1["status"] == "delivered",
      (d1["destination_seq"], e1_seq, d1["status"]))

# Reconciliation started at the real send, not at submit: the copy sat paused
# ~4s, longer than RECEIPT_TIMEOUT_SECONDS=3, so a submit-time countdown would
# already have expired. The deadline must be delivered_at + timeout and a
# sweep right now must NOT time the copy out.
check("reconcile deadline counts from the real send",
      d1["reconcile_state"] == "awaiting"
      and abs((ts(d1["reconcile_deadline"]) - ts(d1["delivered_at"])).total_seconds()
              - settings.receipt_timeout_seconds) < 1.0
      and ts(d1["reconcile_deadline"]) > e1_created
      and (ts(d1["delivered_at"]) - e1_created).total_seconds()
          > settings.receipt_timeout_seconds,
      (d1["reconcile_deadline"], d1["delivered_at"], str(e1_created)))
timed_out, _ = reconciler.sweep_once()
d1 = delivery_for(e1, DA)
check("sweep right after delivery does not time the copy out",
      d1["reconcile_state"] == "awaiting", d1["reconcile_state"])
st = destination_state(DA)
check("window over: address reports not paused",
      st["paused"] is False and st["failure_count"] == 0 and st["status"] == "active",
      (st["paused"], st["failure_count"], st["status"]))

# ===========================================================================
# 2. Queued copies never cut ahead of a copy currently in flight.
# ===========================================================================
until = datetime.now(timezone.utc) + timedelta(seconds=2.5)
client.post(f"/v1/destinations/{DA}/pause", json={"paused_until": until.isoformat()})
r = push_event(K("e2"))
e2 = r.json()["id"]
r = push_event(K("e3"))
e3 = r.json()["id"]
run_worker(10)
check("both copies queue behind the pause",
      len(recv_a.events) == 2, len(recv_a.events))

sleep_until(until)
# The window is over, but the head copy (e2) is now being delivered by
# another worker (simulated): e3 must not jump ahead of it.
d2 = delivery_for(e2, DA)
db = SessionLocal()
db.execute(
    text(
        """
        UPDATE deliveries
        SET status = 'in_flight', claim_token = gen_random_uuid(),
            claimed_at = now(), lease_until = now() + interval '5 minutes'
        WHERE id = CAST(:id AS UUID)
        """
    ),
    {"id": d2["id"]},
)
db.commit()
db.close()
run_worker(10)
check("later copy does not cut ahead of the in-flight head",
      len(recv_a.events) == 2, len(recv_a.events))

# The in-flight call finishes (simulated): e2 goes first, then e3, in order.
db = SessionLocal()
db.execute(
    text(
        """
        UPDATE deliveries
        SET status = 'pending', claim_token = NULL, claimed_at = NULL,
            lease_until = NULL, next_attempt_at = now()
        WHERE id = CAST(:id AS UUID)
        """
    ),
    {"id": d2["id"]},
)
db.commit()
db.close()
run_worker(10)
check("queued copies drain in original FIFO order after the pause",
      [e["dedupe_key"] for e in recv_a.events][-2:] == [K("e2"), K("e3")],
      [e["dedupe_key"] for e in recv_a.events])

# ===========================================================================
# 3. A paused address that would fail every call makes no attempts at all:
#    nothing is charged as consecutive failures, nothing isolates it.
# ===========================================================================
recv_c = MockReceiver(fail_times=10**9)  # always fails when actually called
DC = register_confirmed(recv_c)

until = datetime.now(timezone.utc) + timedelta(seconds=3)
client.post(f"/v1/destinations/{DC}/pause", json={"paused_until": until.isoformat()})
r = push_event(K("e4"))
e4 = r.json()["id"]
run_worker(15)
check("failing receiver got zero calls while paused",
      len(recv_c.events) == 0, len(recv_c.events))
d4 = delivery_for(e4, DC)
check("paused copy shows zero attempts and no error",
      d4["attempts"] == 0 and d4["last_error"] is None,
      (d4["attempts"], d4["last_error"]))
st = destination_state(DC)
check("pause never counts as consecutive failures nor isolates",
      st["failure_count"] == 0 and st["status"] == "active",
      (st["failure_count"], st["status"]))

sleep_until(until)
run_worker(3)
check("after the window real attempts happen and failures count normally",
      len(recv_c.events) >= 1 and destination_state(DC)["failure_count"] >= 1,
      (len(recv_c.events), destination_state(DC)["failure_count"]))

# ===========================================================================
# 4. Indefinite pause: nothing flows until an explicit resume.
# ===========================================================================
r = client.post(f"/v1/destinations/{DA}/pause", json={"paused_from": None})
check("indefinite pause 200", r.status_code == 200, r.status_code)
check("indefinite window: no end, currently paused",
      r.json()["paused_until"] is None and r.json()["paused"] is True,
      (r.json()["paused_until"], r.json()["paused"]))
r = push_event(K("e5"))
e5 = r.json()["id"]
run_worker(10)
check("indefinite pause holds the copy",
      K("e5") not in [e["dedupe_key"] for e in recv_a.events],
      [e["dedupe_key"] for e in recv_a.events])
r = client.post(f"/v1/destinations/{DA}/resume")
check("resume 200 and reports a cleared window",
      r.status_code == 200 and r.json()["resumed"] is True
      and r.json()["destination"]["paused"] is False
      and r.json()["destination"]["paused_from"] is None,
      (r.status_code, r.json().get("resumed")))
r = client.post(f"/v1/destinations/{DA}/resume")
check("resume without a window is idempotent",
      r.status_code == 200 and r.json()["resumed"] is False, r.json().get("resumed"))
run_worker(10)
check("copy flows right after the resume",
      [e["dedupe_key"] for e in recv_a.events][-1:] == [K("e5")],
      [e["dedupe_key"] for e in recv_a.events])

# ===========================================================================
# 5. Future window: delivery flows until the window opens, holds inside it,
#    and resumes when it closes.
# ===========================================================================
start = datetime.now(timezone.utc) + timedelta(seconds=2)
until = start + timedelta(seconds=2.5)
r = client.post(
    f"/v1/destinations/{DB}/pause",
    json={"paused_from": start.isoformat(), "paused_until": until.isoformat()},
)
check("future window 200 and not yet in effect",
      r.status_code == 200 and r.json()["paused"] is False
      and ts(r.json()["paused_from"]) > datetime.now(timezone.utc),
      (r.status_code, r.json()["paused"], r.json()["paused_from"]))

r = push_event(K("e6"))
e6 = r.json()["id"]
run_worker(10)
check("delivery flows before the future window opens",
      [e["dedupe_key"] for e in recv_b.events][-1:] == [K("e6")],
      [e["dedupe_key"] for e in recv_b.events])

sleep_until(start)
r = push_event(K("e7"))
e7 = r.json()["id"]
run_worker(10)
check("window in effect: copy waits",
      K("e7") not in [e["dedupe_key"] for e in recv_b.events],
      [e["dedupe_key"] for e in recv_b.events])
check("destination reports paused inside the window",
      destination_state(DB)["paused"] is True, destination_state(DB)["paused"])

sleep_until(until)
run_worker(10)
check("window closed: waiting copy goes out",
      [e["dedupe_key"] for e in recv_b.events][-1:] == [K("e7")],
      [e["dedupe_key"] for e in recv_b.events])

# ===========================================================================
# 6. Validation and lookup behaviour.
# ===========================================================================
check("pause without any bound is 422",
      client.post(f"/v1/destinations/{DA}/pause", json={}).status_code == 422)
past = datetime.now(timezone.utc) - timedelta(seconds=5)
check("pause ending before it starts is 422",
      client.post(
          f"/v1/destinations/{DA}/pause",
          json={"paused_from": past.isoformat(),
                "paused_until": (past + timedelta(seconds=1)).isoformat()},
      ).status_code == 422)
check("pause unknown destination is 404",
      client.post(
          "/v1/destinations/00000000-0000-0000-0000-000000000000/pause",
          json={"paused_until": until.isoformat()},
      ).status_code == 404)
check("resume unknown destination is 404",
      client.post(
          "/v1/destinations/00000000-0000-0000-0000-000000000000/resume"
      ).status_code == 404)

print()
if failures:
    print(f"{len(failures)} CHECK(S) FAILED: {failures}")
    raise SystemExit(1)
print("ALL RECEIVE-PAUSE CHECKS PASSED")
