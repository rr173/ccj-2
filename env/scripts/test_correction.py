"""End-to-end checks for event corrections ("补一笔更正").

A correction is an additional entry submitted against an already-accepted
event. Covers the guarantees:

- the original copies that already went out are never recalled or rewritten;
  the correction is a new event linked by corrects_event_id;
- the correction fans out only to destinations the original was really
  delivered to — destinations that never got it are not corrected;
- correction copies queue at the tail of each destination's queue and never
  cut ahead of a copy currently in flight;
- reconciliation starts when the correction copy is really sent, never from
  the correction's submit time;
- a failed correction copy is accounted on its own: terminal 'failed', no
  retry, no destination failure tally / isolation, no blocking later copies,
  and the original event's acknowledged standing is untouched;
- shadow (observe_only) addresses get their own correction copy, still as a
  shadow — it can never count toward the correction's for-real quorum;
- events that were never routed or never accepted cannot be corrected and
  are queryable as "no such thing" — never written as sent;
- the same correction submitted twice is applied exactly once.
"""
import hashlib
import hmac
import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
from fastapi.testclient import TestClient
from sqlalchemy import text

os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://event@/events?host=/tmp&port=55432"
)
os.environ["RETRY_BACKOFF_BASE_SECONDS"] = "0"
# Short receipt window: a correction waiting longer than this before its real
# send would look "timed out" if the countdown wrongly started at submit time.
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
ETYPE = f"corr{RUN}"
ETYPE2 = f"corrsolo{RUN}"  # subscribed by A only
ETYPE3 = f"corrpending{RUN}"  # subscribed by an unconfirmed receiver only
ETYPE_NONE = f"corrnobody{RUN}"  # subscribed by nobody


def K(name: str) -> str:
    return f"{RUN}-{name}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    """Records delivered events (body + headers) in order; can fail POSTs."""

    def __init__(self, fail_times=0):
        self.fail_times = fail_times
        self.events: list[dict] = []
        self.challenges: list[dict] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hook-{RUN}"

    def keys(self) -> list[str]:
        return [e["body"]["dedupe_key"] for e in self.events]

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
                state.events.append(
                    {
                        "body": body,
                        "corrects_header": self.headers.get("X-Corrects-Event-Id"),
                    }
                )
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

r = client.post("/v1/sources", json={"name": f"correction-test-{RUN}"})
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


def register_confirmed(receiver: MockReceiver, event_types, observe_only=False):
    r = client.post(
        "/v1/destinations",
        json={
            "url": receiver.url(),
            "event_types": event_types,
            "observe_only": observe_only,
        },
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


def trace(event_id):
    return client.get(f"/v1/events/{event_id}/trace").json()


def delivery_for(event_id, destination_id, dedupe_key=None):
    return next(
        d
        for d in trace(event_id)["deliveries"]
        if d["destination_id"] == destination_id
        and (dedupe_key is None or d["dedupe_key"] == dedupe_key)
    )


def destination_state(destination_id):
    return client.get(f"/v1/destinations/{destination_id}").json()


def correct(event_id, key, payload=None):
    return client.post(
        f"/v1/events/{event_id}/corrections",
        json={"dedupe_key": key, "payload": payload or {}},
    )


def send_receipt(destination_id, key, result="success"):
    return client.post(
        "/v1/receipts",
        json={"destination_id": destination_id, "dedupe_key": key, "result": result},
    )


def ts(value):
    return datetime.fromisoformat(value)


recv_a = MockReceiver()
recv_b = MockReceiver()
recv_c = MockReceiver()
recv_d = MockReceiver()
DA = register_confirmed(recv_a, [ETYPE, ETYPE2])
DB = register_confirmed(recv_b, [ETYPE])
DC = register_confirmed(recv_c, [ETYPE], observe_only=True)
DD = register_confirmed(recv_d, [ETYPE])

# ===========================================================================
# 1. Basic correction: an additional entry behind the original, original
#    copies untouched, correction carries its own identity.
# ===========================================================================
r = push_event(K("e1"), payload={"order_id": "1", "amount": 10})
check("e1 accepted 201", r.status_code == 201, r.status_code)
e1 = r.json()["id"]
run_worker(10)
check("e1 delivered to A, B and shadow C",
      recv_a.keys() == [K("e1")] and recv_b.keys() == [K("e1")]
      and recv_c.keys() == [K("e1")],
      (recv_a.keys(), recv_b.keys(), recv_c.keys()))

r = correct(e1, K("c1"), payload={"order_id": "1", "amount": 12})
check("correction c1 created 201", r.status_code == 201, r.status_code)
c1 = r.json()
check("c1 links the original and snapshots its own quorum",
      c1["corrects_event_id"] == e1 and c1["dedupe_key"] == K("c1")
      and c1["delivery_count"] == 3 and c1["shadow_delivery_count"] == 1
      and c1["required_ack_count"] == 3 and c1["status"] == "pending",
      (c1["corrects_event_id"], c1["delivery_count"],
       c1["shadow_delivery_count"], c1["required_ack_count"], c1["status"]))
c1_id = c1["id"]

# Not sent yet: the correction's copies queue with no reconcile countdown.
c1_trace = trace(c1_id)
check("c1 copies queued with no reconcile countdown before send",
      len(c1_trace["deliveries"]) == 4
      and all(d["status"] == "pending" for d in c1_trace["deliveries"])
      and all(d["reconcile_state"] == "none" for d in c1_trace["deliveries"])
      and all(d["reconcile_deadline"] is None for d in c1_trace["deliveries"]),
      [(d["status"], d["reconcile_state"]) for d in c1_trace["deliveries"]])
check("c1 shadow copy keeps the observe_only snapshot",
      sum(1 for d in c1_trace["deliveries"] if d["observe_only"]) == 1
      and delivery_for(c1_id, DC)["observe_only"] is True,
      [d["observe_only"] for d in c1_trace["deliveries"]])

run_worker(10)
check("correction delivered after the original at every address",
      recv_a.keys() == [K("e1"), K("c1")]
      and recv_b.keys() == [K("e1"), K("c1")]
      and recv_c.keys() == [K("e1"), K("c1")]
      and recv_d.keys() == [K("e1"), K("c1")],
      (recv_a.keys(), recv_b.keys(), recv_c.keys(), recv_d.keys()))
c1_at_a = recv_a.events[-1]
check("correction copy carries its own key, payload and corrects linkage",
      c1_at_a["body"]["corrects_event_id"] == e1
      and c1_at_a["body"]["dedupe_key"] == K("c1")
      and c1_at_a["body"]["payload"] == {"order_id": "1", "amount": 12}
      and c1_at_a["body"]["event_id"] == c1_id
      and c1_at_a["corrects_header"] == e1,
      (c1_at_a["body"].get("corrects_event_id"), c1_at_a["corrects_header"]))
check("original event carries no correction marker on its own copy",
      recv_a.events[0]["body"].get("corrects_event_id") is None
      and recv_a.events[0]["corrects_header"] is None,
      recv_a.events[0]["body"].get("corrects_event_id"))

# The original event is untouched; its trace lists the correction.
e1_trace = trace(e1)
check("original event still has exactly its own 4 copies",
      len(e1_trace["deliveries"]) == 4
      and all(d["dedupe_key"] == K("e1") for d in e1_trace["deliveries"]),
      [d["dedupe_key"] for d in e1_trace["deliveries"]])
check("original trace lists the correction",
      [c["id"] for c in e1_trace["corrections"]] == [c1_id]
      and e1_trace["corrections"][0]["corrects_event_id"] == e1,
      [c["id"] for c in e1_trace["corrections"]])
r = client.get(f"/v1/events/{e1}/corrections")
check("GET corrections lists c1",
      r.status_code == 200 and [c["id"] for c in r.json()] == [c1_id],
      (r.status_code, [c["id"] for c in r.json()]))

# ===========================================================================
# 2. The same correction twice is applied exactly once; a foreign dedupe key
#    conflicts instead of duplicating.
# ===========================================================================
r = correct(e1, K("c1"), payload={"order_id": "1", "amount": 12})
check("same correction again: 200 duplicate of the original",
      r.status_code == 200 and r.json()["duplicate"] is True
      and r.json()["id"] == c1_id,
      (r.status_code, r.json().get("id")))
run_worker(5)
check("no second fan-out from the duplicate",
      recv_a.keys() == [K("e1"), K("c1")], recv_a.keys())
r = correct(e1, K("e1"))
check("dedupe key owned by another event conflicts (409)",
      r.status_code == 409, r.status_code)

# ===========================================================================
# 3. Correction receipts reconcile the correction itself; a shadow's receipt
#    never counts toward the correction's for-real quorum.
# ===========================================================================
r = send_receipt(DC, K("c1"))
check("shadow receipt applies to its own correction copy",
      r.json()["disposition"] == "applied", r.json())
c1_state = trace(c1_id)["event"]
check("shadow ack does not count as for-real",
      c1_state["reconcile_status"] == "pending"
      and c1_state["acknowledged_count"] == 0
      and c1_state["shadow_acknowledged_count"] == 1,
      (c1_state["reconcile_status"], c1_state["acknowledged_count"],
       c1_state["shadow_acknowledged_count"]))
check("shadow copy reconciled on its own",
      delivery_for(c1_id, DC)["reconcile_state"] == "acknowledged",
      delivery_for(c1_id, DC)["reconcile_state"])

send_receipt(DA, K("c1"))
c1_state = trace(c1_id)["event"]
check("one for-real ack: partially acknowledged",
      c1_state["reconcile_status"] == "partially_acknowledged"
      and c1_state["acknowledged_count"] == 1,
      (c1_state["reconcile_status"], c1_state["acknowledged_count"]))
send_receipt(DB, K("c1"))
send_receipt(DD, K("c1"))
c1_state = trace(c1_id)["event"]
check("for-real quorum reached: correction acknowledged",
      c1_state["reconcile_status"] == "acknowledged"
      and c1_state["acknowledged_quorum"] is True,
      (c1_state["reconcile_status"], c1_state["acknowledged_quorum"]))
check("original event unaffected by correction receipts",
      trace(e1)["event"]["reconcile_status"] == "pending"
      and trace(e1)["event"]["acknowledged_count"] == 0,
      trace(e1)["event"]["reconcile_status"])

# ===========================================================================
# 4. Only addresses the original really reached get the correction.
# ===========================================================================
client.post(f"/v1/destinations/{DB}/pause", json={"paused_from": None})
r = push_event(K("e2"))
e2 = r.json()["id"]
run_worker(10)
check("e2 delivered to A and shadow C, held at paused B",
      recv_a.keys()[-1] == K("e2") and recv_c.keys()[-1] == K("e2")
      and K("e2") not in recv_b.keys(),
      (recv_a.keys(), recv_b.keys(), recv_c.keys()))

r = correct(e2, K("c2"), payload={"fixed": True})
check("c2 created for the delivered set only",
      r.status_code == 201 and r.json()["delivery_count"] == 2
      and r.json()["shadow_delivery_count"] == 1
      and r.json()["required_ack_count"] == 2,
      (r.status_code, r.json()["delivery_count"],
       r.json()["shadow_delivery_count"]))
c2_id = r.json()["id"]
run_worker(10)
check("c2 went to A, D and C, not to the never-delivered B",
      recv_a.keys()[-1] == K("c2") and recv_c.keys()[-1] == K("c2")
      and recv_d.keys()[-1] == K("c2")
      and K("c2") not in recv_b.keys(),
      (recv_a.keys(), recv_b.keys(), recv_c.keys(), recv_d.keys()))

client.post(f"/v1/destinations/{DB}/resume")
run_worker(10)
check("B later gets the original e2 but never the correction",
      recv_b.keys() == [K("e1"), K("c1"), K("e2")],
      recv_b.keys())

# ===========================================================================
# 5. Reconciliation of a correction starts at its real send, never at submit.
# ===========================================================================
client.post(f"/v1/destinations/{DA}/pause", json={"paused_from": None})
r = correct(e1, K("c3"), payload={"fixed": 3})
check("c3 created 201", r.status_code == 201, r.status_code)
c3_id = r.json()["id"]
c3_submitted = ts(r.json()["created_at"])
run_worker(10)  # B and C receive c3; A's copy waits behind the pause
d = delivery_for(c3_id, DA)
check("queued correction copy: no reconcile countdown at all",
      d["status"] == "pending" and d["reconcile_state"] == "none"
      and d["reconcile_deadline"] is None,
      (d["status"], d["reconcile_state"], d["reconcile_deadline"]))

time.sleep(settings.receipt_timeout_seconds + 0.4)
d = delivery_for(c3_id, DA)
check("after waiting longer than the receipt timeout: still no countdown",
      d["status"] == "pending" and d["reconcile_state"] == "none"
      and d["reconcile_deadline"] is None,
      (d["status"], d["reconcile_state"]))

client.post(f"/v1/destinations/{DA}/resume")
run_worker(10)
check("c3 delivered to A after the pause",
      K("c3") in recv_a.keys(), recv_a.keys())
d = delivery_for(c3_id, DA)
check("reconcile deadline counts from the real send, not submit",
      d["reconcile_state"] == "awaiting"
      and abs((ts(d["reconcile_deadline"]) - ts(d["delivered_at"])).total_seconds()
              - settings.receipt_timeout_seconds) < 1.0
      and (ts(d["delivered_at"]) - c3_submitted).total_seconds()
          > settings.receipt_timeout_seconds,
      (d["reconcile_deadline"], d["delivered_at"], str(c3_submitted)))
timed_out, _ = reconciler.sweep_once()
d = delivery_for(c3_id, DA)
check("sweep right after the send does not time the correction out",
      d["reconcile_state"] == "awaiting", d["reconcile_state"])

# ===========================================================================
# 6. A correction waits behind a copy currently in flight; the queue drains
#    in FIFO order.
# ===========================================================================
client.post(f"/v1/destinations/{DA}/pause", json={"paused_from": None})
r = push_event(K("e4"))
e4 = r.json()["id"]
r = correct(e1, K("c4"), payload={"fixed": 4})
check("c4 created 201", r.status_code == 201, r.status_code)
c4_id = r.json()["id"]
run_worker(10)  # neighbours drain; A holds both e4 and c4 behind the pause
check("paused A holds the original and the correction",
      K("e4") not in recv_a.keys() and K("c4") not in recv_a.keys(),
      recv_a.keys())

client.post(f"/v1/destinations/{DA}/resume")
# Simulate another worker delivering e4's copy right now: c4 must not jump.
e4_copy = delivery_for(e4, DA)
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
    {"id": e4_copy["id"]},
)
db.commit()
db.close()
run_worker(10)
check("correction does not cut ahead of the in-flight head",
      K("e4") not in recv_a.keys() and K("c4") not in recv_a.keys(),
      recv_a.keys())

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
    {"id": e4_copy["id"]},
)
db.commit()
db.close()
run_worker(10)
check("queue drains in FIFO order: original first, correction after",
      recv_a.keys()[-2:] == [K("e4"), K("c4")], recv_a.keys())

# ===========================================================================
# 7. A failed correction is accounted on its own: terminal 'failed', no
#    retry, no destination failure tally / isolation, no blocking, and the
#    original event's acknowledged standing is untouched.
# ===========================================================================
r = push_event(K("e5"))
e5 = r.json()["id"]
run_worker(10)
check("e5 delivered everywhere including D",
      K("e5") in recv_d.keys(), recv_d.keys())
for did in (DA, DB, DD):
    send_receipt(did, K("e5"))
e5_state = trace(e5)["event"]
check("e5 fully acknowledged before the correction",
      e5_state["reconcile_status"] == "acknowledged"
      and e5_state["acknowledged_count"] == 3,
      (e5_state["reconcile_status"], e5_state["acknowledged_count"]))

recv_d.fail_times = 10**9  # D now fails every real call
r = correct(e5, K("c5"), payload={"fixed": 5})
check("c5 created 201", r.status_code == 201, r.status_code)
c5_id = r.json()["id"]
run_worker(10)
check("c5 delivered to the healthy addresses",
      K("c5") in recv_a.keys() and K("c5") in recv_b.keys()
      and K("c5") in recv_c.keys(),
      (recv_a.keys(), recv_b.keys(), recv_c.keys()))
d = delivery_for(c5_id, DD)
check("failed correction copy is terminally 'failed' after one attempt",
      d["status"] == "failed" and d["attempts"] == 1
      and d["last_error"] is not None,
      (d["status"], d["attempts"], d["last_error"]))
st = destination_state(DD)
check("correction failure is not charged to the address: no tally, no isolation",
      st["failure_count"] == 0 and st["status"] == "active",
      (st["failure_count"], st["status"]))
e5_state = trace(e5)["event"]
check("original event stays acknowledged and delivered",
      e5_state["reconcile_status"] == "acknowledged"
      and e5_state["status"] == "delivered"
      and e5_state["acknowledged_count"] == 3
      and e5_state["failed_count"] == 0,
      (e5_state["reconcile_status"], e5_state["status"],
       e5_state["acknowledged_count"]))
check("original copies untouched by the correction failure",
      delivery_for(e5, DD)["reconcile_state"] == "acknowledged"
      and delivery_for(e5, DD)["status"] == "delivered",
      (delivery_for(e5, DD)["reconcile_state"], delivery_for(e5, DD)["status"]))
c5_state = trace(c5_id)["event"]
check("correction event reports its own failure",
      c5_state["status"] == "failed" and c5_state["failed_count"] == 1
      and c5_state["delivered_count"] == 2
      and c5_state["shadow_delivered_count"] == 1,
      (c5_state["status"], c5_state["failed_count"],
       c5_state["delivered_count"]))

run_worker(5)
d = delivery_for(c5_id, DD)
check("failed correction is not retried",
      d["status"] == "failed" and d["attempts"] == 1,
      (d["status"], d["attempts"]))

r = push_event(K("e6"))
e6 = r.json()["id"]
run_worker(8)
check("a failed correction does not block later copies of the address",
      K("e6") in recv_d.keys(), recv_d.keys())
check("healthy addresses keep draining normally",
      K("e6") in recv_a.keys() and K("e6") in recv_b.keys(),
      (recv_a.keys(), recv_b.keys()))

# ===========================================================================
# 8. Events that never went out — or never existed — cannot be corrected,
#    and are queryable as "no such thing", never as sent.
# ===========================================================================
r = push_event(K("e8"), event_type=ETYPE_NONE)
check("unrouted event accepted 201", r.status_code == 201, r.status_code)
e8 = r.json()["id"]
r = correct(e8, K("c8"))
check("unrouted event cannot be corrected (409)",
      r.status_code == 409, r.status_code)
r = client.get(f"/v1/events/{e8}/corrections")
check("unrouted event has no corrections on record",
      r.status_code == 200 and r.json() == [], (r.status_code, r.json()))
e8_trace = trace(e8)
check("unrouted event still reads as not sent, with no corrections",
      e8_trace["event"]["status"] == "unrouted"
      and e8_trace["deliveries"] == [] and e8_trace["corrections"] == [],
      (e8_trace["event"]["status"], e8_trace["corrections"]))

ghost = "00000000-0000-0000-0000-000000000000"
check("correcting a never-accepted event is 404",
      correct(ghost, K("c-ghost")).status_code == 404)
check("listing corrections of a never-accepted event is 404",
      client.get(f"/v1/events/{ghost}/corrections").status_code == 404)

# Subscribed but never confirmed: accepted, no copies, nothing to correct.
recv_f = MockReceiver()
r = client.post(
    "/v1/destinations",
    json={"url": recv_f.url(), "event_types": [ETYPE3]},
)
check("unconfirmed destination registered", r.status_code == 201, r.status_code)
r = push_event(K("e9"), event_type=ETYPE3)
check("pending-confirmation event accepted", r.status_code == 201, r.status_code)
e9 = r.json()["id"]
r = correct(e9, K("c9"))
check("event with no copies at all cannot be corrected (409)",
      r.status_code == 409, r.status_code)
check("pending-confirmation event has no corrections on record",
      client.get(f"/v1/events/{e9}/corrections").json() == [])

# Copies exist but none has gone out yet (address paused): also a 409, and
# the rejected correction does not burn its dedupe key.
client.post(f"/v1/destinations/{DA}/pause", json={"paused_from": None})
r = push_event(K("e10"), event_type=ETYPE2)
e10 = r.json()["id"]
run_worker(5)
check("e10 queued behind the pause, nothing delivered",
      K("e10") not in recv_a.keys(), recv_a.keys())
r = correct(e10, K("c10"))
check("nothing sent out yet: correction refused (409)",
      r.status_code == 409, r.status_code)
client.post(f"/v1/destinations/{DA}/resume")
run_worker(5)
check("e10 delivered after the resume",
      K("e10") in recv_a.keys(), recv_a.keys())
r = correct(e10, K("c10"), payload={"fixed": 10})
check("the same correction key is accepted once something really went out",
      r.status_code == 201 and r.json()["delivery_count"] == 1,
      (r.status_code, r.json().get("delivery_count")))
run_worker(5)
check("the resubmitted correction goes out",
      K("c10") in recv_a.keys(), recv_a.keys())

# A cancelled event never went out: it cannot be corrected either.
r = push_event(K("e11"), event_type=ETYPE2)
e11 = r.json()["id"]
r = client.post(f"/v1/events/{e11}/cancel")
check("e11 cancelled before send", r.status_code == 200, r.status_code)
r = correct(e11, K("c11"))
check("cancelled event cannot be corrected (409)",
      r.status_code == 409, r.status_code)
check("cancelled event has no corrections on record",
      client.get(f"/v1/events/{e11}/corrections").json() == [])

# ===========================================================================
# 9. The original event's correction list reflects everything submitted.
# ===========================================================================
r = client.get(f"/v1/events/{e1}/corrections")
check("e1 correction list has all three corrections in order",
      [c["dedupe_key"] for c in r.json()]
      == [K("c1"), K("c3"), K("c4")],
      [c["dedupe_key"] for c in r.json()])
check("every listed correction points at e1",
      all(c["corrects_event_id"] == e1 for c in r.json()),)
check("e1 trace corrections section matches",
      [c["dedupe_key"] for c in trace(e1)["corrections"]]
      == [K("c1"), K("c3"), K("c4")],
      [c["dedupe_key"] for c in trace(e1)["corrections"]])

print()
if failures:
    print(f"{len(failures)} CHECK(S) FAILED: {failures}")
    raise SystemExit(1)
print("ALL CORRECTION CHECKS PASSED")
