"""End-to-end checks for observe-only ("shadow", "just watching") destinations.

A shadow destination still gets its own copy of every subscribed event and
runs the normal delivery / retry / quarantine / dead-letter / receipt
lifecycle on that copy, but it must never change whether the whole event
counts as acknowledged:

- its success/failure receipts and its receipt timeouts only reconcile its
  own copy; for-real copies and the whole-event status ignore them;
- event-level "requeue the still-unacknowledged copies" never lists shadow
  copies (the copy itself stays requeueable directly/per-destination);
- isolating or dead-lettering the shadow never blocks a for-real destination
  subscribed to the same type, and never parks the whole event;
- the trace reports which copies are for-real and which are just watching,
  and a shadow ack can never be reported as the whole event acknowledged;
- destinations created without the flag behave exactly as before;
- a type nobody for-real subscribes to is still ingested normally and is
  never reported as already sent.
"""
import hashlib
import hmac
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fastapi.testclient import TestClient

os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://event@/events?host=/tmp&port=55432"
)
os.environ["MAX_DELIVERY_ATTEMPTS"] = "3"
os.environ["MAX_REQUEUE_CYCLES"] = "1"
os.environ["FAILURE_THRESHOLD"] = "2"
os.environ["QUARANTINE_SECONDS"] = "0"
os.environ["RETRY_BACKOFF_BASE_SECONDS"] = "0"
os.environ["RECEIPT_TIMEOUT_SECONDS"] = "1"
os.environ["CONFIRM_BACKOFF_BASE_SECONDS"] = "0"
os.environ["CONFIRM_POLL_INTERVAL_SECONDS"] = "0.1"

from app.db import SessionLocal, build_engine  # noqa: E402
from app.models import init_db  # noqa: E402
from app import main as api  # noqa: E402
import app.worker as worker  # noqa: E402
from app import reconciler  # noqa: E402

failures: list[str] = []
RUN = str(int(time.time() * 1000))[-9:]
ETYPE = f"shadowpaid{RUN}"


def K(name: str) -> str:
    return f"{RUN}-{name}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    """Records delivery keys in order; answers 2xx after fail_times failures."""

    def __init__(self, fail_times=0, auto_confirm=True):
        self.fail_times = fail_times
        self.auto_confirm = auto_confirm
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
                    if state.auto_confirm:
                        self._write(200, {"echo": body["challenge"]})
                    else:
                        self._write(200, {"received": True})
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

r = client.post("/v1/sources", json={"name": f"shadow-test-{RUN}"})
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


def register(recv: MockReceiver, *, observe_only=None, event_types=None):
    body = {"url": recv.url(), "event_types": event_types or [ETYPE]}
    if observe_only is not None:
        body["observe_only"] = observe_only
    r = client.post("/v1/destinations", json=body)
    assert r.status_code == 201, r.text
    did = r.json()["id"]
    hc = __import__("httpx").Client(timeout=5)
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


def run_worker(times=30):
    for _ in range(times):
        worker.process_once()


def trace(event_id):
    return client.get(f"/v1/events/{event_id}/trace").json()


def copy_for(tr, destination_id):
    return next(d for d in tr["deliveries"] if d["destination_id"] == destination_id)


def receipt(destination_id, key, result="success"):
    return client.post(
        "/v1/receipts",
        json={"destination_id": destination_id, "dedupe_key": key, "result": result},
    )


# ===========================================================================
# 1. Registration marks a destination observe-only; unmarked stays for-real.
# ===========================================================================
real_recv = MockReceiver()
shadow_recv = MockReceiver()
DREAL = register(real_recv)
DSHADOW = register(shadow_recv, observe_only=True)

check("unmarked destination is for-real",
      client.get(f"/v1/destinations/{DREAL}").json()["observe_only"] is False)
check("shadow destination reports observe_only",
      client.get(f"/v1/destinations/{DSHADOW}").json()["observe_only"] is True)

# Re-registering the shadow URL without the flag keeps it shadow (None = keep).
r = client.post(
    "/v1/destinations",
    json={"url": shadow_recv.url(), "event_types": [ETYPE]},
)
check("re-register keeps observe_only and stays confirmed",
      r.status_code == 201 and r.json()["observe_only"] is True
      and r.json()["confirmation_state"] == "confirmed",
      (r.status_code, r.json().get("observe_only"), r.json().get("confirmation_state")))

# ===========================================================================
# 2. Both still get their own copy; only the shadow's receipt must never
#    decide the whole event, and it cannot drag an acked event back.
# ===========================================================================
r = push_event(K("e1"))
check("e1 accepted 201", r.status_code == 201, r.status_code)
e1 = r.json()["id"]
ev = r.json()
check("e1 fan-out splits real vs shadow counts",
      ev["delivery_count"] == 1 and ev["shadow_delivery_count"] == 1
      and ev["status"] == "pending" and ev["reconcile_status"] == "pending",
      {k: ev[k] for k in ("delivery_count", "shadow_delivery_count", "status",
                          "reconcile_status")})

run_worker(30)
check("real receiver got e1", [e["dedupe_key"] for e in real_recv.events] == [K("e1")])
check("shadow receiver got e1", [e["dedupe_key"] for e in shadow_recv.events] == [K("e1")])

tr = trace(e1)
real_copy = copy_for(tr, DREAL)
shadow_copy = copy_for(tr, DSHADOW)
check("trace flags the copies correctly",
      real_copy["observe_only"] is False and shadow_copy["observe_only"] is True,
      [(d["destination_id"], d["observe_only"]) for d in tr["deliveries"]])
check("both copies transported",
      real_copy["status"] == "delivered" and shadow_copy["status"] == "delivered")

ev = trace(e1)["event"]
check("event delivered (real copy got 2xx)", ev["status"] == "delivered", ev["status"])
check("no receipt yet -> reconcile pending, counts separated",
      ev["reconcile_status"] == "pending"
      and ev["acknowledged_count"] == 0 and ev["unacknowledged_count"] == 1
      and ev["shadow_acknowledged_count"] == 0
      and ev["shadow_unacknowledged_count"] == 1,
      {k: ev[k] for k in ("reconcile_status", "acknowledged_count",
                          "unacknowledged_count", "shadow_acknowledged_count",
                          "shadow_unacknowledged_count")})

# The shadow acknowledges first: its own copy reconciles, the whole event does not.
r = receipt(DSHADOW, K("e1"))
check("shadow receipt applied to its own copy",
      r.status_code == 200 and r.json()["disposition"] == "applied", r.text)
tr = trace(e1)
check("shadow copy acknowledged", copy_for(tr, DSHADOW)["reconcile_state"] == "acknowledged")
ev = tr["event"]
check("shadow ack does NOT acknowledge the whole event",
      ev["reconcile_status"] == "pending"
      and ev["acknowledged_count"] == 0 and ev["unacknowledged_count"] == 1
      and ev["shadow_acknowledged_count"] == 1
      and ev["shadow_unacknowledged_count"] == 0,
      {k: ev[k] for k in ("reconcile_status", "acknowledged_count",
                          "unacknowledged_count", "shadow_acknowledged_count")})

# Now the for-real copy acknowledges: the whole event is acknowledged.
r = receipt(DREAL, K("e1"))
check("real receipt applied", r.status_code == 200, r.text)
ev = trace(e1)["event"]
check("event acknowledged only via the for-real copy",
      ev["reconcile_status"] == "acknowledged"
      and ev["acknowledged_count"] == 1 and ev["unacknowledged_count"] == 0,
      {k: ev[k] for k in ("reconcile_status", "acknowledged_count",
                          "unacknowledged_count")})

# ===========================================================================
# 3. A shadow timeout / failure receipt cannot drag an acknowledged event
#    back; event-level requeue never lists its copy.
# ===========================================================================
r = push_event(K("e2"))
e2 = r.json()["id"]
run_worker(30)
r = receipt(DREAL, K("e2"))
check("e2 real receipt applied", r.json()["disposition"] == "applied")
# Shadow deliberately answers failure inside its window.
r = receipt(DSHADOW, K("e2"), result="failure")
check("e2 shadow failure receipt applied to the shadow copy",
      r.json()["disposition"] == "applied", r.text)
ev = trace(e2)["event"]
check("shadow failure receipt leaves the whole event acknowledged",
      ev["reconcile_status"] == "acknowledged"
      and copy_for(trace(e2), DSHADOW)["reconcile_state"] == "receipt_failed",
      ev["reconcile_status"])

# A fresh event where the shadow times out after the real copy acked.
r = push_event(K("e2b"))
e2b = r.json()["id"]
run_worker(30)
receipt(DREAL, K("e2b"))
time.sleep(1.3)
timed_out, _dead = reconciler.sweep_once()
check("sweeper ran", timed_out >= 0)
tr = trace(e2b)
check("shadow copy timed out on its own",
      copy_for(tr, DSHADOW)["reconcile_state"] == "timed_out")
ev = tr["event"]
check("shadow timeout leaves the whole event acknowledged and delivered",
      ev["reconcile_status"] == "acknowledged" and ev["status"] == "delivered"
      and ev["acknowledged_count"] == 1,
      {k: ev[k] for k in ("reconcile_status", "status", "acknowledged_count")})

# Event-level requeue must NOT pick the shadow copy.
r = client.post(f"/v1/events/{e2b}/requeue-unreconciled")
check("event-level requeue ignores the timed-out shadow copy",
      r.status_code == 200 and r.json()["requeued_count"] == 0, r.text)
# The shadow copy itself is still directly requeueable on its own lifecycle.
shadow_delivery = copy_for(trace(e2b), DSHADOW)["id"]
r = client.post(f"/v1/deliveries/{shadow_delivery}/requeue")
check("the shadow copy can still be requeued directly",
      r.status_code == 200 and r.json()["requeued"] is True, r.text)

# ===========================================================================
# 4. Event-level requeue of an unacked event lists for-real copies only.
# ===========================================================================
r = push_event(K("e3"))
e3 = r.json()["id"]
run_worker(30)
time.sleep(1.3)
reconciler.sweep_once()
tr = trace(e3)
check("e3 both copies timed out",
      copy_for(tr, DREAL)["reconcile_state"] == "timed_out"
      and copy_for(tr, DSHADOW)["reconcile_state"] == "timed_out")
r = client.post(f"/v1/events/{e3}/requeue-unreconciled")
body = r.json()
requeued_ids = {x["destination_id"] for x in body["deliveries"]}
check("event-level requeue lists for-real copies but never the shadow copy",
      r.status_code == 200 and DREAL in requeued_ids and DSHADOW not in requeued_ids
      and body["requeued_count"] == len(requeued_ids),
      body)
# The shadow's timed-out copy stays timed out (not silently requeued).
check("shadow copy left untouched by the event-level requeue",
      copy_for(trace(e3), DSHADOW)["reconcile_state"] == "timed_out"
      and copy_for(trace(e3), DSHADOW)["status"] == "delivered")
# Per-destination requeue still handles the shadow's own copies (this is
# the whole-destination endpoint, so it may additionally pick up the shadow's
# earlier receipt_failed copy — what matters is that e3's shadow copy is
# among the requeued ones and goes back to its own queue).
r = client.post(f"/v1/destinations/{DSHADOW}/requeue-unreconciled")
check("per-destination requeue still requeues the shadow copy",
      r.status_code == 200 and r.json()["requeued_count"] >= 1, r.text)
check("e3's shadow copy is back on its own queue",
      copy_for(trace(e3), DSHADOW)["status"] == "pending"
      and copy_for(trace(e3), DSHADOW)["reconcile_state"] == "none",
      (copy_for(trace(e3), DSHADOW)["status"],
       copy_for(trace(e3), DSHADOW)["reconcile_state"]))
run_worker(30)
receipt(DREAL, K("e3"))
ev = trace(e3)["event"]
check("whole event acknowledges on the real copy while shadow is still open",
      ev["reconcile_status"] == "acknowledged"
      and copy_for(trace(e3), DSHADOW)["reconcile_state"] == "awaiting",
      (ev["reconcile_status"], copy_for(trace(e3), DSHADOW)["reconcile_state"]))

# ===========================================================================
# 5. A failing shadow gets isolated/dead-lettered on its own and never blocks
#    the for-real address subscribed to the same type; the whole event is
#    never reported dead-lettered because of it.
# ===========================================================================
bad_shadow = MockReceiver(fail_times=999)
DBAD = register(bad_shadow, observe_only=True)

r = push_event(K("e4"))
e4 = r.json()["id"]
# Drain everything: healthy copies succeed; the bad shadow's own copy burns
# its three attempts (quarantine is 0s so isolation auto-recovers as an
# immediate probe) and parks itself in the dead-letter area.
run_worker(40)
tr = trace(e4)
real4 = copy_for(tr, DREAL)
check("failing shadow does not block the for-real address",
      real4["status"] == "delivered"
      and [e["dedupe_key"] for e in real_recv.events][-1:] == [K("e4")],
      real4["status"])
ev = tr["event"]
check("shadow failures do not make the event unrouted or undelivered",
      ev["status"] == "delivered" and ev["dead_lettered_count"] == 0
      and ev["shadow_delivery_count"] == 2,
      {k: ev.get(k) for k in ("status", "dead_lettered_count",
                              "shadow_delivery_count")})

dead = client.get("/v1/dead-letters", params={"dedupe_key": K("e4")}).json()
check("exactly the shadow copy dead-lettered for e4",
      len(dead) == 1 and dead[0]["destination_id"] == DBAD
      and dead[0]["observe_only"] is True,
      [(d["destination_id"], d["observe_only"]) for d in dead])
tr = trace(e4)
check("trace marks the parked copy as shadow",
      copy_for(tr, DBAD)["status"] == "dead_lettered"
      and copy_for(tr, DBAD)["observe_only"] is True)
ev = tr["event"]
check("a dead shadow copy never makes the whole event dead-lettered",
      ev["status"] == "delivered" and ev["dead_lettered_count"] == 0
      and ev["shadow_dead_lettered_count"] == 1,
      {k: ev.get(k) for k in ("status", "dead_lettered_count",
                              "shadow_dead_lettered_count")})

# Now explicitly stop (isolate) the shadow address with a quarantine wall in
# the future. Later events must still fan out to it (its own copy waits) while
# the for-real address keeps flowing uninterrupted.
from sqlalchemy import text as _text  # noqa: E402
_db = SessionLocal()
_db.execute(
    _text(
        """
        UPDATE destinations
        SET status = 'isolated', failure_count = 5,
            recoverable_at = now() + interval '1 hour'
        WHERE id = CAST(:d AS UUID)
        """
    ),
    {"d": DBAD},
)
_db.commit()
_db.close()

r = push_event(K("e5"))
e5 = r.json()["id"]
run_worker(30)
tr = trace(e5)
check("later event still delivers to the for-real address",
      copy_for(tr, DREAL)["status"] == "delivered")
shadow5 = copy_for(tr, DBAD)
check("isolated shadow still gets its own queued copy (never claimed yet)",
      shadow5["observe_only"] is True and shadow5["status"] == "pending",
      (shadow5["observe_only"], shadow5["status"]))
check("isolated shadow does not change the whole event's standing",
      tr["event"]["status"] == "delivered"
      and tr["event"]["dead_lettered_count"] == 0,
      tr["event"]["status"])

# ===========================================================================
# 6. Toggling shadow mode only affects copies fanned out afterwards.
# ===========================================================================
r = client.patch(f"/v1/destinations/{DSHADOW}", json={"observe_only": False})
check("patch flips observe_only without re-arming the handshake",
      r.status_code == 200 and r.json()["observe_only"] is False
      and r.json()["confirmation_state"] == "confirmed",
      (r.status_code, r.json().get("observe_only"),
       r.json().get("confirmation_state")))
r = push_event(K("e6"))
e6 = r.json()["id"]
run_worker(30)
tr = trace(e6)
new_copy = copy_for(tr, DSHADOW)
check("copies fanned out after the toggle are for-real",
      new_copy["observe_only"] is False,
      new_copy["observe_only"])
check("older copies keep the snapshot they were born with",
      copy_for(trace(e1), DSHADOW)["observe_only"] is True)
# Now there are two for-real destinations (DREAL + retoggled DSHADOW).
receipt(DREAL, K("e6"))
ev0 = trace(e6)["event"]
check("partially acked until the (now for-real) retoggled copy acks",
      ev0["reconcile_status"] == "partially_acknowledged"
      and ev0["delivery_count"] == 2,
      (ev0["reconcile_status"], ev0["delivery_count"]))
receipt(DSHADOW, K("e6"))
check("both for-real copies ack -> acknowledged",
      trace(e6)["event"]["reconcile_status"] == "acknowledged")

# Toggle back via idempotent re-registration too, without re-handshaking.
r = client.post(
    "/v1/destinations",
    json={"url": shadow_recv.url(), "observe_only": True},
)
check("re-registration retoggles to shadow and keeps confirmation",
      r.status_code == 201 and r.json()["observe_only"] is True
      and r.json()["confirmation_state"] == "confirmed", r.text)

# A no-op patch is rejected like the old no-op patch.
r = client.patch(f"/v1/destinations/{DSHADOW}", json={})
check("empty patch rejected 422", r.status_code == 422, r.status_code)
# observe_only-only patch is accepted.
r = client.patch(f"/v1/destinations/{DREAL}", json={"observe_only": False})
check("observe_only-only patch accepted", r.status_code == 200, r.status_code)

# ===========================================================================
# 7. Only-shadow subscribers: ingested and actually sent, but the event can
#    never become acknowledged; nobody-subscribed types stay unrouted.
# ===========================================================================
only_shadow_recv = MockReceiver()
DONLY = register(only_shadow_recv, observe_only=True,
                 event_types=[f"onlyshadow{RUN}"])
OTYPE = f"onlyshadow{RUN}"
r = push_event(K("o1"), event_type=OTYPE)
check("shadow-only event 201", r.status_code == 201, r.status_code)
body = r.json()
check("shadow-only event is not unrouted at ingest",
      body["status"] == "pending" and body["delivery_count"] == 0
      and body["shadow_delivery_count"] == 1
      and body["reconcile_status"] == "pending",
      {k: body[k] for k in ("status", "delivery_count",
                            "shadow_delivery_count", "reconcile_status")})
run_worker(20)
ev = client.get(f"/v1/events/{body['id']}").json() if False else trace(body["id"])["event"]
check("shadow-only event transports but never becomes acknowledged",
      ev["status"] == "delivered" and ev["reconcile_status"] == "pending",
      (ev["status"], ev["reconcile_status"]))
receipt(DONLY, K("o1"))
ev = trace(body["id"])["event"]
check("a shadow-only ack is still not a whole-event acknowledgement",
      ev["reconcile_status"] == "pending"
      and ev["shadow_acknowledged_count"] == 1 and ev["acknowledged_count"] == 0,
      {k: ev[k] for k in ("reconcile_status", "shadow_acknowledged_count",
                          "acknowledged_count")})
# The admission record says accepted (it really went out to a subscriber),
# not unrouted.
attempts = client.get(
    "/v1/ingestion/attempts", params={"dedupe_key": K("o1")}
).json()
check("shadow-only admission is accepted, not unrouted",
      any(a["disposition"] == "accepted" for a in attempts),
      [a["disposition"] for a in attempts])

r = push_event(K("nobody"), event_type=f"unsubscribed{RUN}")
check("unrouted event still 201", r.status_code == 201, r.status_code)
body = r.json()
check("nobody subscribes -> unrouted with zero copies of either kind",
      body["status"] == "unrouted" and body["delivery_count"] == 0
      and body["shadow_delivery_count"] == 0,
      {k: body[k] for k in ("status", "delivery_count",
                            "shadow_delivery_count")})

# ===========================================================================
# 8. A shadow's own dead-letter copy is queryable and revivable like any
#    other copy (its lifecycle is entirely its own).
# ===========================================================================
dl = client.get("/v1/dead-letters", params={"dedupe_key": K("e4")}).json()[0]
r = client.post(f"/v1/dead-letters/{dl['id']}/revive")
check("shadow dead copy is manually revivable",
      r.status_code == 200 and r.json()["revived"] is True, r.text)

print()
if failures:
    print(f"{len(failures)} CHECK(S) FAILED: {failures}")
    raise SystemExit(1)
print("ALL OBSERVE-ONLY CHECKS PASSED")
