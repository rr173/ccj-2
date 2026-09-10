"""End-to-end checks for the per-event-type acknowledgement threshold
("认完门槛") semantics.

For each event type an operator may set how many FOR-REAL destinations must
acknowledge before the whole event counts as acknowledged. Observe-only
("shadow") copies never count toward that number. Once reached, the standing
is final: timeouts / failure receipts / dead-letter parking of the remaining
for-real copies never move the event back to pending, and the whole-event
"requeue unreconciled" endpoint becomes a no-op and no longer pulls a list.
Types without a threshold keep the old all-for-real-copies rule; types nobody
subscribes to are still ingested as unrouted and are never reported as sent or
acknowledged.
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
ETYPE = f"quorumpaid{RUN}"
ETYPE_DEFAULT = f"defaultpaid{RUN}"
ETYPE_SHADOW = f"quorumshadow{RUN}"
ETYPE_NOBODY = f"nobody{RUN}"
ETYPE_CHANGE = f"thresholdchange{RUN}"


def K(name: str) -> str:
    return f"{RUN}-{name}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    """Records deliveries; answers 2xx after fail_times failures."""

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

r = client.post("/v1/sources", json={"name": f"quorum-test-{RUN}"})
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


def drain_event(event_id, max_rounds=300):
    """Run the worker until this event has no queued/in-flight copies left.

    Fixed worker-round counts race the per-destination FIFO backlog and can
    leave a copy undelivered (making its receipt arrive 'premature'); poll the
    trace instead so receipts are only sent once transport really finished.
    """
    tr = None
    for _ in range(max_rounds):
        tr = trace(event_id)
        pending = [
            d for d in tr["deliveries"]
            if d["status"] in ("pending", "in_flight")
        ]
        if not pending:
            return tr
        worker.process_once()
    return tr


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
# 1. Threshold configuration endpoints: get/put/list/clear.
# ===========================================================================
r = client.get(f"/v1/event-types/{ETYPE}/ack-threshold")
check("unconfigured threshold reports null/configured=false",
      r.status_code == 200 and r.json()["ack_threshold"] is None
      and r.json()["configured"] is False, r.text)

r = client.put(f"/v1/event-types/{ETYPE}/ack-threshold", json={"ack_threshold": 2})
check("put threshold 200", r.status_code == 200
      and r.json()["ack_threshold"] == 2 and r.json()["configured"] is True, r.text)

r = client.put(f"/v1/event-types/{ETYPE}/ack-threshold", json={"ack_threshold": 0})
check("threshold 0 rejected 422", r.status_code == 422, r.status_code)
r = client.put(f"/v1/event-types/{ETYPE}/ack-threshold", json={"ack_threshold": -1})
check("negative threshold rejected 422", r.status_code == 422, r.status_code)

r = client.get(f"/v1/event-types/{ETYPE}/ack-threshold")
check("get reflects 2", r.json()["ack_threshold"] == 2)

r = client.get("/v1/event-types/ack-thresholds")
check("list contains the type",
      any(row["event_type"] == ETYPE and row["ack_threshold"] == 2
          for row in r.json()),
      [(x["event_type"], x["ack_threshold"]) for x in r.json()])

r = client.put(f"/v1/event-types/{ETYPE}/ack-threshold", json={"ack_threshold": None})
check("null put clears the threshold",
      r.status_code == 200 and r.json()["configured"] is False
      and r.json()["ack_threshold"] is None, r.text)
r = client.put(f"/v1/event-types/{ETYPE}/ack-threshold", json={"ack_threshold": 1})
check("set threshold to 1 for the test", r.status_code == 200)

# Threshold for the later-change checks: 1 of 2 needed.
r = client.put(f"/v1/event-types/{ETYPE_CHANGE}/ack-threshold",
               json={"ack_threshold": 1})
check("threshold-change type set to 1", r.status_code == 200)

# ===========================================================================
# 2. Threshold=1: one for-real ack finishes the whole event; the shadow's
#    receipt never counts; the remaining for-real copies' timeouts, failure
#    receipts and dead-letters never move the standing back.
# ===========================================================================
real_a = MockReceiver()
real_b = MockReceiver()
real_c = MockReceiver()
shadow_s = MockReceiver()
DA = register(real_a, event_types=[ETYPE, ETYPE_DEFAULT, ETYPE_CHANGE, ETYPE_SHADOW])
DB = register(real_b, event_types=[ETYPE, ETYPE_DEFAULT, ETYPE_CHANGE, ETYPE_SHADOW])
DC = register(real_c, event_types=[ETYPE, ETYPE_DEFAULT, ETYPE_CHANGE, ETYPE_SHADOW])
DS = register(shadow_s, observe_only=True,
              event_types=[ETYPE, ETYPE_DEFAULT, ETYPE_CHANGE, ETYPE_SHADOW])

r = push_event(K("q1"))
check("q1 accepted 201", r.status_code == 201, r.status_code)
q1 = r.json()["id"]
ev = r.json()
check("q1 snapshot: threshold=1, required=1",
      ev["ack_threshold"] == 1 and ev["required_ack_count"] == 1
      and ev["acknowledged_quorum"] is False
      and ev["delivery_count"] == 3 and ev["shadow_delivery_count"] == 1
      and ev["reconcile_status"] == "pending",
      {k: ev.get(k) for k in ("ack_threshold", "required_ack_count",
                              "acknowledged_quorum", "delivery_count",
                              "shadow_delivery_count", "reconcile_status")})

drain_event(q1)

# The shadow acknowledges first: it must not finish the event.
r = receipt(DS, K("q1"))
check("shadow receipt applied to its own copy",
      r.json()["disposition"] == "applied", r.text)
ev = trace(q1)["event"]
check("shadow ack alone never reaches the quorum",
      ev["reconcile_status"] == "pending"
      and ev["acknowledged_count"] == 0 and ev["shadow_acknowledged_count"] == 1
      and ev["acknowledged_quorum"] is False,
      {k: ev[k] for k in ("reconcile_status", "acknowledged_count",
                          "shadow_acknowledged_count", "acknowledged_quorum")})

# One for-real destination acknowledges: whole event is acknowledged at once.
r = receipt(DA, K("q1"))
check("first real receipt applied", r.json()["disposition"] == "applied", r.text)

# Another real copy explicitly fails with a failure receipt inside its window.
r = receipt(DC, K("q1"), result="failure")
check("copy C failure receipt applied as receipt_failed",
      r.json()["disposition"] == "applied", r.text)

ev = trace(q1)["event"]
check("whole event acknowledged once 1 real copy acks (threshold=1)",
      ev["reconcile_status"] == "acknowledged"
      and ev["acknowledged_count"] == 1 and ev["unacknowledged_count"] == 2
      and ev["acknowledged_quorum"] is True
      and ev["required_ack_count"] == 1,
      {k: ev[k] for k in ("reconcile_status", "acknowledged_count",
                          "unacknowledged_count", "acknowledged_quorum",
                          "required_ack_count")})

# The remaining copies run their own lifecycle out: copy B (and the shadow)
# pass their reconcile deadline. The shadow parks after its requeue budget;
# the for-real copy B stays visible as timed_out and is requeueable.
time.sleep(1.3)
timed_out, parked = reconciler.sweep_once()
check("sweeper runs", timed_out >= 0 and parked >= 0, (timed_out, parked))

tr = trace(q1)
ev = tr["event"]
check("remaining copies ended unreconciled/dead on their own",
      copy_for(tr, DC)["reconcile_state"] == "receipt_failed"
      and copy_for(tr, DB)["reconcile_state"] == "timed_out",
      [(copy_for(tr, DB)["reconcile_state"],
        copy_for(tr, DC)["reconcile_state"])])
check("whole event STAYS acknowledged after timeout/failure/dead-letter",
      ev["reconcile_status"] == "acknowledged"
      and ev["acknowledged_quorum"] is True,
      {k: ev[k] for k in ("reconcile_status", "acknowledged_quorum",
                          "acknowledged_count", "unacknowledged_count")})

# ===========================================================================
# 3. Whole-event requeue after the quorum is reached pulls no list.
# ===========================================================================
r = client.post(f"/v1/events/{q1}/requeue-unreconciled")
body = r.json()
check("event-level requeue is a no-op after quorum",
      r.status_code == 200 and body["requeued_count"] == 0
      and body["deliveries"] == [] and body["already_acknowledged"] is True,
      body)
# The other copies were not touched by the no-op.
tr = trace(q1)
check("no-op requeue left the unacked copies alone",
      copy_for(tr, DC)["reconcile_state"] == "receipt_failed"
      and copy_for(tr, DB)["reconcile_state"] == "timed_out")

# Per-delivery/per-destination requeue still manage a remaining copy on its
# own lifecycle (whole-event no-op must not lock them out).
b_id = copy_for(tr, DB)["id"]
r = client.post(f"/v1/deliveries/{b_id}/requeue")
check("a remaining copy is still individually requeueable",
      r.status_code == 200 and r.json()["requeued"] is True, r.text)

# ===========================================================================
# 4. Before the quorum is reached, event-level requeue lists unacked for-real
#    copies only — the shadow copy is never counted — and a shadow ack does
#    not end the event.
# ===========================================================================
r = push_event(K("q2"))
q2 = r.json()["id"]
drain_event(q2)
# Nobody for-real acks; all copies time out (shadow included).
time.sleep(1.3)
reconciler.sweep_once()
tr = trace(q2)
check("q2 every copy timed out",
      all(d["reconcile_state"] == "timed_out" for d in tr["deliveries"]))
r = client.post(f"/v1/events/{q2}/requeue-unreconciled")
body = r.json()
ids = {x["destination_id"] for x in body["deliveries"]}
check("q2 not at quorum yet: for-real timed-out copies listed",
      body["already_acknowledged"] is False
      and ids == {DA, DB, DC} and DS not in ids and body["requeued_count"] == 3,
      body)
check("q2 shadow copy stays timed out (not in the whole-event list)",
      copy_for(trace(q2), DS)["reconcile_state"] == "timed_out"
      and copy_for(trace(q2), DS)["status"] == "delivered")

drain_event(q2)
# One real ack reaches threshold; the other two are still awaiting.
receipt(DA, K("q2"))
ev = trace(q2)["event"]
check("q2 acknowledged with one real ack while two real copies are open",
      ev["reconcile_status"] == "acknowledged"
      and ev["acknowledged_quorum"] is True
      and ev["unacknowledged_count"] == 2,
      {k: ev[k] for k in ("reconcile_status", "unacknowledged_count")})
# Now the whole-event requeue is a no-op even though two copies are awaiting.
r = client.post(f"/v1/events/{q2}/requeue-unreconciled")
check("q2 whole-event requeue no longer pulls a list",
      r.json()["requeued_count"] == 0
      and r.json()["already_acknowledged"] is True, r.json())

# Partially acknowledged below the quorum: threshold 2 on a fresh event type
# would need two acks. Raise ETYPE threshold to 2 for a new event — existing
# events keep their snapshot (checked in section 7).
client.put(f"/v1/event-types/{ETYPE}/ack-threshold", json={"ack_threshold": 2})
r = push_event(K("q3"))
q3 = r.json()["id"]
check("q3 snapshot picks up the new threshold (2)",
      r.json()["required_ack_count"] == 2 and r.json()["ack_threshold"] == 2,
      {k: r.json().get(k) for k in ("required_ack_count", "ack_threshold")})
drain_event(q3)
receipt(DA, K("q3"))
ev = trace(q3)["event"]
check("q3 one real ack with threshold 2 -> partially_acknowledged",
      ev["reconcile_status"] == "partially_acknowledged"
      and ev["acknowledged_quorum"] is False
      and ev["unacknowledged_count"] == 2,
      {k: ev[k] for k in ("reconcile_status", "unacknowledged_count")})
# None of the open copies is terminal, so the whole-event requeue selects
# nothing (awaiting copies are never re-thrown early).
r = client.post(f"/v1/events/{q3}/requeue-unreconciled")
check("q3 partial: no terminal copies -> empty list, not acknowledged-flagged",
      r.json()["requeued_count"] == 0
      and r.json()["already_acknowledged"] is False, r.json())
receipt(DB, K("q3"))
check("q3 second real ack -> acknowledged",
      trace(q3)["event"]["reconcile_status"] == "acknowledged")
# Failure receipt on the third copy cannot drag it back.
receipt(DC, K("q3"), result="failure")
ev = trace(q3)["event"]
check("q3 stays acknowledged after the third copy fails",
      ev["reconcile_status"] == "acknowledged"
      and ev["acknowledged_quorum"] is True, ev["reconcile_status"])

# ===========================================================================
# 5. Types with NO threshold keep the all-for-real-copies rule.
# ===========================================================================
r = push_event(K("d1"), event_type=ETYPE_DEFAULT)
d1 = r.json()["id"]
check("unthresholded type: required equals all 3 real copies",
      r.json()["ack_threshold"] is None
      and r.json()["required_ack_count"] == 3,
      {k: r.json().get(k) for k in ("ack_threshold", "required_ack_count")})
drain_event(d1)
receipt(DA, K("d1"))
receipt(DB, K("d1"))
ev = trace(d1)["event"]
check("default type partial after 2 of 3",
      ev["reconcile_status"] == "partially_acknowledged"
      and ev["acknowledged_quorum"] is False,
      ev["reconcile_status"])
receipt(DS, K("d1"))  # shadow ack, must not help
ev = trace(d1)["event"]
check("shadow ack does not complete the default-rule event",
      ev["reconcile_status"] == "partially_acknowledged"
      and ev["unacknowledged_count"] == 1,
      ev["reconcile_status"])
receipt(DC, K("d1"))
check("default type acknowledged only after the 3rd real ack",
      trace(d1)["event"]["reconcile_status"] == "acknowledged")

# A threshold larger than the for-real subscriber count is capped, so the
# event can still finish (but does not finish merely because it was accepted).
client.put(f"/v1/event-types/{ETYPE_SHADOW}/ack-threshold",
           json={"ack_threshold": 99})
r = push_event(K("s1"), event_type=ETYPE_SHADOW)
s1 = r.json()["id"]
check("oversized threshold capped at the 3 real subscribers",
      r.json()["required_ack_count"] == 3 and r.json()["ack_threshold"] == 99,
      {k: r.json().get(k) for k in ("required_ack_count", "ack_threshold")})
drain_event(s1)
check("s1 pending after delivery, not auto-acknowledged",
      trace(s1)["event"]["reconcile_status"] == "pending")

# ===========================================================================
# 6. Shadow-only and nobody-subscribed types stay ingest-only and are never
#    reported as acknowledged or sent.
# ===========================================================================
r = push_event(K("n1"), event_type=ETYPE_NOBODY)
check("unrouted event still 201", r.status_code == 201, r.status_code)
body = r.json()
check("nobody subscribes -> unrouted, zero copies, not acknowledged",
      body["status"] == "unrouted" and body["delivery_count"] == 0
      and body["shadow_delivery_count"] == 0
      and body["required_ack_count"] == 0
      and body["acknowledged_quorum"] is False
      and body["reconcile_status"] == "pending",
      {k: body.get(k) for k in ("status", "delivery_count",
                                "shadow_delivery_count", "required_ack_count",
                                "acknowledged_quorum", "reconcile_status")})
attempts = client.get(
    "/v1/ingestion/attempts", params={"dedupe_key": K("n1")}
).json()
check("unrouted admission recorded as unrouted",
      any(a["disposition"] == "unrouted" for a in attempts),
      [a["disposition"] for a in attempts])
# And the whole-event requeue on an unrouted event is harmless: no copies,
# and it must not claim "already acknowledged".
r = client.post(f"/v1/events/{body['id']}/requeue-unreconciled")
check("unrouted event requeue is empty and not flagged acknowledged",
      r.status_code == 200 and r.json()["requeued_count"] == 0
      and r.json()["already_acknowledged"] is False, r.json())

# Shadow-only type (threshold=1, but only shadows subscribe): set up a type
# with a threshold whose sole subscriber is observe-only.
OTYPE = f"onlyshadowq{RUN}"
client.put(f"/v1/event-types/{OTYPE}/ack-threshold", json={"ack_threshold": 1})
only_shadow = MockReceiver()
DONLY = register(only_shadow, observe_only=True, event_types=[OTYPE])
r = push_event(K("o1"), event_type=OTYPE)
o1 = r.json()["id"]
check("shadow-only quorum event ingested, zero real copies",
      r.status_code == 201 and r.json()["delivery_count"] == 0
      and r.json()["shadow_delivery_count"] == 1
      and r.json()["required_ack_count"] == 0
      and r.json()["reconcile_status"] == "pending",
      {k: r.json().get(k) for k in ("delivery_count", "shadow_delivery_count",
                                    "required_ack_count", "reconcile_status")})
drain_event(o1)
receipt(DONLY, K("o1"))
ev = trace(o1)["event"]
check("shadow-only ack never acknowledges the event even with threshold=1",
      ev["reconcile_status"] == "pending" and ev["acknowledged_quorum"] is False
      and ev["acknowledged_count"] == 0,
      {k: ev[k] for k in ("reconcile_status", "acknowledged_quorum",
                          "acknowledged_count")})

# ===========================================================================
# 7. Changing/deleting the threshold only affects later events; accepted
#    events keep their snapshot and stay (or become) acknowledged per it.
# ===========================================================================
# ETYPE_CHANGE was configured to 1 before DA/DB subscribed. Event c1 ingests
# with required=1.
r = push_event(K("c1"), event_type=ETYPE_CHANGE)
c1 = r.json()["id"]
check("c1 snapshotted required=1",
      r.json()["required_ack_count"] == 1
      and r.json()["ack_threshold"] == 1,
      {k: r.json().get(k) for k in ("required_ack_count", "ack_threshold")})
# Raise the type threshold to 2 before c1 finishes: c1 must keep required=1.
r = client.put(f"/v1/event-types/{ETYPE_CHANGE}/ack-threshold",
               json={"ack_threshold": 2})
check("threshold raised to 2", r.status_code == 200 and r.json()["ack_threshold"] == 2)
drain_event(c1)
receipt(DA, K("c1"))
ev = trace(c1)["event"]
check("c1 finishes at 1 ack per its old snapshot despite the new threshold",
      ev["reconcile_status"] == "acknowledged"
      and ev["required_ack_count"] == 1,
      {k: ev[k] for k in ("reconcile_status", "required_ack_count")})

r = push_event(K("c2"), event_type=ETYPE_CHANGE)
c2 = r.json()["id"]
check("c2 ingests under the new threshold (2)",
      r.json()["required_ack_count"] == 2
      and r.json()["ack_threshold"] == 2)

# Clear the threshold entirely: new events fall back to all-real-copies.
r = client.delete(f"/v1/event-types/{ETYPE_CHANGE}/ack-threshold")
check("delete clears the threshold",
      r.status_code == 200 and r.json()["configured"] is False, r.text)
r = client.get(f"/v1/event-types/{ETYPE_CHANGE}/ack-threshold")
check("get after delete is null", r.json()["ack_threshold"] is None
      and r.json()["configured"] is False)
r = push_event(K("c3"), event_type=ETYPE_CHANGE)
c3 = r.json()["id"]
check("c3 uses the default rule (required=3) after the clear",
      r.json()["ack_threshold"] is None
      and r.json()["required_ack_count"] == 3,
      {k: r.json().get(k) for k in ("ack_threshold", "required_ack_count")})
# c2 still requires 2; finish it under its own snapshot.
drain_event(c2)
drain_event(c3)
receipt(DA, K("c2"))
receipt(DB, K("c2"))
_c2tr = trace(c2)
check("c2 acknowledged per its snapshot=2",
      _c2tr["event"]["reconcile_status"] == "acknowledged",
      {**{k: _c2tr["event"][k] for k in ("reconcile_status", "required_ack_count",
                                         "acknowledged_count", "ack_threshold",
                                         "acknowledged_quorum")},
       "copies": [(d["destination_id"] == DA, d["destination_id"] == DB,
                   d["observe_only"], d["status"], d["reconcile_state"])
                  for d in _c2tr["deliveries"]]})
# c3 needs all three.
receipt(DA, K("c3"))
receipt(DB, K("c3"))
ev = trace(c3)["event"]
check("c3 partial after 2 under the default rule",
      ev["reconcile_status"] == "partially_acknowledged", ev["reconcile_status"])
receipt(DC, K("c3"))
check("c3 acknowledged after the third real ack",
      trace(c3)["event"]["reconcile_status"] == "acknowledged")

print()
if failures:
    print(f"{len(failures)} CHECK(S) FAILED: {failures}")
    raise SystemExit(1)
print("ALL ACK-THRESHOLD CHECKS PASSED")
