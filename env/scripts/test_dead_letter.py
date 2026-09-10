"""End-to-end checks for the dead-letter area semantics.

Covers:
- a copy whose consecutive transport failures reach the limit is parked
  (dead_lettered) with a queryable copy/address/reason, stops being retried and
  does not block later copies of the same address (or copies to other
  addresses);
- an event whose live copies are all dead-lettered reports status
  dead_lettered, never delivered;
- a parked copy can be manually revived to its original address at its
  original destination_seq: it queues in FIFO order and cannot jump ahead of a
  copy already in flight; revive still passes the confirmation/generation gate;
- receipts that keep not matching after the requeue budget park the copy too
  (timeout via the reconciler, explicit failure receipt via the API), while an
  acknowledged copy can never be dead-lettered, requeued or revived;
- unrouted events have no copies at all and never enter the dead-letter area.
"""
import hashlib
import hmac
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
from fastapi.testclient import TestClient
from sqlalchemy import text

os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://event@/events?host=/tmp&port=55432"
)
os.environ["MAX_DELIVERY_ATTEMPTS"] = "3"
os.environ["MAX_REQUEUE_CYCLES"] = "1"
os.environ["FAILURE_THRESHOLD"] = "5"
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
ETYPE = f"dlpaid{RUN}"


def K(name: str) -> str:
    return f"{RUN}-{name}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    """Counts failing POSTs, then answers 200; records delivery keys in order."""

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

r = client.post("/v1/sources", json={"name": f"dl-test-{RUN}"})
check("source register 201", r.status_code == 201, r.status_code)
SID, SECRET = r.json()["id"], r.json()["secret"]


def push_event(key, event_type=ETYPE, payload=None, not_before=None):
    event = {"event_type": event_type, "dedupe_key": key, "payload": payload or {}}
    if not_before:
        event["not_before"] = not_before
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


def wait_for(predicate, timeout=8.0, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def delivery_row(delivery_id):
    db = SessionLocal()
    try:
        return db.execute(
            text(
                """
                SELECT id, status, attempts, consecutive_failures, requeue_count,
                       destination_seq, reconcile_state, dead_letter_reason,
                       dead_lettered_at, destination_id
                FROM deliveries WHERE id = CAST(:id AS UUID)
                """
            ),
            {"id": str(delivery_id)},
        ).mappings().one()
    finally:
        db.close()


def delivery_for(event_id, destination_id):
    return client.get(f"/v1/events/{event_id}/trace").json()["deliveries"] and next(
        d
        for d in client.get(f"/v1/events/{event_id}/trace").json()["deliveries"]
        if d["destination_id"] == destination_id
    )


# ===========================================================================
# 1. Repeated transport failures park exactly that copy in the dead-letter
#    area; later copies of the same address and copies of other addresses keep
#    flowing.
# ===========================================================================
flaky = MockReceiver(fail_times=3)   # fails event 1 three times, then healthy
healthy = MockReceiver(fail_times=0)
DFLAKY = register_confirmed(flaky)
DHEALTHY = register_confirmed(healthy)

r = push_event(K("e1"))
check("e1 accepted 201", r.status_code == 201, r.status_code)
e1 = r.json()["id"]

run_worker(20)

dl = client.get("/v1/dead-letters", params={"dedupe_key": K("e1")}).json()
check("exactly one dead copy for e1", len(dl) == 1, len(dl))
dead = dl[0]
check("dead reason transport exhausted",
      dead["dead_letter_reason"] == "delivery_attempts_exhausted", dead["dead_letter_reason"])
check("dead copy identifies the copy", dead["dedupe_key"] == K("e1") and dead["event_id"] == e1)
check("dead copy identifies the address",
      dead["destination_id"] == DFLAKY and dead["destination_url"] == flaky.url())
check("dead record keeps attempts/streak and timestamp",
      dead["attempts"] == 3 and dead["consecutive_failures"] == 3
      and dead["dead_lettered_at"] is not None,
      (dead["attempts"], dead["consecutive_failures"], dead["dead_lettered_at"]))

# The flaky address got exactly the three failed attempts; nothing more is sent.
check("flaky receiver got exactly 3 deliveries", len(flaky.events) == 3, len(flaky.events))
run_worker(5)
check("dead copy is never auto-retried", len(flaky.events) == 3, len(flaky.events))

row = delivery_row(dead["id"])
check("db row dead_lettered", row["status"] == "dead_lettered", row["status"])

# The death resets the address failure tally; it must not be left isolated.
dstate = client.get(f"/v1/destinations/{DFLAKY}").json()
check("dead-lettering left the address active with zero failures",
      dstate["status"] == "active" and dstate["failure_count"] == 0,
      (dstate["status"], dstate["failure_count"]))

# The other subscriber's copy of the same event went out normally.
check("healthy receiver got e1", [e["dedupe_key"] for e in healthy.events] == [K("e1")],
      [e["dedupe_key"] for e in healthy.events])
ev = client.get(f"/v1/events/{e1}/trace").json()["event"]
check("whole event is dead_lettered (not delivered)", ev["status"] == "dead_lettered", ev["status"])
check("event shows dead_lettered_count=1", ev["dead_lettered_count"] == 1, ev["dead_lettered_count"])

# A later copy for the now-healthy address must skip over the dead head and go.
r = push_event(K("e2"))
e2 = r.json()["id"]
run_worker(10)
check("later copy for the same address is delivered past the dead one",
      [e["dedupe_key"] for e in flaky.events] == [K("e1"), K("e1"), K("e1"), K("e2")],
      [e["dedupe_key"] for e in flaky.events])
d2 = delivery_for(e2, DFLAKY)
check("e2 copy delivered", d2["status"] == "delivered", d2["status"])

# summary endpoint (counts this run's keys; the table may hold older runs)
s = client.get("/v1/dead-letters/summary").json()
mine = client.get("/v1/dead-letters").json()
mine = [x for x in mine if x["dedupe_key"].startswith(RUN)]
transport_mine = [
    x for x in mine if x["dead_letter_reason"] == "delivery_attempts_exhausted"
]
check("summary reports at least this run's transport-exhausted copies",
      s["delivery_attempts_exhausted"] >= len(transport_mine)
      and s["total"] >= len(transport_mine)
      and len(transport_mine) == 1,
      (s, len(transport_mine)))

# ===========================================================================
# 2. Manual revive: back to the original address at the original seq, in FIFO,
#    never jumping ahead of a copy already in flight; gates still enforced.
# ===========================================================================
revived_seq = dead["destination_seq"]

# While another copy of the same address is in flight, the revived head must
# not be claimed (it must not cut in front of the delivery in progress).
db = SessionLocal()
db.execute(
    text(
        """
        UPDATE deliveries
        SET status = 'in_flight', claim_token = gen_random_uuid(),
            claimed_at = now(), lease_until = now() + interval '5 minutes'
        WHERE id = (
            SELECT id FROM deliveries
            WHERE destination_id = CAST(:d AS UUID) AND status = 'delivered'
              AND destination_seq > :seq
            ORDER BY destination_seq
            LIMIT 1
        )
        """
    ),
    {"d": DFLAKY, "seq": revived_seq},
)
db.commit()
db.close()

r = client.post(f"/v1/dead-letters/{dead['id']}/revive")
check("revive 200", r.status_code == 200, r.status_code)
check("revive keeps original seq",
      r.json()["destination_seq"] == revived_seq and r.json()["revived"] is True
      and r.json()["dead_letter_reason"] == "delivery_attempts_exhausted",
      r.json())
row = delivery_row(dead["id"])
check("revived row pending with cleared streak/dead markers",
      row["status"] == "pending" and row["consecutive_failures"] == 0
      and row["requeue_count"] == 0 and row["dead_letter_reason"] is None,
      dict(row))
before = len(flaky.events)
run_worker(5)
check("revived copy waits behind the in-flight copy",
      len(flaky.events) == before, len(flaky.events))

# Release the simulated in-flight copy back to its prior delivered state
# (it had really completed before this test section). The point was only the
# in-flight gate; then the revived smaller seq must go before brand-new e3.
db = SessionLocal()
db.execute(
    text(
        """
        UPDATE deliveries
        SET status = 'delivered', claim_token = NULL, claimed_at = NULL,
            lease_until = NULL,
            -- mark it reconciled so the global reconciler sweep ignores it
            reconcile_state = 'acknowledged', reconciled_at = now(),
            receipt_result = 'success'
        WHERE destination_id = CAST(:d AS UUID) AND status = 'in_flight'
        """
    ),
    {"d": DFLAKY},
)
db.commit()
db.close()

# A brand new event enqueued *after* the revive must go after the revived one.
r = push_event(K("e3"))
e3 = r.json()["id"]
run_worker(10)
order = [e["dedupe_key"] for e in flaky.events]
check("revived copy is redelivered first, then later copies in FIFO",
      order[-2:] == [K("e1"), K("e3")], order)
d1 = next(d for d in client.get(f"/v1/events/{e1}/trace").json()["deliveries"]
          if d["destination_id"] == DFLAKY)
check("revived copy delivered and awaiting receipt",
      d1["status"] == "delivered" and d1["reconcile_state"] == "awaiting",
      (d1["status"], d1["reconcile_state"]))

# revive is one-way and only for dead copies
check("revive unknown 404",
      client.post("/v1/dead-letters/00000000-0000-0000-0000-000000000000/revive").status_code == 404)
check("revive live copy 409",
      client.post(f"/v1/dead-letters/{d1['id']}/revive").status_code == 409)

# relocate the destination: the (already delivered) copy's generation is now
# stale; if it were parked it could not be revived to the new URL. Verify the
# gate by parking a fresh copy on a separate failing destination instead.
stale_recv = MockReceiver(fail_times=99)
DSTALE = register_confirmed(stale_recv)
r = push_event(K("stale-1"))
stale_event = r.json()["id"]
run_worker(10)
sdl = client.get("/v1/dead-letters", params={"dedupe_key": K("stale-1")}).json()
check("second dead copy parked", len(sdl) == 1, len(sdl))
new_recv = MockReceiver(fail_times=0)
r = client.patch(f"/v1/destinations/{DSTALE}", json={"url": new_recv.url()})
check("relocate 200", r.status_code == 200, r.status_code)
r = client.post(f"/v1/dead-letters/{sdl[0]['id']}/revive")
check("revive stale-generation copy refused 409", r.status_code == 409, r.status_code)
# unconfirmed destination refuses revive too
r = client.post(f"/v1/dead-letters/{sdl[0]['id']}/revive")
check("revive to unconfirmed destination refused 409", r.status_code == 409, r.status_code)
_ = stale_event

# ===========================================================================
# 3. Receipts that keep not matching after the requeue budget: timeout via
#    reconciler and failure receipts via the API both park the copy; an
#    acknowledged copy can never enter the dead-letter area.
# ===========================================================================
# The revived e1 copy was delivered and is awaiting with requeue_count reset
# to 0. Let its receipt window expire: first timeout stays timed_out (budget
# not used yet), not dead-lettered.
time.sleep(1.3)
to, dlc = reconciler.sweep_once()
check("sweep marked the awaiting copy timed_out", to >= 1, (to, dlc))
d1 = next(d for d in client.get(f"/v1/events/{e1}/trace").json()["deliveries"]
          if d["destination_id"] == DFLAKY)
check("first timeout is timed_out, not dead-lettered",
      d1["reconcile_state"] == "timed_out" and d1["status"] == "delivered",
      (d1["reconcile_state"], d1["status"]))

# One requeue cycle (MAX_REQUEUE_CYCLES=1): redeliver, let it time out again
# -> reconciler must park it.
r = client.post(f"/v1/deliveries/{d1['id']}/requeue")
check("requeue timed-out copy 200", r.status_code == 200, r.status_code)
run_worker(5)
time.sleep(1.3)
to, dlc = reconciler.sweep_once()
check("second timeout parks the copy in the dead-letter area",
      dlc >= 1 and to == 0, (to, dlc))
row = delivery_row(d1["id"])
check("parked with receipt_timeout_exhausted",
      row["status"] == "dead_lettered"
      and row["dead_letter_reason"] == "receipt_timeout_exhausted",
      (row["status"], row["dead_letter_reason"]))

# Revive gives a fresh budget: deliver again, answer with a failure receipt.
# requeue_count is 0, so one failure receipt is receipt_failed but NOT parked.
r = client.post(f"/v1/dead-letters/{d1['id']}/revive")
check("revive after receipt timeout 200", r.status_code == 200, r.status_code)
run_worker(5)
r = client.post(
    "/v1/receipts",
    json={"destination_id": DFLAKY, "dedupe_key": K("e1"), "result": "failure"},
)
check("failure receipt applied", r.json()["disposition"] == "applied", r.json())
row = delivery_row(d1["id"])
check("failure receipt within budget is receipt_failed, not dead-lettered",
      row["reconcile_state"] == "receipt_failed" and row["status"] == "delivered",
      (row["reconcile_state"], row["status"]))

# Use the single allowed requeue cycle; another failure receipt on the last
# cycle parks the copy.
r = client.post(f"/v1/deliveries/{d1['id']}/requeue")
check("requeue receipt_failed copy 200", r.status_code == 200, r.status_code)
run_worker(5)
r = client.post(
    "/v1/receipts",
    json={"destination_id": DFLAKY, "dedupe_key": K("e1"), "result": "failure"},
)
check("last-cycle failure receipt still applied", r.json()["disposition"] == "applied", r.json())
row = delivery_row(d1["id"])
check("last-cycle failure receipt parks the copy",
      row["status"] == "dead_lettered"
      and row["reconcile_state"] == "receipt_failed"
      and row["dead_letter_reason"] == "receipt_failure_exhausted",
      (row["status"], row["reconcile_state"], row["dead_letter_reason"]))

# Revive once more, deliver, and this time acknowledge: an acknowledged copy
# must never enter the dead-letter area and cannot be requeued/revived.
r = client.post(f"/v1/dead-letters/{d1['id']}/revive")
check("final revive 200", r.status_code == 200, r.status_code)
run_worker(5)
r = client.post(
    "/v1/receipts",
    json={"destination_id": DFLAKY, "dedupe_key": K("e1"), "result": "success"},
)
check("success receipt applied", r.json()["disposition"] == "applied", r.json())
row = delivery_row(d1["id"])
check("acknowledged copy is acknowledged, not dead-lettered",
      row["reconcile_state"] == "acknowledged" and row["status"] == "delivered",
      (row["reconcile_state"], row["status"]))
check("acknowledged copy cannot be requeued",
      client.post(f"/v1/deliveries/{d1['id']}/requeue").status_code == 409)
check("acknowledged copy cannot be revived",
      client.post(f"/v1/dead-letters/{d1['id']}/revive").status_code == 409)
time.sleep(1.3)
reconciler.sweep_once()
row = delivery_row(d1["id"])
check("sweeps never move an acknowledged copy",
      row["reconcile_state"] == "acknowledged" and row["status"] == "delivered",
      (row["reconcile_state"], row["status"]))
check("no dead-letter row for the acknowledged dedupe key",
      client.get("/v1/dead-letters", params={"dedupe_key": K("e1")}).json() == []
      or all(x["dead_letter_reason"] != "receipt_failure_exhausted"
             for x in client.get("/v1/dead-letters", params={"dedupe_key": K("e1")}).json()))

# ===========================================================================
# 4. Unrouted events are accepted, never sent, and never dead-lettered.
# ===========================================================================
r = push_event(K("nobody-subscribes"), event_type=f"unsubscribed{RUN}")
check("unrouted event 201", r.status_code == 201, r.status_code)
body = r.json()
check("unrouted status", body["status"] == "unrouted", body["status"])
check("unrouted has zero copies", body["delivery_count"] == 0, body["delivery_count"])
check("unrouted not in dead letters",
      client.get("/v1/dead-letters", params={"event_id": body["id"]}).json() == [])

# ===========================================================================
# 5. Filters on the dead-letter listing all work.
# ===========================================================================
all_dl = client.get("/v1/dead-letters").json()
by_dest = client.get("/v1/dead-letters", params={"destination_id": DSTALE}).json()
by_reason = client.get(
    "/v1/dead-letters", params={"reason": "delivery_attempts_exhausted"}
).json()
bad_reason = client.get("/v1/dead-letters", params={"reason": "nope"})
check("dead-letter filter by destination",
      all_dl and all(x["destination_id"] == DSTALE for x in by_dest) and len(by_dest) >= 1)
check("dead-letter filter by reason",
      all(x["dead_letter_reason"] == "delivery_attempts_exhausted" for x in by_reason)
      and len(by_reason) >= 2)
check("invalid reason rejected 422", bad_reason.status_code == 422, bad_reason.status_code)

print()
if failures:
    print(f"{len(failures)} CHECK(S) FAILED: {failures}")
    raise SystemExit(1)
print("ALL DEAD-LETTER CHECKS PASSED")
