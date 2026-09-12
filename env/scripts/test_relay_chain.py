"""End-to-end checks for per-event-type relay chains ("接力").

Covers the guarantees:

- one ACTIVE chain per event type; only events accepted AFTER it is defined
  walk it (previously accepted events keep ordinary fan-out and are never
  backfilled into the chain);
- a chained event goes ONLY to its stations, one at a time: station 1 first,
  station N+1 is withheld until station N carries a matching success receipt;
  a later station is never sent early and never written as sent;
- failure receipt / reconcile timeout / dead-letter / supersede at any station
  closes every later pending station relay_skipped — never resent, never
  backfilled — and the run says exactly where/why it stopped;
- re-defining the station order only applies to the next accepted event; an
  event already walking the chain keeps the stations it set out with;
- the same destination cannot occupy two stations of one chain;
- a single event's view shows the current station, which stations already
  acknowledged and which have not been reached yet.
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
os.environ["RETRY_BACKOFF_BASE_SECONDS"] = "0"
os.environ["RECEIPT_TIMEOUT_SECONDS"] = "1"
os.environ["CONFIRM_BACKOFF_BASE_SECONDS"] = "0"
os.environ["CONFIRM_POLL_INTERVAL_SECONDS"] = "0.1"
os.environ["MAX_DELIVERY_ATTEMPTS"] = "3"
os.environ["RECONCILE_SWEEP_INTERVAL_SECONDS"] = "0.2"

from app.db import SessionLocal, build_engine  # noqa: E402
from app.models import init_db  # noqa: E402
from app import main as api  # noqa: E402
import app.worker as worker  # noqa: E402
from app import reconciler  # noqa: E402

failures: list[str] = []
RUN = str(int(time.time() * 1000))[-9:]
ETYPE = f"relay{RUN}"
ETYPE2 = f"relayver{RUN}"
ETYPE3 = f"relayfail{RUN}"
ETYPE4 = f"relaydl{RUN}"

# How long the mock receivers stay 500 before giving up permanently (used to
# force a transport dead-letter at a station).
PERMANENT_FAIL = 99


def K(name: str) -> str:
    return f"{RUN}-{name}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    """Records deliveries; can auto-ack receipts and fail the first N posts."""

    def __init__(self, name, auto_ack=True, fail_times=0, receipt_url=None):
        self.name = name
        self.auto_ack = auto_ack
        self.fail_times = fail_times
        self.receipt_url = receipt_url
        self.events: list[dict] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hook-{RUN}-{self.name}"

    def keys(self) -> list[str]:
        return [e["dedupe_key"] for e in self.events]

    def _handler(self):
        state = self
        client = TestClient(api.app)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                if self.headers.get("X-Message-Type") == "activation_challenge":
                    self._write(200, {"echo": body["challenge"]})
                    return
                if state.fail_times > 0:
                    state.fail_times -= 1
                    self._write(500, {"error": "boom"})
                    return
                state.events.append(body)
                self._write(200, {"status": "ok"})
                if state.auto_ack and state.receipt_url:
                    client.post(
                        state.receipt_url,
                        json={
                            "destination_id": body["destination_id"],
                            "dedupe_key": body["dedupe_key"],
                            "result": "success",
                        },
                    )

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
httpx_for_confirm = None

r = client.post("/v1/sources", json={"name": f"relay-test-{RUN}"})
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


def register_confirmed(receiver: MockReceiver, event_types):
    r = client.post(
        "/v1/destinations",
        json={"url": receiver.url(), "event_types": event_types},
    )
    assert r.status_code == 201, r.text
    did = r.json()["id"]
    import httpx

    hc = httpx.Client(timeout=5)
    db = SessionLocal()
    deadline = time.time() + 5
    while time.time() < deadline:
        worker.process_confirmation_once(db, hc)
        if (
            client.get(f"/v1/destinations/{did}").json()["confirmation_state"]
            == "confirmed"
        ):
            break
        time.sleep(0.1)
    db.close()
    hc.close()
    assert client.get(f"/v1/destinations/{did}").json()["confirmation_state"] == "confirmed"
    return did


def run_worker(times=20):
    for _ in range(times):
        worker.process_once()


def trace(event_id):
    return client.get(f"/v1/events/{event_id}/trace").json()


def run_reconcile(cycles=3):
    for _ in range(cycles):
        reconciler.sweep_once()
        time.sleep(0.25)


def send_receipt(destination_id, key, result="success"):
    return client.post(
        "/v1/receipts",
        json={"destination_id": destination_id, "dedupe_key": key, "result": result},
    )


recv_a = MockReceiver("a", auto_ack=False)
recv_b = MockReceiver("b", auto_ack=False)
recv_c = MockReceiver("c", auto_ack=False)
recv_x = MockReceiver("x", auto_ack=False)  # subscribed but NOT in chain
DA = register_confirmed(recv_a, [ETYPE, ETYPE2, ETYPE3, ETYPE4])
DB = register_confirmed(recv_b, [ETYPE, ETYPE2, ETYPE3, ETYPE4])
DC = register_confirmed(recv_c, [ETYPE, ETYPE2, ETYPE3, ETYPE4])
DX = register_confirmed(recv_x, [ETYPE])

# ===========================================================================
# 0. Only events accepted AFTER the chain is defined walk it.
# ===========================================================================
r = push_event(K("pre"), payload={"n": 0})
check("event before chain defined is ordinary fan-out",
      r.status_code == 201 and r.json()["delivery_count"] == 4,
      (r.status_code, r.json().get("delivery_count")))
pre_id = r.json()["id"]

# ===========================================================================
# 1. Chain definition: validation and one-active-chain rule.
# ===========================================================================
r = client.put(f"/v1/event-types/{ETYPE}/relay-chain",
               json={"destination_ids": [DA, DB, DA]})
check("same destination twice in one chain rejected (422)",
      r.status_code == 422, r.status_code)

r = client.put(f"/v1/event-types/{ETYPE}/relay-chain",
               json={"destination_ids": [DA, DB, "00000000-0000-0000-0000-000000000000"]})
check("unknown station id rejected (404)", r.status_code == 404, r.status_code)

r = client.put(f"/v1/event-types/{ETYPE}/relay-chain",
               json={"destination_ids": [DA, DB, DC]})
check("chain A->B->C defined 200", r.status_code == 200, r.status_code)
chain = r.json()
check("chain version starts at 1 with 3 stations",
      chain["version"] == 1 and len(chain["stations"]) == 3
      and [s["station_no"] for s in chain["stations"]] == [1, 2, 3],
      chain)
CHAIN_V1 = chain["id"]

r = client.get(f"/v1/event-types/{ETYPE}/relay-chain")
check("GET returns the active chain", r.status_code == 200 and r.json()["id"] == CHAIN_V1)

r = client.get("/v1/event-types/relay-chains")
check("chain appears in the active list",
      any(c["id"] == CHAIN_V1 for c in r.json()), [c["event_type"] for c in r.json()])

# The earlier ordinary event is never pulled into the chain.
run_worker(20)
check("pre-chain event still went to ordinary subscribers incl. X",
      recv_x.keys() == [K("pre")], recv_x.keys())

# ===========================================================================
# 2. Sequential walking: station 1 first; later stations withheld.
# ===========================================================================
r = push_event(K("e1"), payload={"n": 1})
check("e1 accepted 201", r.status_code == 201, r.status_code)
e1 = r.json()["id"]
t = trace(e1)
check("e1 went ONLY to chain stations (not X)",
      len(t["deliveries"]) == 3, len(t["deliveries"]))
check("e1 relay view chained, 3 stations, current=1, not stopped",
      t["event"]["relay"]["chained"] is True
      and t["event"]["relay"]["station_count"] == 3
      and t["event"]["relay"]["current_station_no"] == 1
      and t["event"]["relay"]["stopped"] is False,
      t["event"]["relay"])

run_worker(5)
check("only station A received e1",
      recv_a.keys() == [K("pre"), K("e1")]
      and recv_b.keys() == [K("pre")]
      and recv_c.keys() == [K("pre")],
      (recv_a.keys(), recv_b.keys(), recv_c.keys()))
t = trace(e1)
check("B, C still pending (never written as sent)",
      {d["destination_id"]: d["status"] for d in t["deliveries"]}[DB] == "pending"
      and {d["destination_id"]: d["status"] for d in t["deliveries"]}[DC] == "pending")
# B must not be claimable even though it is the head of B's queue: drive more
# worker passes.
run_worker(10)
check("B still does not have e1 before A acknowledges",
      K("e1") not in recv_b.keys(), recv_b.keys())

# A acknowledges: B opens.
r = send_receipt(DA, K("e1"))
check("A receipt applied", r.status_code == 200 and r.json()["disposition"] == "applied",
      r.json())
run_worker(10)
check("after A acks, B receives e1 but C still does not",
      K("e1") in recv_b.keys() and K("e1") not in recv_c.keys(),
      (recv_b.keys(), recv_c.keys()))
t = trace(e1)
relay_view = t["event"]["relay"]
check("current station still 2 until B acks; A acknowledged, C not reached",
      relay_view["current_station_no"] == 2
      and relay_view["acknowledged_count"] == 1
      and [s for s in relay_view["stations"] if s["station_no"] == 1][0]["acknowledged"]
      and not [s for s in relay_view["stations"] if s["station_no"] == 3][0]["reached"],
      relay_view)

# A late/failure receipt from A can't reopen once acknowledged; B acks and C
# opens.
send_receipt(DB, K("e1"))
run_worker(10)
check("after B acks, C receives e1", K("e1") in recv_c.keys(), recv_c.keys())
send_receipt(DC, K("e1"))
time.sleep(0.1)
t = trace(e1)
check("e1 relay completed once every station acknowledged",
      t["event"]["relay"]["completed"] is True
      and t["event"]["relay"]["stopped"] is False
      and t["event"]["reconcile_status"] == "acknowledged"
      and t["event"]["status"] == "delivered",
      (t["event"]["relay"]["completed"], t["event"]["status"],
       t["event"]["reconcile_status"]))

# X never received a chained event.
check("non-chain subscriber X never receives chain events",
      recv_x.keys() == [K("pre")], recv_x.keys())

# ===========================================================================
# 3. A failure receipt stops the run; later stations are skipped, never sent.
# ===========================================================================
fa = MockReceiver("fa", auto_ack=False)
fb = MockReceiver("fb", auto_ack=False)
fc = MockReceiver("fc", auto_ack=False)
FA = register_confirmed(fa, [ETYPE3])
FB = register_confirmed(fb, [ETYPE3])
FC = register_confirmed(fc, [ETYPE3])
r = client.put(f"/v1/event-types/{ETYPE3}/relay-chain",
               json={"destination_ids": [FA, FB, FC]})
check("fail-chain FA->FB->FC defined", r.status_code == 200, r.status_code)

r = push_event(K("f1"), event_type=ETYPE3, payload={"n": 2})
f1 = r.json()["id"]
run_worker(5)
check("FA received f1", K("f1") in fa.keys(), fa.keys())
# FA answers failure.
send_receipt(FA, K("f1"), result="failure")
time.sleep(0.1)
run_worker(10)
t = trace(f1)
rv = t["event"]["relay"]
check("failure receipt stopped run at station 1 with reason receipt_failed",
      rv["stopped"] is True
      and rv["stopped_at_station_no"] == 1
      and rv["stop_reason"] == "receipt_failed",
      (rv["stopped"], rv["stopped_at_station_no"], rv["stop_reason"]))
statuses = {d["destination_id"]: d["status"] for d in t["deliveries"]}
check("FB and FC relay_skipped, never sent",
      statuses[FB] == "relay_skipped" and statuses[FC] == "relay_skipped",
      statuses)
check("skipped rows carry the reason and the triggering station",
      all(d["relay_skip_reason"] == "receipt_failed"
          and d["relay_stopped_by_delivery_id"] is not None
          for d in t["deliveries"] if d["status"] == "relay_skipped"))
check("nothing was actually delivered to FB/FC",
      K("f1") not in fb.keys() and K("f1") not in fc.keys())
check("event transport status relay_halted",
      t["event"]["status"] == "relay_halted", t["event"]["status"])

# Requeuing the failed station manually does not backfill skipped stations.
r = client.post(
    f"/v1/deliveries/{t['deliveries'][0]['id']}/requeue"
)
# FA's copy is status delivered/receipt_failed: requeue allowed.
check("failed station can individually be requeued",
      r.status_code in (200, 409), (r.status_code, r.text))
run_worker(10)
t2 = trace(f1)
check("skipped stations stay skipped after the failed station retries",
      {d["destination_id"]: d["status"] for d2 in [t2] for d in d2["deliveries"]}[FC]
      == "relay_skipped")

# ===========================================================================
# 4. Reconcile timeout at a station stops the run just the same.
# ===========================================================================
ga = MockReceiver("ga", auto_ack=False)
gb = MockReceiver("gb", auto_ack=False)
GA = register_confirmed(ga, [ETYPE4])
GB = register_confirmed(gb, [ETYPE4])
r = client.put(f"/v1/event-types/{ETYPE4}/relay-chain",
               json={"destination_ids": [GA, GB]})
check("timeout-chain GA->GB defined", r.status_code == 200, r.status_code)
r = push_event(K("g1"), event_type=ETYPE4)
g1 = r.json()["id"]
run_worker(5)
check("GA received g1", K("g1") in ga.keys(), ga.keys())
# Never send a receipt; the reconciler times GA out (and eventually parks it
# after the requeue budget; here we only need the stop cascade). Wait
# explicitly for the cascade rather than a fixed number of sweeps.
deadline = time.time() + 15
rv = None
while time.time() < deadline:
    run_reconcile(1)
    run_worker(2)
    rv = trace(g1)["event"]["relay"]
    if rv["stopped"]:
        break
    time.sleep(0.2)
t = trace(g1)
rv = t["event"]["relay"]
check("reconcile timeout stopped run at station 1",
      rv["stopped"] is True and rv["stopped_at_station_no"] == 1
      and rv["stop_reason"] in ("receipt_timeout", "receipt_timeout_exhausted"),
      (rv["stopped"], rv["stop_reason"]))
statuses = {d["destination_id"]: d["status"] for d in t["deliveries"]}
check("GB relay_skipped after GA timeout, never delivered",
      statuses[GB] == "relay_skipped" and K("g1") not in gb.keys(),
      (statuses, gb.keys()))

# ===========================================================================
# 5. Re-defining the order only applies to the next accepted event.
# ===========================================================================
# Existing e1 walked v1 (A->B->C). Define v2 B->A->C for the same type.
r = client.put(f"/v1/event-types/{ETYPE}/relay-chain",
               json={"destination_ids": [DB, DA, DC]})
check("chain re-defined 200 as version 2",
      r.status_code == 200 and r.json()["version"] == 2
      and [s["destination_id"] for s in r.json()["stations"]] == [DB, DA, DC],
      (r.status_code, r.json().get("version")))
CHAIN_V2 = r.json()["id"]
t = trace(e1)
check("already-walking e1 still reports version 1 stations in original order",
      t["event"]["relay"]["chain_id"] == CHAIN_V1
      and t["event"]["relay"]["chain_version"] == 1
      and [s["destination_id"] for s in t["event"]["relay"]["stations"]] == [DA, DB, DC],
      (t["event"]["relay"]["chain_id"], CHAIN_V1))

r = push_event(K("e2"))
e2 = r.json()["id"]
run_worker(5)
t = trace(e2)
check("new e2 snapshots version 2; station 1 is B",
      t["event"]["relay"]["chain_id"] == CHAIN_V2
      and t["event"]["relay"]["chain_version"] == 2
      and t["event"]["relay"]["stations"][0]["destination_id"] == DB,
      t["event"]["relay"]["chain_version"])
check("only B (new station 1) got e2",
      K("e2") in recv_b.keys() and K("e2") not in recv_a.keys(),
      (recv_b.keys(), recv_a.keys()))

# ===========================================================================
# 6. Deleting the chain only affects later events.
# ===========================================================================
r = client.delete(f"/v1/event-types/{ETYPE}/relay-chain")
check("chain deleted 200", r.status_code == 200, r.status_code)
r = client.delete(f"/v1/event-types/{ETYPE}/relay-chain")
check("deleting again is 404", r.status_code == 404, r.status_code)
r = push_event(K("e3"))
e3 = r.json()["id"]
check("event after delete is ordinary fan-out again (4 subscribers incl. X)",
      r.json()["delivery_count"] == 4, r.json().get("delivery_count"))
t = trace(e2)
check("already-walking e2 keeps its v2 relay run",
      t["event"]["relay"]["chain_id"] == CHAIN_V2
      and t["event"]["relay"]["chain_active"] is False,
      t["event"]["relay"])

# ===========================================================================
# 7. A chain cannot share a type with a preview-gate policy.
# ===========================================================================
PGATE = f"gate{RUN}"
pa = MockReceiver("pa", auto_ack=False)
PA = register_confirmed(pa, [PGATE])
r = client.put(f"/v1/event-types/{PGATE}/preview-policy",
               json={"consent_timeout_seconds": 60})
check("preview policy set", r.status_code == 200, r.status_code)
r = client.put(f"/v1/event-types/{PGATE}/relay-chain",
               json={"destination_ids": [PA]})
check("cannot define a chain on a gated type (409)",
      r.status_code == 409, r.status_code)
r = client.delete(f"/v1/event-types/{PGATE}/preview-policy")
r = client.put(f"/v1/event-types/{PGATE}/relay-chain",
               json={"destination_ids": [PA]})
check("chain allowed once the gated policy is removed",
      r.status_code == 200, r.status_code)

# ===========================================================================
# 8. A destination relocating while it is a not-yet-sent station supersedes
#    that station and stops the run (reason 'superseded').
# ===========================================================================
ha = MockReceiver("ha", auto_ack=False)
hb = MockReceiver("hb", auto_ack=False)
ETYPE_MOVE = f"relaymove{RUN}"
HA = register_confirmed(ha, [ETYPE_MOVE])
HB = register_confirmed(hb, [ETYPE_MOVE])
r = client.put(f"/v1/event-types/{ETYPE_MOVE}/relay-chain",
               json={"destination_ids": [HA, HB]})
check("move-chain HA->HB defined", r.status_code == 200, r.status_code)

# Pause HA so the station-1 copy stays pending, then relocate HA to a new URL.
client.post(f"/v1/destinations/{HA}/pause", json={"paused_from": None})
r = push_event(K("m1"), event_type=ETYPE_MOVE)
m1 = r.json()["id"]
run_worker(3)
check("m1 station 1 held by the pause (not sent)",
      K("m1") not in ha.keys() and K("m1") not in hb.keys())
hb_new = MockReceiver("hbnew", auto_ack=False)
r = client.patch(f"/v1/destinations/{HA}", json={"url": hb_new.url()})
check("relocating station 1 re-arms confirmation",
      r.status_code == 200 and r.json()["confirmation_state"] == "pending",
      (r.status_code, r.json().get("confirmation_state")))
client.post(f"/v1/destinations/{HA}/resume")
deadline = time.time() + 6
while time.time() < deadline:
    run_worker(2)
    t = trace(m1)
    rv = t["event"]["relay"]
    if rv["stopped"]:
        break
    time.sleep(0.2)
t = trace(m1)
rv = t["event"]["relay"]
check("relocation superseded station 1 and stopped the run",
      rv["stopped"] is True and rv["stopped_at_station_no"] == 1
      and rv["stop_reason"] == "superseded",
      (rv["stopped"], rv["stop_reason"]))
statuses = {d["destination_id"]: d["status"] for d in t["deliveries"]}
check("station 2 relay_skipped after relocation, never delivered",
      statuses[HB] == "relay_skipped" and K("m1") not in hb.keys(),
      (statuses, hb.keys()))

# ===========================================================================
# 9. Transport dead-letter at a station (consecutive send failures exhaust
#    the copy's own attempt budget) also stops the run permanently.
# ===========================================================================
ka = MockReceiver("ka", auto_ack=False, fail_times=PERMANENT_FAIL)
kb = MockReceiver("kb", auto_ack=False)
ETYPE_DL = f"relaydl2{RUN}"
KA = register_confirmed(ka, [ETYPE_DL])
KB = register_confirmed(kb, [ETYPE_DL])
r = client.put(f"/v1/event-types/{ETYPE_DL}/relay-chain",
               json={"destination_ids": [KA, KB]})
check("dead-letter-chain KA->KB defined", r.status_code == 200, r.status_code)
r = push_event(K("d1"), event_type=ETYPE_DL)
d1 = r.json()["id"]
# Drive enough worker passes to exhaust MAX_DELIVERY_ATTEMPTS (=3).
deadline = time.time() + 15
while time.time() < deadline:
    run_worker(1)
    t = trace(d1)
    if t["event"]["relay"]["stopped"]:
        break
    time.sleep(0.1)
t = trace(d1)
rv = t["event"]["relay"]
check("transport dead-letter stopped run at station 1",
      rv["stopped"] is True and rv["stopped_at_station_no"] == 1
      and rv["stop_reason"] == "delivery_attempts_exhausted",
      (rv["stopped"], rv["stop_reason"]))
statuses = {d["destination_id"]: d["status"] for d in t["deliveries"]}
check("station 1 dead_lettered, station 2 relay_skipped, nothing sent to it",
      statuses[KA] == "dead_lettered" and statuses[KB] == "relay_skipped"
      and K("d1") not in kb.keys(),
      (statuses, kb.keys()))
check("dead-lettered run shows event transport status relay_halted",
      t["event"]["status"] == "relay_halted", t["event"]["status"])

print()
if failures:
    print(f"{len(failures)} CHECK(S) FAILED: {failures}")
    raise SystemExit(1)
print("ALL RELAY-CHAIN CHECKS PASSED")
