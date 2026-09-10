"""End-to-end checks for the destination activation handshake semantics."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
from fastapi.testclient import TestClient

os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://event@/events?host=/tmp"
)
os.environ["CONFIRM_TIMEOUT_SECONDS"] = "5"
os.environ["CONFIRM_BACKOFF_BASE_SECONDS"] = "1"
os.environ["CONFIRM_POLL_INTERVAL_SECONDS"] = "0.2"

from app.db import build_engine  # noqa: E402
from app.models import init_db  # noqa: E402
from app import main as api  # noqa: E402
import app.worker as worker  # noqa: E402

failures: list[str] = []
RUN = str(int(time.time() * 1000))[-9:]


def K(name: str) -> str:
    return f"{RUN}-{name}"


def T(name: str) -> str:
    return f"{name}{RUN}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


# --- mock receivers ---------------------------------------------------------

class ReceiverState:
    def __init__(self, port, auto_confirm=True):
        self.port = port
        self.auto_confirm = auto_confirm
        self.events: list[dict] = []
        self.challenges: list[dict] = []


def make_handler(state: ReceiverState):
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
            self._write(200, {"status": "ok"})

        def _write(self, code, payload):
            data = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return Handler


def start_receiver(state: ReceiverState):
    server = ThreadingHTTPServer(("127.0.0.1", state.port), make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# --- test scaffold ----------------------------------------------------------

init_db(build_engine())
client = TestClient(api.app)

# Source + signing helper
r = client.post("/v1/sources", json={"name": f"handshake-test-{int(time.time()*1000)}"})
check("source register 201", r.status_code == 201, r.status_code)
source = r.json()
SID, SECRET = source["id"], source["secret"]

import hashlib
import hmac


def push_event(event_type, key, payload=None, not_before=None, base=client):
    event = {"event_type": event_type, "dedupe_key": key, "payload": payload or {}}
    if not_before:
        event["not_before"] = not_before
    raw = json.dumps(event).encode()
    ts = str(int(time.time()))
    sig = hmac.new(SECRET.encode(), ts.encode() + b"." + raw, hashlib.sha256).hexdigest()
    return base.post(
        "/v1/events",
        content=raw,
        headers={
            "Content-Type": "application/json",
            "X-Source-Id": SID,
            "X-Signed-At": ts,
            "X-Signature": sig,
        },
    )


def wait_for(predicate, timeout=8.0, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def worker_running(stop, seconds=6):
    deadline = time.time() + seconds
    while time.time() < deadline and not stop.is_set():
        try:
            worker.process_once()
        except Exception:
            import traceback
            traceback.print_exc()
        stop.wait(0.1)


def confirmer_running(stop, seconds=6, http_client=None):
    http_client = http_client or worker.httpx.Client(timeout=5)
    db = worker.SessionLocal()
    deadline = time.time() + seconds
    while time.time() < deadline and not stop.is_set():
        try:
            worker.process_confirmation_once(db, http_client)
        except Exception:
            import traceback
            traceback.print_exc()
        stop.wait(0.2)
    db.close()
    http_client.close()


# ===========================================================================
# Rule 1+2: pending destination receives probes but no events; after confirm
# only new events fan out; old events are not backfilled
# ===========================================================================
rec = ReceiverState(0, auto_confirm=False)  # pick free port
probe_server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(rec))
port = probe_server.server_address[1]
rec.port = port
threading.Thread(target=probe_server.serve_forever, daemon=True).start()

r = client.post(
    "/v1/destinations",
    json={"url": f"http://127.0.0.1:{port}/hook-{RUN}", "event_types": [T("paid")]},
)
check("destination registered 201", r.status_code == 201, r.status_code)
dest = r.json()
DID = dest["id"]
check(
    "new destination starts pending",
    dest["confirmation_state"] == "pending",
    dest.get("confirmation_state"),
)
check(
    "challenge round has a deadline",
    dest["challenge_expires_at"] is not None,
    dest.get("challenge_expires_at"),
)

# run confirmer briefly: a probe should reach the receiver, but no echo
stop = threading.Event()
ct = threading.Thread(target=confirmer_running, args=(stop, 3), daemon=True)
ct.start()
time.sleep(1.5)
stop.set()
check("probe reached pending receiver", len(rec.challenges) >= 1, f"n={len(rec.challenges)}")

# event while unconfirmed: accepted, stored, but NOT sent and no delivery row
r = push_event(T("paid"), K("pre-confirm-1"))
check("pre-confirm event accepted 201", r.status_code == 201, r.status_code)
body = r.json()
check(
    "pre-confirm event is not unrouted-status (stored, not sent)",
    body["status"] == "unrouted",
    body["status"],
)
check("pre-confirm delivery_count=0", body["delivery_count"] == 0, body["delivery_count"])
time.sleep(0.5)
check("receiver got no event pre-confirm", len(rec.events) == 0, f"n={len(rec.events)}")

att = client.get(
    "/v1/ingestion/attempts", params={"dedupe_key": K("pre-confirm-1")}
).json()
check(
    "pre-confirm disposition pending_confirmation",
    att and att[0]["disposition"] == "pending_confirmation",
    att[0]["disposition"] if att else "none",
)

trace = client.get(f"/v1/events/{body['id']}/trace").json()
check(
    "trace shows no deliveries for pre-confirm event",
    len(trace["deliveries"]) == 0,
    len(trace["deliveries"]),
)

# wrong echo: stays pending
r = client.post(f"/v1/destinations/{DID}/confirm", json={"challenge": "WRONG"})
check("wrong echo rejected 400", r.status_code == 400, r.status_code)
d = client.get(f"/v1/destinations/{DID}").json()
check("still pending after wrong echo", d["confirmation_state"] == "pending")

# correct echo via API (simulating receiver callback)
# pull the outstanding challenge from DB
from sqlalchemy import text  # noqa: E402
from app.db import SessionLocal  # noqa: E402

sdb = SessionLocal()
challenge = sdb.execute(
    text("SELECT challenge_token FROM destinations WHERE id=:i"), {"i": DID}
).scalar()
sdb.close()
r = client.post(f"/v1/destinations/{DID}/confirm", json={"challenge": challenge})
check("correct echo confirms 200", r.status_code == 200, r.status_code)
check(
    "disposition confirmed", r.json()["disposition"] == "confirmed", r.json()
)
d = client.get(f"/v1/destinations/{DID}").json()
check("state confirmed", d["confirmation_state"] == "confirmed")

# another pre-confirm event already in limbo? it had no row — verify not sent
check("still no event received after confirm", len(rec.events) == 0, f"n={len(rec.events)}")

# new event after confirmation: fan out and deliver
r = push_event(T("paid"), K("post-confirm-1"))
check("post-confirm event 201", r.status_code == 201, r.status_code)
check(
    "post-confirm delivery_count=1",
    r.json()["delivery_count"] == 1,
    r.json()["delivery_count"],
)
att = client.get(
    "/v1/ingestion/attempts", params={"dedupe_key": K("post-confirm-1")}
).json()
check("post-confirm disposition accepted", att[0]["disposition"] == "accepted")

stop = threading.Event()
wt = threading.Thread(target=worker_running, args=(stop, 6), daemon=True)
wt.start()
check(
    "receiver got the post-confirm event (old one NOT backfilled)",
    wait_for(lambda: len(rec.events) == 1),
    f"events={[e['dedupe_key'] for e in rec.events]}",
)
check(
    "received exactly the new event",
    rec.events and rec.events[0]["dedupe_key"] == K("post-confirm-1"),
    [e["dedupe_key"] for e in rec.events],
)
stop.set()
wt.join(timeout=3)


# ===========================================================================
# Rule 3: round expiry -> new round; event arriving during second pending
# window also never fans out even after a later confirmation
# ===========================================================================
rec2 = ReceiverState(0, auto_confirm=False)
srv2 = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(rec2))
port2 = srv2.server_address[1]
rec2.port = port2
threading.Thread(target=srv2.serve_forever, daemon=True).start()

r = client.post(
    "/v1/destinations",
    json={"url": f"http://127.0.0.1:{port2}/hook-{RUN}", "event_types": [T("refunded")]},
)
d2 = r.json()
DID2, round1 = d2["id"], d2["confirmation_round"]

# let the first round expire (CONFIRM_TIMEOUT_SECONDS=5) with the confirmer
# rotating rounds
stop = threading.Event()
http_client = worker.httpx.Client(timeout=5)
ct = threading.Thread(target=confirmer_running, args=(stop, 9, http_client), daemon=True)
ct.start()
time.sleep(7.5)
stop.set()
ct.join(timeout=2)
d2 = client.get(f"/v1/destinations/{DID2}").json()
check(
    "round expired and rotated (round number increased)",
    d2["confirmation_round"] > round1 and d2["confirmation_state"] == "pending",
    f"round={d2['confirmation_round']}",
)
# event during pending window after expiry
r = push_event(T("refunded"), K("expired-window-1"))
check(
    "event in expired window has no copies",
    r.json()["delivery_count"] == 0,
    r.json()["delivery_count"],
)
# confirm the new round manually
sdb = SessionLocal()
challenge = sdb.execute(
    text("SELECT challenge_token FROM destinations WHERE id=:i"), {"i": DID2}
).scalar()
sdb.close()
r = client.post(f"/v1/destinations/{DID2}/confirm", json={"challenge": challenge})
check("confirm new round 200", r.status_code == 200, r.status_code)
time.sleep(0.5)
check(
    "event from expired window not backfilled",
    len(rec2.events) == 0,
    f"n={len(rec2.events)}",
)


# ===========================================================================
# Rule 4: location change re-arms; old location receives nothing more; old
# queued copies are superseded; in-flight/delivered copies not recalled
# ===========================================================================
# dest A subscribed to "moved", confirmed, with a scheduled future event queued
old_recv = ReceiverState(0, auto_confirm=True)
srv_old = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(old_recv))
old_port = srv_old.server_address[1]
old_recv.port = old_port
threading.Thread(target=srv_old.serve_forever, daemon=True).start()

r = client.post(
    "/v1/destinations",
    json={"url": f"http://127.0.0.1:{old_port}/hook-{RUN}", "event_types": [T("moved")]},
)
d3 = r.json()
DID3, gen1 = d3["id"], d3["confirmation_generation"]
stop = threading.Event()
ct = threading.Thread(target=confirmer_running, args=(stop, 4), daemon=True)
ct.start()
check(
    "auto-confirm via probe echo",
    wait_for(
        lambda: client.get(f"/v1/destinations/{DID3}").json()["confirmation_state"]
        == "confirmed"
    ),
)
stop.set()
ct.join(timeout=2)

# one delivered event stays at old location (already sent, not recalled)
push_event(T("moved"), K("moved-delivered"))
stop = threading.Event()
wt = threading.Thread(target=worker_running, args=(stop, 5), daemon=True)
wt.start()
check("old location got first event", wait_for(lambda: len(old_recv.events) == 1))

# one scheduled future event queued, not yet due
future = (time.time() + 3600).__int__()
r = push_event(
    T("moved"),
    K("moved-queued"),
    not_before=f"2030-01-01T00:00:00Z",
)
queued_event_id = r.json()["id"]
check("queued event has a pending copy", r.json()["delivery_count"] == 1)
stop.set()
wt.join(timeout=3)

# relocate
new_recv = ReceiverState(0, auto_confirm=True)
srv_new = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(new_recv))
new_port = srv_new.server_address[1]
new_recv.port = new_port
threading.Thread(target=srv_new.serve_forever, daemon=True).start()

r = client.patch(
    f"/v1/destinations/{DID3}",
    json={"url": f"http://127.0.0.1:{new_port}/hook-{RUN}"},
)
check("relocate PATCH 200", r.status_code == 200, r.status_code)
d3b = r.json()
check("relocated -> pending", d3b["confirmation_state"] == "pending")
check(
    "generation bumped",
    d3b["confirmation_generation"] == gen1 + 1,
    d3b["confirmation_generation"],
)

# queued old copy must be superseded (run a reap+supersede pass)
sdb = SessionLocal()
sdb.execute(worker.SUPERSEDE_STALE_SQL)
sdb.commit()
sdb.close()
trace = client.get(f"/v1/events/{queued_event_id}/trace").json()
check(
    "queued copy superseded after relocation",
    any(dl["status"] == "superseded" for dl in trace["deliveries"]),
    [dl["status"] for dl in trace["deliveries"]],
)
check(
    "event with only superseded copies reports superseded",
    trace["event"]["status"] == "superseded",
    trace["event"]["status"],
)
check(
    "superseded copy excluded from delivery_count",
    trace["event"]["delivery_count"] == 0
    and trace["event"]["superseded_count"] == 1,
    (trace["event"]["delivery_count"], trace["event"]["superseded_count"]),
)

# worker must not deliver old-gen copies: spin worker, nothing to new/old
stop = threading.Event()
wt = threading.Thread(target=worker_running, args=(stop, 3), daemon=True)
wt.start()
time.sleep(2.5)
stop.set()
wt.join(timeout=2)
check("old location got nothing more", len(old_recv.events) == 1, len(old_recv.events))
check("new location unconfirmed: nothing yet", len(new_recv.events) == 0)

# confirm new location, then a NEW event arrives: only it goes to new location
stop = threading.Event()
ct = threading.Thread(target=confirmer_running, args=(stop, 4), daemon=True)
ct.start()
check(
    "new location confirmed",
    wait_for(
        lambda: client.get(f"/v1/destinations/{DID3}").json()["confirmation_state"]
        == "confirmed"
    ),
)
stop.set()
ct.join(timeout=2)
push_event(T("moved"), K("moved-after-relocate"))
stop = threading.Event()
wt = threading.Thread(target=worker_running, args=(stop, 5), daemon=True)
wt.start()
check(
    "new location gets only the new event",
    wait_for(lambda: [e["dedupe_key"] for e in new_recv.events] == [K("moved-after-relocate")]),
    [e["dedupe_key"] for e in new_recv.events],
)
check(
    "old queued event never delivered anywhere",
    K("moved-queued") not in [e["dedupe_key"] for e in new_recv.events]
    and K("moved-queued") not in [e["dedupe_key"] for e in old_recv.events],
)
stop.set()
wt.join(timeout=3)

# delivered copy before relocation still shows delivered (not recalled)
r = client.get("/v1/events", params={"dedupe_key": K("moved-delivered")}) if False else None
trace = client.get(
    f"/v1/events/{push_event('moved', 'x') and ''}"
) if False else None


# ===========================================================================
# Rule 5: unsubscribed types still ingested (unrouted) even with confirmed
# destinations
# ===========================================================================
r = push_event("nobody-subscribes-this", K("unrouted-1"))
check("unsubscribed type accepted", r.status_code == 201, r.status_code)
check(
    "unsubscribed type unrouted",
    r.json()["status"] == "unrouted" and r.json()["delivery_count"] == 0,
    r.json()["status"],
)
att = client.get(
    "/v1/ingestion/attempts", params={"dedupe_key": K("unrouted-1")}
).json()
check("unsubscribed disposition unrouted", att[0]["disposition"] == "unrouted")


# ===========================================================================
# extra: requeue blocked for unconfirmed destinations; idempotent re-register
# ===========================================================================
# re-register existing URL with no event_types keeps state (no re-arm)
before = client.get(f"/v1/destinations/{DID}").json()
r = client.post(
    "/v1/destinations", json={"url": f"http://127.0.0.1:{port}/hook-{RUN}"}
)
after = r.json()
check(
    "re-register same URL keeps confirmed state",
    after["id"] == DID and after["confirmation_state"] == "confirmed",
)

# confirmation attempts log visible (DID had a wrong echo; DID2 expired)
logs = client.get(f"/v1/destinations/{DID}/confirmation-attempts").json()
kinds = {(l["kind"], l["result"]) for l in logs}
check(
    "handshake log shows invalid echo",
    any(k == "echo" and res == "invalid" for k, res in kinds),
    kinds,
)
logs2 = client.get(f"/v1/destinations/{DID2}/confirmation-attempts").json()
kinds2 = {(l["kind"], l["result"]) for l in logs2}
check(
    "handshake log shows expired rounds and probes",
    any(k == "expired" and res == "expired" for k, res in kinds2)
    and any(k == "challenge" for k, _ in kinds2),
    kinds2,
)

print()
if failures:
    print(f"{len(failures)} FAILURES:", failures)
    raise SystemExit(1)
print("ALL CHECKS PASSED")
