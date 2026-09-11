"""End-to-end checks for the preview-consent gate ("预告 + 点头才给正文").

Covers the guarantees:

1. a gated event fans out as two distinct messages per subscriber — a preview
   first and a body after; the body can never arrive before the preview;
2. the body only goes out after this same address nods before the deadline;
3. "不要" closes the body for that address for good; a later nod never revives
   it, and the already-delivered preview is not taken back;
4. no nod before the agreed deadline voids the body (release_expired) —
   queryable as "this address did not nod", never written as a delivered body;
5. a nod arriving late is recorded late_ignored and cannot revive the voided
   body;
6. another address's nod can never release this address's body;
7. manually voiding a not-yet-out body leaves the preview (and anything else)
   untouched; a body already handed off cannot be voided;
8. a destination not subscribed to the type gets neither preview nor body,
   and its answer is an orphan;
9. the same address nodding the same preview twice counts exactly once.

Like the other test_* scripts this talks to the service's PostgreSQL directly
(DATABASE_URL, defaulting to the local test socket) and drives the worker /
reconciler loops in-process.
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
os.environ["CONFIRM_BACKOFF_BASE_SECONDS"] = "0"
os.environ["CONFIRM_POLL_INTERVAL_SECONDS"] = "0.1"

import httpx  # noqa: E402

from app.db import SessionLocal, build_engine  # noqa: E402
from app.models import init_db  # noqa: E402
from app import main as api  # noqa: E402
import app.worker as worker  # noqa: E402
from app import reconciler  # noqa: E402

failures: list[str] = []
RUN = str(int(time.time() * 1000))[-9:]
GTYPE = f"gated{RUN}"          # long consent timeout, exercised manually
GTYPE_SHORT = f"gatedshort{RUN}"  # 5s timeout for the expiry sweep
NTYPE = f"normal{RUN}"         # non-gated control type

SECRET_CONTENT = {"order_id": "1001", "amount": 42, "secret": "sauce"}


def K(name: str) -> str:
    return f"{RUN}-{name}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    """Records previews and bodies in arrival order; never auto-consents."""

    def __init__(self):
        self.previews: list[dict] = []
        self.bodies: list[dict] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hook-{RUN}"

    def preview_keys(self):
        return [p["key"] for p in self.previews]

    def body_keys(self):
        return [b["key"] for b in self.bodies]

    def _handler(self):
        state = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                if self.headers.get("X-Message-Type") == "activation_challenge":
                    self._write(200, {"echo": body["challenge"]})
                    return
                msg = {
                    "key": self.headers.get("Idempotency-Key"),
                    "event_id": body.get("event_id"),
                    "payload": body.get("payload"),
                    "preview_payload": body.get("preview_payload"),
                    "timeout": body.get("consent_timeout_seconds"),
                    "seq": body.get("destination_seq"),
                }
                if (
                    self.headers.get("X-Message-Type") == "event_preview"
                    or body.get("message_type") == "event_preview"
                ):
                    state.previews.append(msg)
                else:
                    state.bodies.append(msg)
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

r = client.post("/v1/sources", json={"name": f"preview-test-{RUN}"})
check("source register 201", r.status_code == 201, r.status_code)
SID, SECRET = r.json()["id"], r.json()["secret"]

# Gated types: a long-timeout one for manual decisions and a short one (5s)
# for expiry.
r = client.put(
    f"/v1/event-types/{GTYPE}/preview-policy",
    json={"consent_timeout_seconds": 3600},
)
check("gated policy created", r.status_code == 200 and r.json()["gated"] is True, r.text)
r = client.put(
    f"/v1/event-types/{GTYPE_SHORT}/preview-policy",
    json={"consent_timeout_seconds": 5},
)
check("short gated policy created", r.status_code == 200, r.text)
r = client.get(f"/v1/event-types/{NTYPE}/preview-policy")
check("normal type reports not gated", r.json()["gated"] is False, r.text)

recv_a, recv_b, recv_c, recv_d, recv_s = (
    MockReceiver(),
    MockReceiver(),
    MockReceiver(),
    MockReceiver(),
    MockReceiver(),
)


def register_confirmed(url, event_types, observe_only=False):
    r = client.post(
        "/v1/destinations",
        json={"url": url, "event_types": event_types, "observe_only": observe_only},
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
        time.sleep(0.05)
    db.close()
    hc.close()
    assert client.get(f"/v1/destinations/{did}").json()["confirmation_state"] == "confirmed"
    return did


DA = register_confirmed(recv_a.url(), [GTYPE, NTYPE])
DB = register_confirmed(recv_b.url(), [GTYPE])
DC = register_confirmed(recv_c.url(), [GTYPE_SHORT])
# D subscribes only to the normal type: never a gated preview/body.
DD = register_confirmed(recv_d.url(), [NTYPE])
DS = register_confirmed(recv_s.url(), [GTYPE], observe_only=True)


def push_event(key, event_type=GTYPE, payload=None, preview_payload=None):
    event = {
        "event_type": event_type,
        "dedupe_key": key,
        "payload": payload or SECRET_CONTENT,
    }
    if preview_payload is not None:
        event["preview_payload"] = preview_payload
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


def run_worker(times=30):
    for _ in range(times):
        worker.process_once()


def trace(event_id):
    return client.get(f"/v1/events/{event_id}/trace").json()


def gate(event_id, destination_id):
    rows = client.get(
        f"/v1/events/{event_id}/release-gates",
        params={"destination_id": destination_id},
    ).json()
    return rows[0] if rows else None


def body_delivery(event_id, destination_id):
    return next(
        d
        for d in trace(event_id)["deliveries"]
        if d["destination_id"] == destination_id and d["phase"] == "body"
    )


def preview_delivery(event_id, destination_id):
    return next(
        d
        for d in trace(event_id)["deliveries"]
        if d["destination_id"] == destination_id and d["phase"] == "preview"
    )


def consent(event_id, destination_id, decision):
    return client.post(
        f"/v1/events/{event_id}/consent",
        params={"destination_id": destination_id},
        json={"decision": decision},
    )


# ===========================================================================
# 1. Preview first, body only after THIS address nods; body never precedes it.
# ===========================================================================
r = push_event(K("e1"), preview_payload={"teaser": "new paid order"})
check("e1 accepted", r.status_code == 201, r.status_code)
e1 = r.json()["id"]
check(
    "e1 reports gated with two held for-real bodies (A,B) and one shadow (S)",
    r.json()["preview_gated"] is True
    and r.json()["bodies_waiting_count"] == 2
    and r.json()["shadow_bodies_waiting_count"] == 1
    and r.json()["status"] == "pending"
    and r.json()["delivered_count"] == 0,
    {k: r.json().get(k) for k in (
        "preview_gated", "bodies_waiting_count", "shadow_bodies_waiting_count",
        "status", "delivered_count")},
)

run_worker(20)
check("A and shadow S received the preview only (no body yet)",
      recv_a.preview_keys() == [f"prev:{K('e1')}"] and recv_a.body_keys() == []
      and recv_s.preview_keys() == [f"prev:{K('e1')}"] and recv_s.body_keys() == [],
      (recv_a.preview_keys(), recv_a.body_keys(), recv_s.preview_keys()))
check("B also got its own preview",
      recv_b.preview_keys() == [f"prev:{K('e1')}"], recv_b.preview_keys())

# Two distinct copies per subscriber, preview delivered, body held pending.
g_a = gate(e1, DA)
check("A's gate body is held after the preview landed",
      g_a["status"] == "pending" and g_a["release_state"] == "held"
      and g_a["preview_status"] == "delivered"
      and g_a["consent_deadline"] is not None,
      {k: g_a.get(k) for k in ("status", "release_state", "preview_status", "consent_deadline")})
p_a = preview_delivery(e1, DA)
check("preview carries no reconcile countdown",
      p_a["reconcile_state"] == "none" and p_a["reconcile_deadline"] is None,
      (p_a["reconcile_state"], p_a["reconcile_deadline"]))

# Preview message never contains the real content.
check("preview hides the real payload, carries the teaser",
      recv_a.previews[0]["payload"] == {}
      and recv_a.previews[0]["preview_payload"] == {"teaser": "new paid order"}
      and "secret" not in json.dumps(recv_a.previews[0]),
      recv_a.previews[0])

# preview_payload on a non-gated type is rejected rather than silently dropped.
r = push_event(K("bad"), event_type=NTYPE, preview_payload={"x": 1})
check("preview_payload on a non-gated type -> 422 invalid_body",
      r.status_code == 422 and r.json()["detail"]["disposition"] == "invalid_body",
      (r.status_code, r.json().get("detail")))

# A nods: only A's own body releases.
r = consent(e1, DA, "approve")
check("A approve -> released",
      r.status_code == 200 and r.json()["disposition"] == "released"
      and r.json()["release_state"] == "released",
      r.json())
run_worker(20)
check("A receives the body after nodding; B and S still have no body",
      recv_a.body_keys() == [K("e1")] and recv_b.body_keys() == []
      and recv_s.body_keys() == [],
      (recv_a.body_keys(), recv_b.body_keys(), recv_s.body_keys()))
check("the body arrived strictly after the preview at A",
      recv_a.preview_keys() and recv_a.bodies[0]["key"] == K("e1")
      and recv_a.bodies[0]["payload"] == SECRET_CONTENT,
      recv_a.bodies[0])

# ===========================================================================
# 2. Another address's nod cannot release this one (B's body stays held even
#    though A already nodded and S is a separate shadow).
# ===========================================================================
g_b = gate(e1, DB)
check("B's body is still held after A nodded",
      g_b["release_state"] == "held" and recv_b.body_keys() == [],
      g_b["release_state"])
r = consent(e1, DA, "approve")  # A nodding again — handled below, must not touch B
check("A repeat nod does not release B",
      gate(e1, DB)["release_state"] == "held",
      gate(e1, DB)["release_state"])

# ===========================================================================
# 3. "不要" closes B's body for good; later nod conflicts; preview stays out.
# ===========================================================================
r = consent(e1, DB, "deny")
check("B deny -> denied",
      r.json()["disposition"] == "denied"
      and r.json()["release_state"] == "release_denied",
      r.json())
run_worker(10)
check("B never receives the body after denying", recv_b.body_keys() == [], recv_b.body_keys())
g_b = gate(e1, DB)
check("B gate is terminal release_denied",
      g_b["status"] == "release_denied" and g_b["release_state"] == "release_denied"
      and g_b["voided_at"] is not None,
      {k: g_b.get(k) for k in ("status", "release_state", "voided_at")})
# The already-sent preview is not taken back.
check("B's delivered preview is untouched by the denial",
      recv_b.preview_keys() == [f"prev:{K('e1')}"]
      and preview_delivery(e1, DB)["status"] == "delivered",
      recv_b.preview_keys())
# A later nod cannot revive it.
r = consent(e1, DB, "approve")
check("late approve after deny -> conflict, body stays closed",
      r.json()["disposition"] == "conflict"
      and gate(e1, DB)["release_state"] == "release_denied",
      r.json())
run_worker(10)
check("still no body to B", recv_b.body_keys() == [], recv_b.body_keys())

# ===========================================================================
# 4. The same address nodding the same preview twice counts exactly once.
# ===========================================================================
r1 = consent(e1, DA, "approve").json()
r2 = consent(e1, DA, "approve").json()
check("second nod is a duplicate, not a second release",
      r1["disposition"] == "duplicate" and r2["disposition"] == "duplicate",
      (r1, r2))
decisions = client.get(
    "/v1/release-gate-decisions",
    params={"event_id": e1, "destination_id": DA},
).json()
effective = [d for d in decisions if d["disposition"] == "released"]
check("exactly one effective released decision for A",
      len(effective) == 1 and len(recv_a.bodies) == 1,
      ([d["disposition"] for d in decisions], len(recv_a.bodies)))

# ===========================================================================
# 5. No nod before the deadline voids the body; queryable as "did not nod";
#    the body is never written delivered; a late nod cannot revive it.
# ===========================================================================
r = push_event(K("e2"), event_type=GTYPE_SHORT)
check("e2 accepted", r.status_code == 201, r.status_code)
e2 = r.json()["id"]
run_worker(20)
check("C received the e2 preview, no body",
      f"prev:{K('e2')}" in recv_c.preview_keys() and recv_c.body_keys() == [],
      (recv_c.preview_keys(), recv_c.body_keys()))
g_c = gate(e2, DC)
check("C gate held with a ~5s deadline", g_c["release_state"] == "held", g_c)

print("... waiting for C's consent deadline to pass (5s) ...")
time.sleep(5.2)
timed, dead = reconciler.sweep_once()
check("reconciler sweep ran", isinstance(timed, int) and isinstance(dead, int), (timed, dead))
g_c = gate(e2, DC)
check("C's body expired (this address did not nod in time)",
      g_c["status"] == "release_expired" and g_c["release_state"] == "release_expired"
      and g_c["delivered_at"] is None,
      {k: g_c.get(k) for k in ("status", "release_state", "delivered_at")})
ev = trace(e2)["event"]
check("event never reports the expired body as delivered",
      ev["delivered_count"] == 0 and ev["bodies_expired_count"] == 1
      and ev["delivery_count"] == 0 and ev["status"] == "release_closed",
      {k: ev.get(k) for k in ("delivered_count", "bodies_expired_count",
                              "delivery_count", "status")})
check("expired gate is answerable as 'this address did not nod'",
      gate(e2, DC)["release_state"] == "release_expired"
      and client.get(
          "/v1/release-gates", params={"release_state": "release_expired"}
      ).json(),
      "no expired gates queryable")
# Find expired gates by filter: "who did not nod".
expired = client.get(
    "/v1/release-gates",
    params={"event_id": e2, "status": "release_expired"},
).json()
check("release-gates query finds the expired C body",
      [g["destination_id"] for g in expired] == [DC],
      [g["destination_id"] for g in expired])
# A nod arriving after the deadline cannot revive it.
r = consent(e2, DC, "approve")
check("late nod -> late_ignored, body stays expired",
      r.json()["disposition"] == "late_ignored"
      and gate(e2, DC)["release_state"] == "release_expired",
      r.json())
run_worker(20)
check("the expired body is never sent", recv_c.body_keys() == [], recv_c.body_keys())
late = client.get(
    "/v1/release-gate-decisions",
    params={"event_id": e2, "disposition": "late_ignored"},
).json()
check("the late nod is auditable", len(late) == 1 and late[0]["decision"] == "approve", late)

# ===========================================================================
# 6. Manually void a not-yet-out body: preview and others untouched.
# ===========================================================================
r = push_event(K("e3"))
check("e3 accepted", r.status_code == 201, r.status_code)
e3 = r.json()["id"]
run_worker(20)
check("A received e3 preview only",
      recv_a.preview_keys()[-1] == f"prev:{K('e3')}"
      and K("e3") not in recv_a.body_keys(),
      (recv_a.preview_keys(), recv_a.body_keys()))
body_a = body_delivery(e3, DA)
r = client.post(f"/v1/deliveries/{body_a['id']}/void-body")
check("manual void -> release_voided",
      r.status_code == 200 and r.json()["voided"] is True
      and r.json()["status"] == "release_voided",
      r.json())
check("preview row is untouched after voiding the body",
      preview_delivery(e3, DA)["status"] == "delivered",
      preview_delivery(e3, DA)["status"])
# Voiding again is idempotent, not a second action.
r = client.post(f"/v1/deliveries/{body_a['id']}/void-body")
check("repeated void is idempotent (voided=false)",
      r.status_code == 200 and r.json()["voided"] is False, r.json())
run_worker(10)
check("voided body never goes out", K("e3") not in recv_a.body_keys(), recv_a.body_keys())
# B's gate for the same event is independent and still held.
check("B's e3 gate is unaffected by voiding A's body",
      gate(e3, DB)["release_state"] == "held", gate(e3, DB))

# ===========================================================================
# 7. A body already handed off cannot be voided.
# ===========================================================================
body_e1_a = body_delivery(e1, DA)
r = client.post(f"/v1/deliveries/{body_e1_a['id']}/void-body")
check("voiding an already-delivered body -> 409",
      r.status_code == 409, (r.status_code, r.json().get("detail")))

# ===========================================================================
# 8. A destination not subscribed to the gated type gets neither message;
#    its answer is an orphan and releases nothing.
# ===========================================================================
check("D (not subscribed) got neither preview nor body for gated events",
      recv_d.preview_keys() == [] and recv_d.body_keys() == [],
      (recv_d.preview_keys(), recv_d.body_keys()))
r = consent(e1, DD, "approve")
check("D's answer to an event it does not gate -> orphan, 200",
      r.status_code == 200 and r.json()["disposition"] == "orphan",
      r.json())
run_worker(10)
check("orphan answer sends nothing to D", recv_d.body_keys() == [], recv_d.body_keys())
# But D still gets its normal (non-gated) type straight away.
r = push_event(K("n1"), event_type=NTYPE)
n1 = r.json()["id"]
run_worker(20)
check("non-gated type delivers a single body to D (no preview message)",
      recv_d.body_keys() == [K("n1")] and recv_d.preview_keys() == [],
      (recv_d.body_keys(), recv_d.preview_keys()))
n1_copies = trace(n1)["deliveries"]
check("non-gated copies are ordinary phase=body rows",
      all(c["phase"] == "body" and c["release_state"] is None for c in n1_copies),
      [(c["destination_id"], c["phase"], c["release_state"]) for c in n1_copies])

# ===========================================================================
# 9. Decision for an unknown event is a 404; endpoint requires destination.
# ===========================================================================
r = client.post(
    f"/v1/events/00000000-0000-0000-0000-000000000000/consent",
    params={"destination_id": DA},
    json={"decision": "approve"},
)
check("consent on unknown event -> 404", r.status_code == 404, r.status_code)
r = client.post(f"/v1/events/{e1}/consent", json={"decision": "approve"})
check("consent without destination_id -> 422", r.status_code == 422, r.status_code)

# ===========================================================================
# 10. Answering before the preview reached the address releases nothing.
# ===========================================================================
# Use the shadow S on a fresh gated event and do NOT run the worker, so its
# preview is still queued.
r = push_event(K("e4"))
e4 = r.json()["id"]
r = consent(e4, DS, "approve")
check("nod before preview delivery -> preview_not_delivered, body stays held",
      r.json()["disposition"] == "preview_not_delivered"
      and gate(e4, DS)["release_state"] == "held",
      r.json())
run_worker(20)
# Preview now delivered; the earlier premature nod must not count, body held.
check("after the preview lands the body is still held (premature nod not applied)",
      gate(e4, DS)["release_state"] == "held"
      and recv_s.body_keys() == [],
      gate(e4, DS)["release_state"])
r = consent(e4, DS, "approve")
check("a proper in-time nod now releases the shadow body",
      r.json()["disposition"] == "released", r.json())
run_worker(20)
check("shadow S receives its e4 body after nodding",
      recv_s.body_keys() == [K("e4")], recv_s.body_keys())

# ===========================================================================
# 11. Relocating an address abandons its queued gated pair: the held body is
#     voided (the notice never reached the address), and a late nod cannot
#     revive it.
# ===========================================================================
recv_r = MockReceiver()
DR = register_confirmed(recv_r.url(), [GTYPE])
r = push_event(K("e5"))
e5 = r.json()["id"]
# Do NOT run the worker: preview and body are still queued at the old URL.
check("R gate held before anything is sent",
      gate(e5, DR)["release_state"] == "held", gate(e5, DR))
recv_r2 = MockReceiver()
r = client.patch(f"/v1/destinations/{DR}", json={"url": recv_r2.url()})
check("relocation re-arms the handshake", r.status_code == 200, r.status_code)
g = gate(e5, DR)
check("held body is voided when its preview was superseded by relocation",
      g["status"] == "release_voided" and g["void_reason"] == "preview_superseded",
      {k: g.get(k) for k in ("status", "void_reason")})
r = consent(e5, DR, "approve")
check("a nod after relocation cannot revive the voided body",
      r.json()["disposition"] == "late_ignored"
      and gate(e5, DR)["release_state"] == "release_voided",
      r.json())

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL PREVIEW-CONSENT CHECKS PASSED")
