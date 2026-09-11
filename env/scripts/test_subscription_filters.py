"""End-to-end checks for per-destination, per-event-type subscription filters
("订阅条件": 只有正文对得上这个地址自己提的条件才给它).

The semantics under test:

1. a condition-holding subscriber receives a body only when its OWN condition
   holds; a non-matching body creates no delivery at all (it is never written
   as sent) and stays queryable as "this address's own condition did not
   match";
2. a subscription without a condition receives every event of the type;
3. replacing a condition only affects the next event: already-fanned copies
   keep their snapshot and are not recalled;
4. conditions are per address — one address's condition never gates another;
5. a condition on a type the address does not subscribe to is refused;
6. an event withheld by every confirmed subscriber is disposition 'filtered',
   never collapsed into 'unrouted' (nobody subscribed);
7. the trace distinguishes the two, per address;
8. the judgement is deterministic: the same body against the same condition
   snapshot always yields the same answer (pure evaluator), including on
   gated types (no preview at all when withheld) and on corrections
   (re-evaluated against the corrected body under the CURRENT condition).
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
os.environ["FAILURE_THRESHOLD"] = "5"
os.environ["QUARANTINE_SECONDS"] = "0"
os.environ["RETRY_BACKOFF_BASE_SECONDS"] = "0"
os.environ["RECEIPT_TIMEOUT_SECONDS"] = "30"
os.environ["CONFIRM_BACKOFF_BASE_SECONDS"] = "0"
os.environ["CONFIRM_POLL_INTERVAL_SECONDS"] = "0.1"

from app.db import SessionLocal, build_engine  # noqa: E402
from app.models import init_db  # noqa: E402
from app import main as api  # noqa: E402
import app.worker as worker  # noqa: E402
from app.subfilters import evaluate  # noqa: E402

failures: list[str] = []
RUN = str(int(time.time() * 1000))[-9:]
ETYPE = f"ftype{RUN}"
GTYPE = f"gtype{RUN}"
OTYPE = f"otype{RUN}"
LTYPE = f"ltype{RUN}"
CTYPE = f"ctype{RUN}"
UTYPE = f"unc{RUN}"


def K(name: str) -> str:
    return f"{RUN}-{name}"


def check(name: str, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    if not cond:
        failures.append(name)


class MockReceiver:
    """Records every delivered body (and preview) in arrival order."""

    def __init__(self):
        self.events: list[dict] = []
        self.previews: list[dict] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self, tag: str) -> str:
        return f"http://127.0.0.1:{self.port}/hook-{RUN}-{tag}"

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
                if self.headers.get("X-Message-Type") == "event_preview":
                    state.previews.append(body)
                else:
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

r = client.post("/v1/sources", json={"name": f"filter-test-{RUN}"})
check("source register 201", r.status_code == 201, r.status_code)
SID, SECRET = r.json()["id"], r.json()["secret"]


def push_event(key, event_type=ETYPE, payload=None, preview_payload=None):
    event = {
        "event_type": event_type,
        "dedupe_key": key,
        "payload": payload if payload is not None else {},
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


def register(recv, tag, *, event_types, filters=None):
    body = {"url": recv.url(tag), "event_types": event_types}
    if filters is not None:
        body["filters"] = filters
    r = client.post("/v1/destinations", json=body)
    assert r.status_code == 201, r.text
    did = r.json()["id"]
    hc = __import__("httpx").Client(timeout=5)
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
    state = client.get(f"/v1/destinations/{did}").json()
    assert state["confirmation_state"] == "confirmed", state
    return did


def run_worker(times=40):
    for _ in range(times):
        worker.process_once()


def keys(received):
    return [e["dedupe_key"] for e in received]


def eval_for(tr, destination_id):
    return next(
        (e for e in tr["filter_evaluations"] if e["destination_id"] == destination_id),
        None,
    )


# --- receivers --------------------------------------------------------------

recv_match = MockReceiver()   # condition: amount >= 100
recv_other = MockReceiver()   # condition: region == 'east'
recv_plain = MockReceiver()   # no condition at all
recv_shadow = MockReceiver()  # shadow with a condition

COND_HIGH = {"path": "amount", "op": "gte", "value": 100}
COND_EAST = {"path": "region", "op": "eq", "value": "east"}

DHIGH = register(
    recv_match, "high", event_types=[ETYPE], filters={ETYPE: COND_HIGH}
)
DEAST = register(
    recv_other, "east", event_types=[ETYPE], filters={ETYPE: COND_EAST}
)
DPLAIN = register(recv_plain, "plain", event_types=[ETYPE])
DSHADOW = register(
    recv_shadow,
    "shadow",
    event_types=[ETYPE],
    filters={ETYPE: COND_HIGH},
)
client.patch(f"/v1/destinations/{DSHADOW}", json={"observe_only": True})

# Destination details echo the stored conditions (and nothing for the plain
# subscription).
details = client.get(f"/v1/destinations/{DHIGH}").json()
check(
    "destination detail exposes its condition",
    details["filters"] == {ETYPE: COND_HIGH},
    details["filters"],
)
check(
    "unconditional subscription has empty filters map",
    client.get(f"/v1/destinations/{DPLAIN}").json()["filters"] == {},
)

# ===========================================================================
# 1+4. Same event, per-address conditions decided independently.
# ===========================================================================
r = push_event(K("e-high-east"), payload={"amount": 250, "region": "east"})
ev = r.json()
check("matching body accepted 201", r.status_code == 201, r.status_code)
e_he = r.json()["id"]
check(
    "both real conditions held -> all three real copies, 1 shadow",
    ev["delivery_count"] == 3 and ev["shadow_delivery_count"] == 1
    and ev["filtered_out_count"] == 0 and ev["shadow_filtered_out_count"] == 0,
    {k: ev[k] for k in ("delivery_count", "shadow_delivery_count",
                        "filtered_out_count", "shadow_filtered_out_count")},
)

r = push_event(K("e-low-east"), payload={"amount": 10, "region": "east"})
ev = r.json()
e_le = r.json()["id"]
check(
    "low amount withheld by high-condition addresses, east+plain get it",
    ev["delivery_count"] == 2 and ev["filtered_out_count"] == 1
    and ev["shadow_delivery_count"] == 0
    and ev["shadow_filtered_out_count"] == 1
    and ev["status"] == "pending",
    {k: ev[k] for k in ("delivery_count", "filtered_out_count",
                        "shadow_delivery_count", "shadow_filtered_out_count",
                        "status")},
)

r = push_event(K("e-high-west"), payload={"amount": 500, "region": "west"})
ev = r.json()
e_hw = r.json()["id"]
check(
    "high/west withheld only by the east address",
    ev["delivery_count"] == 2 and ev["filtered_out_count"] == 1
    and ev["shadow_delivery_count"] == 1
    and ev["shadow_filtered_out_count"] == 0,
    {k: ev[k] for k in ("delivery_count", "filtered_out_count",
                        "shadow_delivery_count", "shadow_filtered_out_count")},
)

r = push_event(K("e-low-west"), payload={"amount": 1, "region": "west"})
ev = r.json()
e_lw = r.json()["id"]
check(
    "every condition withholds, plain address still receives -> accepted",
    ev["delivery_count"] == 1 and ev["filtered_out_count"] == 2
    and ev["status"] == "pending",
    {k: ev[k] for k in ("delivery_count", "filtered_out_count", "status")},
)

run_worker(60)
check("high receiver got exactly the two high bodies",
      keys(recv_match.events) == [K("e-high-east"), K("e-high-west")],
      keys(recv_match.events))
check("east receiver got exactly the two east ETYPE bodies",
      keys(recv_other.events) == [K("e-high-east"), K("e-low-east")],
      keys(recv_other.events))
check("plain receiver got all four",
      keys(recv_plain.events)
      == [K("e-high-east"), K("e-low-east"), K("e-high-west"), K("e-low-west")],
      keys(recv_plain.events))
check("shadow receiver got only the high bodies",
      keys(recv_shadow.events) == [K("e-high-east"), K("e-high-west")],
      keys(recv_shadow.events))

# ===========================================================================
# 6+7. All confirmed subscribers withhold: 'filtered', distinct from unrouted.
# ===========================================================================
fresh = MockReceiver()
ONLY = register(
    fresh,
    "only",
    event_types=[OTYPE],
    filters={OTYPE: {"path": "v", "op": "eq", "value": 1}},
)
# An event of this type while the one and only subscriber does not match.
r = push_event(K("o-nomatch"), event_type=OTYPE, payload={"v": 2})
ev = r.json()
o_nomatch = r.json()["id"]
check(
    "all-withheld event status is 'filtered', not unrouted",
    r.status_code == 201 and ev["status"] == "filtered"
    and ev["delivery_count"] == 0 and ev["filtered_out_count"] == 1,
    {k: ev.get(k) for k in ("status", "delivery_count", "filtered_out_count")},
)
attempts = client.get(
    "/v1/ingestion/attempts", params={"dedupe_key": K("o-nomatch")}
).json()
check(
    "admission record disposition is 'filtered'",
    [a["disposition"] for a in attempts] == ["filtered"],
    [a["disposition"] for a in attempts],
)
tr = client.get(f"/v1/events/{o_nomatch}/trace").json()
evrow = eval_for(tr, ONLY)
check(
    "trace names the withheld address with its condition snapshot and false",
    evrow is not None and evrow["matched"] is False
    and evrow["filter_spec"] == {"path": "v", "op": "eq", "value": 1}
    and len(tr["deliveries"]) == 0,
    evrow,
)
check(
    "withheld body was never delivered: receiver got nothing",
    fresh.events == [],
)

# A type with NO subscribers is still explicitly unrouted.
r = push_event(K("u-none"), event_type=f"nobody{RUN}", payload={})
ev = r.json()
check(
    "no subscribers at all stays unrouted",
    ev["status"] == "unrouted" and ev["delivery_count"] == 0
    and ev["filtered_out_count"] == 0,
    {k: ev.get(k) for k in ("status", "filtered_out_count")},
)
attempts = client.get(
    "/v1/ingestion/attempts", params={"dedupe_key": K("u-none")}
).json()
check("unrouted disposition kept distinct",
      [a["disposition"] for a in attempts] == ["unrouted"])

# A matching body later is delivered normally (condition still the same).
r = push_event(K("o-match"), event_type=OTYPE, payload={"v": 1})
check("matching body 201", r.status_code == 201, r.status_code)
run_worker(20)
check("conditional receiver gets the matching body",
      keys(fresh.events) == [K("o-match")])

# ===========================================================================
# 7b. Matched evaluations are recorded too, on deliveries the snapshot sticks.
# ===========================================================================
tr = client.get(f"/v1/events/{e_hw}/trace").json()
# high+shadow got copies with snapshot; east withheld.
for did, matched in ((DHIGH, True), (DSHADOW, True), (DEAST, False)):
    row = eval_for(tr, did)
    check(
        f"evaluation row for {did} matched={matched}",
        row is not None and row["matched"] is matched,
        row,
    )
copy = next(d for d in tr["deliveries"] if d["destination_id"] == DHIGH)
check(
    "the fanned copy carries the condition snapshot it was born with",
    copy["filter_spec"] == COND_HIGH,
    copy["filter_spec"],
)

# ===========================================================================
# 3. Replacing a condition affects only the NEXT event; queued copies stay.
# ===========================================================================
r = client.patch(
    f"/v1/destinations/{DHIGH}",
    json={"filters": {ETYPE: {"path": "amount", "op": "gte", "value": 9999}}},
)
check("condition replace 200", r.status_code == 200, r.text)
check(
    "new condition is stored",
    client.get(f"/v1/destinations/{DHIGH}").json()["filters"]
    == {ETYPE: {"path": "amount", "op": "gte", "value": 9999}},
)
# The already-delivered older copy is untouched and still carries its old
# snapshot (nothing is recalled).
tr_old = client.get(f"/v1/events/{e_he}/trace").json()
old_copy = next(
    d for d in tr_old["deliveries"] if d["destination_id"] == DHIGH
)
check(
    "already-fanned copy keeps its old condition snapshot",
    old_copy["filter_spec"] == COND_HIGH and old_copy["status"] == "delivered",
    (old_copy["filter_spec"], old_copy["status"]),
)
# Only the next event is judged under the new condition.
r = push_event(K("e-after-change-500"), payload={"amount": 500, "region": "east"})
ev = r.json()
check(
    "next event withheld under the new stricter condition",
    ev["filtered_out_count"] >= 1 and ev["delivery_count"] == 2,
    {k: ev[k] for k in ("delivery_count", "filtered_out_count")},
)
# Clearing the condition: null per key on PATCH.
r = client.patch(f"/v1/destinations/{DHIGH}", json={"filters": {ETYPE: None}})
check("condition clear 200", r.status_code == 200, r.text)
check(
    "cleared condition is gone",
    client.get(f"/v1/destinations/{DHIGH}").json()["filters"] == {},
)
r = push_event(K("e-after-clear-5"), payload={"amount": 5, "region": "west"})
ev = r.json()
check(
    "after clearing, the address receives the low body again",
    ev["delivery_count"] == 2 and ev["filtered_out_count"] == 1,
    {k: ev[k] for k in ("delivery_count", "filtered_out_count")},
)
run_worker(40)
check(
    "high receiver order after change: old two, then the post-clear body only",
    keys(recv_match.events)
    == [K("e-high-east"), K("e-high-west"), K("e-after-clear-5")],
    keys(recv_match.events),
)

# ===========================================================================
# 5. A condition on a type the address does not subscribe to is refused.
# ===========================================================================
r = client.post(
    "/v1/destinations",
    json={
        "url": MockReceiver().url("bad"),
        "event_types": [ETYPE],
        "filters": {OTYPE: {"path": "v", "op": "eq", "value": 1}},
    },
)
check("register: filter key outside event_types is 422",
      r.status_code == 422, r.status_code)
r = client.post(
    "/v1/destinations",
    json={"url": MockReceiver().url("bad2"), "filters": {ETYPE: COND_HIGH}},
)
check("register: filters without event_types is 422",
      r.status_code == 422, r.status_code)
r = client.patch(
    f"/v1/destinations/{DPLAIN}",
    json={"filters": {OTYPE: {"path": "v", "op": "eq", "value": 1}}},
)
check("patch: filter on an unsubscribed type is 422",
      r.status_code == 422, r.status_code)
r = client.patch(
    f"/v1/destinations/{DPLAIN}",
    json={"filters": {ETYPE: {"op": "eq", "value": 1}}},  # missing path
)
check("malformed condition is 422", r.status_code == 422, r.status_code)
r = client.patch(f"/v1/destinations/{DPLAIN}", json={})
check("empty patch stays 422", r.status_code == 422, r.status_code)

# A dedicated subscriber on a fresh type LTYPE: it keeps the simple
# amount>=100 condition and never changes, so the only routing move below is
# DEAST leaving the ETYPE subscription.
leaver_recv = MockReceiver()
DELEAVE = register(
    leaver_recv, "leaver", event_types=[LTYPE], filters={LTYPE: COND_HIGH}
)
# Subscription replacement that drops a type drops its condition too; a
# filter naming only the surviving set is accepted.
r = client.patch(
    f"/v1/destinations/{DEAST}",
    json={
        "event_types": [LTYPE],
        "filters": {LTYPE: {"path": "v", "op": "eq", "value": 1}},
    },
)
check("replace subscription + condition 200", r.status_code == 200, r.text)
r = push_event(K("east-unsub-now"), payload={"amount": 1, "region": "east"})
ev = r.json()
check(
    "address that left the ETYPE subscription no longer judges its events",
    # Remaining real ETYPE subscribers: DHIGH (unconditional now) and DPLAIN;
    # DEAST is off the type entirely (no copy and no evaluation), DSHADOW is
    # the only condition-carrying subscriber and withholds.
    ev["delivery_count"] == 2 and ev["filtered_out_count"] == 0
    and ev["shadow_filtered_out_count"] == 1,
    {k: ev[k] for k in ("delivery_count", "filtered_out_count",
                        "shadow_filtered_out_count")},
)
tr = client.get(f"/v1/events/{r.json()['id']}/trace").json()
check(
    "the unsubscribed address has no evaluation row for the event",
    eval_for(tr, DEAST) is None,
)
# The replacement address judges LTYPE independently, with its new condition.
r = push_event(K("leave-type-no"), event_type=LTYPE, payload={"v": 2})
ev = r.json()
check(
    "moved address judges its new type with the new condition",
    ev["delivery_count"] == 0 and ev["filtered_out_count"] == 2,
    {k: ev[k] for k in ("delivery_count", "filtered_out_count")},
)

# ===========================================================================
# 8. Determinism: same body + same condition snapshot always judges the same.
# ===========================================================================
spec = {
    "all": [
        {"path": "amount", "op": "gte", "value": 100},
        {"any": [
            {"path": "region", "op": "eq", "value": "east"},
            {"path": "memo", "op": "contains", "value": "urgent"},
        ]},
        {"not": {"path": "draft", "op": "eq", "value": True}},
    ]
}
body_ok = {"amount": 100, "region": "west", "memo": "very urgent", "draft": False}
body_no = {"amount": 99, "region": "east", "memo": "routine", "draft": False}
answers = {(evaluate(spec, body_ok), evaluate(spec, body_no))
           for _ in range(50)}
check("pure evaluator is deterministic across repeats",
      answers == {(True, False)}, answers)
# JSON typing: bool is not a number, missing path never matches eq/ne.
check("string vs number ordering never coerces",
      evaluate({"path": "a", "op": "gte", "value": 1}, {"a": "100"}) is False)
check("true != 1",
      evaluate({"path": "a", "op": "eq", "value": 1}, {"a": True}) is False)
check("missing path fails ne as well",
      evaluate({"path": "a", "op": "ne", "value": 1}, {}) is False)

# Same condition on two different subscribers judges the same body the same
# way; one address's answer never gates the other (already covered above too).
a = MockReceiver()
b = MockReceiver()
DA = register(a, "da", event_types=[OTYPE],
              filters={OTYPE: {"path": "v", "op": "eq", "value": 1}})
DB = register(b, "db", event_types=[OTYPE],
              filters={OTYPE: {"path": "v", "op": "eq", "value": 1}})
r = push_event(K("det-1"), event_type=OTYPE, payload={"v": 2})
tr = client.get(f"/v1/events/{r.json()['id']}/trace").json()
check(
    "both subscribers independently withhold the same body",
    eval_for(tr, DA)["matched"] is False
    and eval_for(tr, DB)["matched"] is False,
)

# Global audit endpoint.
rows = client.get(
    "/v1/filter-evaluations",
    params={"event_id": o_nomatch, "matched": False},
).json()
check("global evaluations endpoint lists the withheld judgement",
      len(rows) == 1 and rows[0]["destination_id"] == ONLY
      and rows[0]["observe_only"] is False, rows)

# ===========================================================================
# Gated type: a withheld subscriber gets neither preview nor body.
# ===========================================================================
client.put(
    f"/v1/event-types/{GTYPE}/preview-policy",
    json={"consent_timeout_seconds": 3600},
)
g_recv = MockReceiver()
g_other = MockReceiver()
DG = register(
    g_recv,
    "gated-cond",
    event_types=[GTYPE],
    filters={GTYPE: {"path": "allow", "op": "eq", "value": True}},
)
DG2 = register(g_other, "gated-plain", event_types=[GTYPE])

r = push_event(
    K("g-withheld"),
    event_type=GTYPE,
    payload={"allow": False},
    preview_payload={"teaser": "nope"},
)
ev = r.json()
g_withheld_id = r.json()["id"]
check(
    "gated withheld: no pair at all, counted as filtered",
    ev["delivery_count"] == 1 and ev["filtered_out_count"] == 1,
    {k: ev[k] for k in ("delivery_count", "filtered_out_count")},
)
run_worker(30)
check("withheld gated address received no preview and no body",
      g_recv.previews == [] and g_recv.events == [])
check("the other gated address got its preview",
      [p["dedupe_key"] for p in g_other.previews] == [f"prev:{K('g-withheld')}"])

r = push_event(
    K("g-allowed"),
    event_type=GTYPE,
    payload={"allow": True},
    preview_payload={"teaser": "yes"},
)
run_worker(30)
gid = r.json()["id"]
# Approve for the conditional address and let the body go.
r2 = client.post(
    f"/v1/events/{gid}/consent", params={"destination_id": DG},
    json={"decision": "approve"},
)
check("conditional address approves its gated body",
      r2.json()["disposition"] == "released", r2.text)
run_worker(30)
check("conditional gated address got preview then body",
      [p["dedupe_key"] for p in g_recv.previews] == [f"prev:{K('g-allowed')}"]
      and keys(g_recv.events) == [K("g-allowed")],
      ([p["dedupe_key"] for p in g_recv.previews], keys(g_recv.events)))

# Consent from the withheld address on the event it was filtered from is an
# orphan (no gate row exists for it at all).
r2 = client.post(
    f"/v1/events/{g_withheld_id}/consent",
    params={"destination_id": DG},
    json={"decision": "approve"},
)
check("withheld gated address consent is orphan",
      r2.status_code == 200 and r2.json().get("disposition") == "orphan",
      (r2.status_code, r2.text[:160]))

# ===========================================================================
# Corrections: the corrected body is re-evaluated against the CURRENT
# condition of every address the original really reached.
# ===========================================================================
# OTYPE corrections need fresh destinations: DA/DB below were created earlier
# for other checks and may have been retoggled.
corr_a_recv = MockReceiver()
corr_b_recv = MockReceiver()
DCA = register(
    corr_a_recv, "corr-a", event_types=[CTYPE],
    filters={CTYPE: {"path": "v", "op": "eq", "value": 1}},
)
DCB = register(
    corr_b_recv, "corr-b", event_types=[CTYPE],
    filters={CTYPE: {"path": "v", "op": "eq", "value": 1}},
)
r = push_event(K("orig-1"), event_type=CTYPE, payload={"v": 1, "n": 1})
orig = r.json()["id"]
run_worker(30)
check("original delivered to both conditional addresses",
      keys(corr_a_recv.events) == [K("orig-1")]
      and keys(corr_b_recv.events) == [K("orig-1")])

client.patch(
    f"/v1/destinations/{DCA}",
    json={"filters": {CTYPE: {"path": "v", "op": "eq", "value": 3}}},
)
r = client.post(
    f"/v1/events/{orig}/corrections",
    json={"dedupe_key": K("corr-1"), "payload": {"v": 1, "n": 2}},
)
check("correction 201", r.status_code == 201, r.text)
cev = r.json()
check(
    "correction judged against current conditions: 1 copy, 1 withheld",
    cev["delivery_count"] == 1 and cev["filtered_out_count"] == 1,
    {k: cev[k] for k in ("delivery_count", "filtered_out_count")},
)
run_worker(30)
check(
    "only DCB (v==1 still matches) got the correction; DCA did not",
    keys(corr_a_recv.events) == [K("orig-1")]
    and keys(corr_b_recv.events) == [K("orig-1"), K("corr-1")],
    (keys(corr_a_recv.events), keys(corr_b_recv.events)),
)
ctr = client.get(f"/v1/events/{cev['id']}/trace").json()
check(
    "correction trace names DCA as withheld under the new snapshot",
    eval_for(ctr, DCA)["matched"] is False
    and eval_for(ctr, DCA)["filter_spec"]
    == {"path": "v", "op": "eq", "value": 3},
)

# A correction whose corrected body is withheld by everyone is refused and
# creates nothing (DCA needs v==3, DCB currently needs v==9).
client.patch(
    f"/v1/destinations/{DCB}",
    json={"filters": {CTYPE: {"path": "v", "op": "eq", "value": 9}}},
)
r = client.post(
    f"/v1/events/{orig}/corrections",
    json={"dedupe_key": K("corr-2"), "payload": {"v": 2}},
)
check("all-withheld correction is 409 and creates nothing",
      r.status_code == 409, r.status_code)
# The refused key is not consumed: once DCB asks for v==2 it can be reused.
client.patch(
    f"/v1/destinations/{DCB}",
    json={"filters": {CTYPE: {"path": "v", "op": "eq", "value": 2}}},
)
r = client.post(
    f"/v1/events/{orig}/corrections",
    json={"dedupe_key": K("corr-2"), "payload": {"v": 2}},
)
check("same key usable once the body matches",
      r.status_code == 201, (r.status_code, r.text[:120]))

# Still-unconfirmed subscriber: event accepted but its condition is not even
# evaluated (pending_confirmation semantics preserved). Use a fresh type and
# don't run the confirmer, so the destination stays pending.
unconf = MockReceiver()
UTYPE = f"unc{RUN}"
r = client.post(
    "/v1/destinations",
    json={
        "url": unconf.url("unconfirmed"),
        "event_types": [UTYPE],
        "filters": {UTYPE: {"path": "v", "op": "eq", "value": 1}},
    },
)
r = push_event(K("unc-1"), event_type=UTYPE, payload={"v": 2})
ev = r.json()
check(
    "unconfirmed subscriber is pending_confirmation, not filtered",
    ev["status"] == "unrouted",
    ev["status"],
)
attempts = client.get(
    "/v1/ingestion/attempts", params={"dedupe_key": K("unc-1")}
).json()
check(
    "admission says pending_confirmation (condition never evaluated)",
    [a["disposition"] for a in attempts] == ["pending_confirmation"],
    [a["disposition"] for a in attempts],
)

# Re-registering the same URL keeps its condition when event_types omitted.
r = client.post(
    "/v1/destinations", json={"url": recv_plain.url("plain")}
)
check(
    "re-register without event_types keeps the (empty) conditions",
    r.status_code == 201 and r.json()["filters"] == {},
    (r.status_code, r.json().get("filters")),
)

print()
if failures:
    print(f"{len(failures)} FILTER CHECK(S) FAILED: {failures}")
    raise SystemExit(1)
print("ALL SUBSCRIPTION-FILTER CHECKS PASSED")
