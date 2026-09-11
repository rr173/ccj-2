"""Per-destination, per-event-type subscription filters ("订阅条件").

A destination normally receives a copy of every event whose type it
subscribes to. A subscription may additionally carry a *filter*: a purely
declarative condition over the event body (``payload``). At fan-out time the
condition is evaluated once against the exact body being routed:

* it matches      -> the destination gets its own delivery copy, exactly as
                     without a condition, and the condition is snapshotted
                     onto that copy;
* it does not     -> no delivery row is created at all (the event must never
                     be reported as sent to that address), and one
                     ``subscription_filter_evaluations`` row records
                     ``matched = false`` together with the exact condition
                     snapshot, so the trace can say "this address's own
                     condition did not match" instead of "nobody subscribed".

The evaluator is a **total, deterministic pure function** of (spec, body):
it performs no I/O, reads no clock, uses no randomness and never evaluates
arbitrary expressions. Re-evaluating the same body against the same snapshot
therefore always yields the same answer — on ingest, on a correction, or when
inspecting history.

Filter grammar (JSON)
---------------------

A leaf condition is an object with ``path`` / ``op`` / ``value``::

    {"path": "order.amount", "op": "gte", "value": 100}

Path is a dot-separated walk through objects; numeric segments descend into
arrays (``items.0.id``). A path that is absent fails every leaf op except
``exists``.

Leaf ops:

* ``eq`` / ``ne``           — deep JSON equality (``true`` is not equal to 1);
* ``gt`` / ``gte`` / ``lt`` / ``lte`` — ordering over two numbers (booleans
                              excluded) or two strings;
* ``in``                     — the body value equals one member of ``value``;
* ``exists``                 — the path is present (``value`` ignored);
* ``starts_with`` / ``ends_with`` / ``contains`` — string operations, both
                              sides must be strings.

Conditions combine with exactly one of:

* ``{"all": [<spec>, ...]}``  — every member must match;
* ``{"any": [<spec>, ...]}``  — at least one member must match;
* ``{"not": <spec>}``         — invert one spec.

A subscription without a stored condition (NULL) means "receive everything
of this type", exactly as before.
"""

from __future__ import annotations

from typing import Any

# Leaf operators.
OP_EQ = "eq"
OP_NE = "ne"
OP_GT = "gt"
OP_GTE = "gte"
OP_LT = "lt"
OP_LTE = "lte"
OP_IN = "in"
OP_EXISTS = "exists"
OP_STARTS_WITH = "starts_with"
OP_ENDS_WITH = "ends_with"
OP_CONTAINS = "contains"

ORDERING_OPS = frozenset({OP_GT, OP_GTE, OP_LT, OP_LTE})
STRING_OPS = frozenset({OP_STARTS_WITH, OP_ENDS_WITH, OP_CONTAINS})
LEAF_OPS = frozenset(
    {
        OP_EQ,
        OP_NE,
        OP_GT,
        OP_GTE,
        OP_LT,
        OP_LTE,
        OP_IN,
        OP_EXISTS,
        OP_STARTS_WITH,
        OP_ENDS_WITH,
        OP_CONTAINS,
    }
)

COMBINATOR_KEYS = ("all", "any", "not")
LEAF_KEYS = ("path", "op")

# Bounds keep a stored condition cheap to evaluate and impossible to nest
# unboundedly; they are validated once, when the condition is written.
MAX_NESTING_DEPTH = 8
MAX_NODES = 100
MAX_PATH_LENGTH = 512
MAX_IN_LIST = 100
MAX_STRING_OPERAND_LENGTH = 4096


class FilterSpecError(ValueError):
    """A filter condition is malformed at write time."""


_MISSING = object()


def _is_number(value: Any) -> bool:
    # bool is a subtype of int in Python; in JSON terms true/false are not
    # numbers, so they must not compare (or compare equal) numerically.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def json_equal(left: Any, right: Any) -> bool:
    """Deep equality with JSON typing (``true`` != ``1``)."""
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if _is_number(left) and _is_number(right):
        return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        if left.keys() != right.keys():
            return False
        return all(json_equal(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(
            json_equal(a, b) for a, b in zip(left, right)
        )
    return left == right


def _resolve_path(payload: Any, path: str) -> Any:
    current = payload
    for segment in path.split("."):
        if isinstance(current, dict):
            if segment not in current:
                return _MISSING
            current = current[segment]
        elif isinstance(current, list) and segment.isdigit():
            index = int(segment)
            if index >= len(current):
                return _MISSING
            current = current[index]
        else:
            return _MISSING
    return current


def _leaf_matches(node: dict[str, Any], payload: Any) -> bool:
    op = node["op"]
    actual = _resolve_path(payload, node["path"])
    if op == OP_EXISTS:
        return actual is not _MISSING
    if actual is _MISSING:
        # "not equal to something" on a missing path is still false: a
        # withheld copy means "the condition said nothing about this body",
        # not "some null comparison happened to hold".
        return False
    operand = node.get("value")
    if op == OP_EQ:
        return json_equal(actual, operand)
    if op == OP_NE:
        return not json_equal(actual, operand)
    if op in ORDERING_OPS:
        if not (
            ( _is_number(actual) and _is_number(operand))
            or (isinstance(actual, str) and isinstance(operand, str))
        ):
            return False
        if op == OP_GT:
            return actual > operand
        if op == OP_GTE:
            return actual >= operand
        if op == OP_LT:
            return actual < operand
        return actual <= operand
    if op == OP_IN:
        return any(json_equal(actual, member) for member in operand)
    if not isinstance(actual, str) or not isinstance(operand, str):
        return False
    if op == OP_STARTS_WITH:
        return actual.startswith(operand)
    if op == OP_ENDS_WITH:
        return actual.endswith(operand)
    return operand in actual


def evaluate(spec: dict[str, Any], payload: Any) -> bool:
    """Evaluate a validated filter spec against an event body.

    Pure and total: the result depends only on its two arguments, and an
    unknown shape evaluates to ``False`` (withdraw) rather than raising — a
    stored condition was validated when written, and a later code change must
    never silently route content that the address did not ask for.
    """
    if not isinstance(spec, dict):
        return False
    if "all" in spec:
        members = spec["all"]
        return isinstance(members, list) and all(
            isinstance(member, dict) and evaluate(member, payload) for member in members
        )
    if "any" in spec:
        members = spec["any"]
        return isinstance(members, list) and any(
            isinstance(member, dict) and evaluate(member, payload) for member in members
        )
    if "not" in spec:
        inner = spec["not"]
        return isinstance(inner, dict) and not evaluate(inner, payload)
    if spec.get("op") in LEAF_OPS and isinstance(spec.get("path"), str):
        return _leaf_matches(spec, payload)
    return False


def validate_filter_spec(spec: Any) -> dict[str, Any]:
    """Validate an inbound condition and return its canonical form.

    Raises :class:`FilterSpecError` with a human-readable reason on any
    malformed shape. The returned value is plain JSON-serializable data and
    is what gets stored (and snapshotted onto deliveries / evaluations).
    """
    node_count = 0

    def walk(node: Any, depth: int) -> dict[str, Any]:
        nonlocal node_count
        node_count += 1
        if node_count > MAX_NODES:
            raise FilterSpecError(
                f"filter is too complex: at most {MAX_NODES} conditions"
            )
        if depth > MAX_NESTING_DEPTH:
            raise FilterSpecError(
                f"filter is nested too deeply (max {MAX_NESTING_DEPTH})"
            )
        if not isinstance(node, dict):
            raise FilterSpecError("each filter condition must be a JSON object")

        combinators = [key for key in COMBINATOR_KEYS if key in node]
        is_leaf = "op" in node or "path" in node
        if is_leaf and combinators:
            raise FilterSpecError(
                "a filter condition is either a leaf (path/op) or a "
                "combinator (all/any/not), never both"
            )
        if not is_leaf and not combinators:
            raise FilterSpecError(
                "a filter condition needs path/op or one of all/any/not"
            )
        if not is_leaf and len(combinators) > 1:
            raise FilterSpecError(
                f"a combinator condition must use exactly one of {COMBINATOR_KEYS}"
            )

        if is_leaf:
            return _validate_leaf(node)

        key = combinators[0]
        if key == "not":
            inner = node[key]
            if not isinstance(inner, dict):
                raise FilterSpecError("'not' must wrap one filter condition")
            unexpected = set(node) - {"not"}
            if unexpected:
                raise FilterSpecError(f"'not' takes no extra keys: {sorted(unexpected)}")
            return {"not": walk(inner, depth + 1)}

        members = node[key]
        if not isinstance(members, list) or not members:
            raise FilterSpecError(f"'{key}' must wrap a non-empty list of conditions")
        unexpected = set(node) - {key}
        if unexpected:
            raise FilterSpecError(
                f"'{key}' takes no extra keys: {sorted(unexpected)}"
            )
        return {key: [walk(member, depth + 1) for member in members]}

    canonical = walk(spec, 1)
    return canonical


def _validate_leaf(node: dict[str, Any]) -> dict[str, Any]:
    unexpected = set(node) - {"path", "op", "value"}
    if unexpected:
        raise FilterSpecError(f"unknown leaf fields: {sorted(unexpected)}")
    path = node.get("path")
    op = node.get("op")
    if not isinstance(path, str) or not path.strip():
        raise FilterSpecError("filter leaf requires a non-empty 'path' string")
    if len(path) > MAX_PATH_LENGTH:
        raise FilterSpecError(
            f"filter path must be at most {MAX_PATH_LENGTH} characters"
        )
    segments = path.split(".")
    if any(segment == "" for segment in segments):
        raise FilterSpecError("filter path must not contain empty segments")
    if not isinstance(op, str) or op not in LEAF_OPS:
        raise FilterSpecError(
            f"filter op must be one of {sorted(LEAF_OPS)}"
        )

    if op == OP_EXISTS:
        # value is irrelevant for an existence check; store only path/op.
        return {"path": path, "op": op}

    if "value" not in node:
        raise FilterSpecError(f"filter op '{op}' requires a 'value'")
    value = node["value"]

    if op in (OP_EQ, OP_NE):
        _ensure_json_value(value)
    elif op in ORDERING_OPS:
        if not (
            (_is_number(value))
            or (isinstance(value, str))
        ) or isinstance(value, bool):
            raise FilterSpecError(
                f"filter op '{op}' requires a number or string value"
            )
    elif op == OP_IN:
        if not isinstance(value, list) or not value:
            raise FilterSpecError("filter op 'in' requires a non-empty list value")
        if len(value) > MAX_IN_LIST:
            raise FilterSpecError(
                f"'in' lists at most {MAX_IN_LIST} members"
            )
        for member in value:
            _ensure_json_value(member)
    elif op in STRING_OPS:
        if not isinstance(value, str):
            raise FilterSpecError(f"filter op '{op}' requires a string value")
        if len(value) > MAX_STRING_OPERAND_LENGTH:
            raise FilterSpecError(
                f"filter string operands are at most "
                f"{MAX_STRING_OPERAND_LENGTH} characters"
            )
    return {"path": path, "op": op, "value": value}


def _ensure_json_value(value: Any) -> None:
    if value is None or isinstance(value, (str, bool, int, float)):
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            raise FilterSpecError("filter values must be finite JSON values")
        return
    if isinstance(value, list):
        for member in value:
            _ensure_json_value(member)
        return
    if isinstance(value, dict):
        for key, member in value.items():
            if not isinstance(key, str):
                raise FilterSpecError("filter object keys must be strings")
            _ensure_json_value(member)
        return
    raise FilterSpecError("filter values must be JSON-compatible")
