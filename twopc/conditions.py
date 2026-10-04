"""Conditional put requests share a durable identity with the transaction."""

import json
from typing import Any, Dict, List, Optional


def _string_or_null(value: Any, field: str) -> Optional[str]:
    if value is not None and not isinstance(value, str):
        raise ValueError(f"{field} must be a string or null")
    return value


def canonical_ops(ops: Any) -> List[Dict[str, Any]]:
    """Return the complete, stable representation of a write request.

    ``value`` may be a string or JSON null.  A missing key is represented by
    the database returning no row; a row containing SQL NULL is a real value
    and must never be confused with that missing-row result.
    """
    if not isinstance(ops, list):
        raise ValueError("ops must be a list")

    result = []
    for op in ops:
        if not isinstance(op, dict):
            raise ValueError("each op must be an object")
        if "key" not in op or "value" not in op:
            raise ValueError("each op must contain key and value")
        item = {
            "key": str(op["key"]),
            "value": _string_or_null(op["value"], "value"),
        }
        if "expected" in op:
            # null keeps the public legacy meaning "the key must be absent".
            # An actual null value is a row with value NULL (exists=true), and
            # therefore does not satisfy this condition.
            item["expected"] = _string_or_null(op["expected"], "expected")
        result.append(item)

    if not result or len({o["key"] for o in result}) != len(result):
        raise ValueError("empty request or duplicate keys")
    return sorted(result, key=lambda o: o["key"])


def coerce_stored_ops(ops: Any):
    """Normalize representations found in older durable logs.

    Current requests must still pass :func:`canonical_ops`; this only converts
    the previous ``[key, value]`` tuple representation when recovering an
    already-fsynchronous transaction.
    """
    if ops and isinstance(ops[0], (list, tuple)):
        ops = [{"key": key, "value": value} for key, value in ops]
    return canonical_ops(ops)


def identity(ops: Any) -> str:
    return json.dumps(canonical_ops(ops), sort_keys=True, separators=(",", ":"))


def stored_identity(ops: Any) -> str:
    return json.dumps(coerce_stored_ops(ops), sort_keys=True, separators=(",", ":"))


def condition_failure(conn, ops: Any):
    """Check conditions in the caller's current database transaction.

    Return ``None`` when all conditions pass; otherwise return details of the
    first failed condition.  Missing rows and NULL values are distinguished by
    the explicit ``exists`` field.
    """
    for op in canonical_ops(ops):
        if "expected" not in op:
            continue
        row = conn.execute(
            "SELECT value FROM kv WHERE key=?", (op["key"],)
        ).fetchone()
        exists = row is not None
        actual = row[0] if exists else None
        expected = op["expected"]

        if expected is None:
            passed = not exists
        else:
            passed = exists and actual == expected
        if not passed:
            return {
                "key": op["key"],
                "expected": expected,
                "actual": actual,
                "exists": exists,
            }
    return None


# Backwards-compatible internal helper name.
failed_condition = condition_failure
