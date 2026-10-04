"""Conditional put requests share a durable identity with the transaction."""

import json


def canonical_ops(ops):
    result = []
    for op in ops:
        item = {"key": str(op["key"]), "value": str(op["value"])}
        if "expected" in op:
            expected = op["expected"]
            if expected is not None and not isinstance(expected, str):
                raise ValueError("expected must be a string or null for absent")
            item["expected"] = expected
        result.append(item)
    if not result or len({o["key"] for o in result}) != len(result):
        raise ValueError("empty request or duplicate keys")
    return sorted(result, key=lambda o: o["key"])


def identity(ops):
    return json.dumps(canonical_ops(ops), sort_keys=True, separators=(",", ":"))


def failed_condition(conn, ops):
    for op in canonical_ops(ops):
        if "expected" not in op:
            continue
        row = conn.execute("SELECT value FROM kv WHERE key=?", (op["key"],)).fetchone()
        value = row[0] if row else None
        if value != op["expected"]:
            return op["key"]
    return None
