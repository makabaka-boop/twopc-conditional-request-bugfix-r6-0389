"""条件写请求的规范化、身份与条件评估。

一个完整请求（所有 op 的 key/value/expected 组合）有一个确定性的
:func:`identity`，随事务一起持久化。同一事务编号在协调者与每个参与者处
都必须对应同一个身份——重试或重启后携带不同值/条件的请求会被拒绝。

条件语义:

* ``expected`` 为字符串：键的当前已提交值必须逐字节等于它；
* ``expected`` 为 ``None``：键必须**不存在**（不是值为空串，也不是
  SQL NULL——kv 表 value 列为 NOT NULL，根本存不进 NULL）；
* 不带 ``expected``：无条件写。

所有条件评估都发生在参与者 prepare 的同一个写事务里：先对涉及的键
做存在性/冲突检查并插入磁盘锁，再在持锁状态下读取当前值比对，
“检查条件 -> 持锁 -> 落盘投票”原子完成，条件所约束的值在形成持久
决定前不会被其它已提交事务改变。
"""

import hashlib
import json

# 键不存在的哨兵：kv.value 为 NOT NULL，读出来是 Python None 即代表
# “没有这一行”；显式哨兵让“不存在”与任何可存储的值（含空串）可区分。
ABSENT = None


def canonical_ops(ops):
    """把请求规范化成确定顺序的列表，并严格校验类型。"""
    result = []
    for op in ops:
        if "key" not in op or "value" not in op:
            raise ValueError("each op requires string 'key' and 'value'")
        key = op["key"]
        value = op["value"]
        if not isinstance(key, str):
            raise ValueError("key must be a string")
        # value 只接受字符串；None/"None" 这种歧义在入口就拒绝，
        # 空字符串 "" 是合法且与“不存在”不同的值。
        if not isinstance(value, str):
            raise ValueError("value must be a string (null/absent is not storable)")
        item = {"key": key, "value": value}
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
    """完整请求的确定性身份字符串（规范化 JSON 的 sha256）。"""
    body = json.dumps(canonical_ops(ops), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def evaluate(conn, ops):
    """在**已持有 ops 全部键的磁盘锁**的写事务内评估全部条件。

    必须由参与者在同一 ``BEGIN IMMEDIATE`` 事务中、插入 locks 行之后
    调用：读到的要么是更早的已提交值，要么是本事务自己持锁——不存在
    检查与决定之间被其它事务改写的窗口。

    返回 ``None`` 表示全部满足；否则返回
    ``{"key", "expected", "actual", "exists"}`` 描述第一个不满足项，
    其中 ``exists=False`` 时 ``actual`` 恒为 ``None``。
    """
    for op in canonical_ops(ops):
        if "expected" not in op:
            continue
        row = conn.execute(
            "SELECT value FROM kv WHERE key=?", (op["key"],)
        ).fetchone()
        exists = row is not None
        actual = row[0] if exists else ABSENT
        if op["expected"] is None:
            # 要求“不存在”：键存在（哪怕值是空串）即不满足
            satisfied = not exists
        else:
            # 要求具体值：键必须存在且逐字节相等
            satisfied = exists and actual == op["expected"]
        if not satisfied:
            return {
                "key": op["key"],
                "expected": op["expected"],
                "actual": actual,
                "exists": exists,
            }
    return None
