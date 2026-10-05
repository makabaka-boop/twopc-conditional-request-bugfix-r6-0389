"""2PC 参与者。

每个参与者使用**独立的 SQLite 文件**，包含三类持久状态:

* ``kv``       —— 键值数据，只有已提交事务才能写入；
* ``txn_log``  —— 事务投票/结果日志（prepared / committed / aborted），
                  投票（yes/no）在回复协调者之前就已落盘；
* ``locks``    —— prepared 事务所持有的键锁，重启后仍然存在，
                  在收到协调者的提交/中止决定前绝不释放。

崩溃恢复语义: 重启后 ``prepared`` 的事务保持待决并继续持锁，
参与者**不会**自行猜测结论——只能等待协调者重发决定。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional

from . import rpc
from .conditions import canonical_ops, evaluate, identity

# txn_log.state
PREPARED = "prepared"
COMMITTED = "committed"
ABORTED = "aborted"


def _ops_fingerprint(ops_json: str) -> Optional[str]:
    """从参与者日志里存的 ops JSON 反推请求身份；空 ops（补记中止）为 None。"""
    ops = json.loads(ops_json)
    return identity(ops) if ops else None


class Participant:
    def __init__(
        self,
        name: str,
        db_path: str,
        crash_plan: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.name = name
        self.db_path = db_path
        # 崩溃点集合，每项形如 {"point": "...", "tid": "..."}，仅触发一次。
        self._crash_triggers: List[Dict[str, str]] = list(
            (crash_plan or {}).get("triggers", [])
        )
        self._lock = threading.Lock()  # 串行化写操作，SQLite 自身再兜底
        self._init_db()

    # ------------------------------------------------------------------ 存储

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        # WAL + FULL: commit 返回前 WAL 帧已 fsync，断电/被杀也不丢已确认状态。
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS kv ("
                "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS txn_log ("
                "tid TEXT PRIMARY KEY, state TEXT NOT NULL, ops TEXT NOT NULL, "
                "commit_count INTEGER NOT NULL DEFAULT 0)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS locks ("
                "key TEXT PRIMARY KEY, tid TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS meta ("
                "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )

    # --------------------------------------------------------------- 崩溃注入

    def _crash_if(self, point: str, tid: str) -> None:
        """命中开关则立即以退出码 99“崩溃”（进程被杀），开关一次性。

        只在状态成功 fsync 之后调用，从而能精确模拟
        “落盘后、通知前”这类窗口。
        """
        for i, trig in enumerate(self._crash_triggers):
            if trig.get("point") == point and trig.get("tid") == tid:
                del self._crash_triggers[i]
                os._exit(99)

    # ------------------------------------------------------------- 业务操作

    @staticmethod
    def _reject_mismatch(tid: str, stored: Optional[str], incoming: str) -> None:
        """同一 tid 已持久化的是另一个完整请求：明确拒绝，绝不按新内容执行。"""
        raise rpc.RPCError(
            f"REQUEST_MISMATCH {tid}: stored request {stored} "
            f"!= incoming {incoming}"
        )

    def _get_txn(self, conn: sqlite3.Connection, tid: str):
        return conn.execute(
            "SELECT state, ops, commit_count FROM txn_log WHERE tid=?", (tid,)
        ).fetchone()

    def prepare(self, tid: str, ops: List[Dict[str, Any]]) -> Dict[str, Any]:
        norm = canonical_ops(ops)
        incoming_id = identity(norm)
        keys = [o["key"] for o in norm]
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._get_txn(conn, tid)
                if row is not None:
                    # 幂等重试 / 恢复后协调者重发 prepare：
                    # 同一 tid 必须是同一个完整请求，否则拒绝。
                    state, stored_ops_json, _ = row
                    stored_id = _ops_fingerprint(stored_ops_json)
                    if stored_id is not None and stored_id != incoming_id:
                        conn.rollback()
                        self._reject_mismatch(tid, stored_id, incoming_id)
                    if state == PREPARED:
                        vote = "yes"
                    elif state == COMMITTED:
                        vote = "yes"
                    else:
                        vote = "no"
                    conn.commit()
                    return {
                        "vote": vote,
                        "state": state,
                        "duplicate": True,
                        "identity": incoming_id,
                    }

                # 投票 No 也要持久记录：检查键锁冲突
                holders = conn.execute(
                    "SELECT key, tid FROM locks WHERE key IN (%s)"
                    % ",".join("?" * len(keys)),
                    keys,
                ).fetchall()
                if holders:
                    conn.execute(
                        "INSERT INTO txn_log(tid, state, ops, commit_count) "
                        "VALUES(?,?,?,0)",
                        (tid, ABORTED, json.dumps(norm)),
                    )
                    conn.commit()
                    # 持久的 No 票已落盘再回复
                    self._crash_if("participant:vote:after", tid)
                    return {
                        "vote": "no",
                        "state": ABORTED,
                        "reason": "lock conflict",
                        "conflicts": [{"key": k, "tid": t} for k, t in holders],
                        "identity": incoming_id,
                    }

                # 先把涉及的键全部锁定，再在同一事务/同一持锁状态下评估
                # 条件：条件读到的值直到提交决定都受本事务的磁盘锁约束，
                # 不存在“检查后、决定前被改写”的窗口。
                conn.executemany(
                    "INSERT INTO locks(key, tid) VALUES(?,?)",
                    [(k, tid) for k in keys],
                )
                violation = evaluate(conn, norm)
                if violation is not None:
                    # 条件不满足：持久 No 票。键不属于本事务（已提交/中止
                    # 的旧值必须原样保留），不留下锁。
                    conn.execute("DELETE FROM locks WHERE tid=?", (tid,))
                    conn.execute(
                        "INSERT INTO txn_log(tid, state, ops, commit_count) "
                        "VALUES(?,?,?,0)",
                        (tid, ABORTED, json.dumps(norm)),
                    )
                    conn.commit()
                    self._crash_if("participant:vote:after", tid)
                    return {
                        "vote": "no",
                        "state": ABORTED,
                        "reason": "condition not satisfied",
                        "violation": violation,
                        "identity": incoming_id,
                    }

                # 持久记录 Yes 票（锁已在上面插入，随本事务一起落盘）
                conn.execute(
                    "INSERT INTO txn_log(tid, state, ops, commit_count) "
                    "VALUES(?,?,?,0)",
                    (tid, PREPARED, json.dumps(norm)),
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

        # Yes 票已 fsync，模拟“投票后、协调者决定前”崩溃
        self._crash_if("participant:vote:after", tid)
        return {"vote": "yes", "state": PREPARED, "identity": incoming_id}

    def commit(self, tid: str) -> Dict[str, Any]:
        applied_identity: Optional[str] = None
        with self._lock:
            conn = self._connect()
            try:
                self._crash_if("participant:commit:before", tid)
                conn.execute("BEGIN IMMEDIATE")
                row = self._get_txn(conn, tid)
                if row is None:
                    # 从未准备过（或 prepare 在落盘前崩溃）。
                    # 不能假装成功——让协调者稍后重试，事务保持待决。
                    conn.rollback()
                    raise rpc.RPCError(
                        f"UNKNOWN_TXN {tid}: not prepared on {self.name}"
                    )
                state, ops_json, _ = row
                applied_identity = _ops_fingerprint(ops_json)
                if state == COMMITTED:
                    conn.commit()
                    return {
                        "state": COMMITTED,
                        "applied": False,
                        "identity": _ops_fingerprint(ops_json),
                    }
                if state == ABORTED:
                    conn.rollback()
                    raise rpc.RPCError(f"CONFLICT {tid} already aborted on {self.name}")

                # prepared -> 应用写入、记录提交、释放锁，原子完成。
                # 写入内容只能来自 prepare 时持久化的同一个完整请求。
                ops = json.loads(ops_json)
                conn.executemany(
                    "INSERT INTO kv(key, value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    [(o["key"], o["value"]) for o in ops],
                )
                conn.execute(
                    "UPDATE txn_log SET state=?, commit_count=1 WHERE tid=?",
                    (COMMITTED, tid),
                )
                conn.execute("DELETE FROM locks WHERE tid=?", (tid,))
                conn.execute(
                    "INSERT INTO meta(key, value) "
                    "VALUES('applied_commits', "
                    "COALESCE((SELECT value FROM meta WHERE key='applied_commits'),"
                    "'0')) "
                    "ON CONFLICT(key) DO UPDATE SET value=value+0"
                )
                conn.execute(
                    "UPDATE meta SET value = CAST(CAST(value AS INTEGER)+1 AS TEXT) "
                    "WHERE key='applied_commits'"
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

        # 提交已落盘之后崩溃：协调者没收到 ack 会重试，幂等返回已提交
        self._crash_if("participant:commit:after", tid)
        return {
            "state": COMMITTED,
            "applied": True,
            "identity": applied_identity,
        }

    def abort(self, tid: str, ops: Optional[Any] = None) -> Dict[str, Any]:
        norm = canonical_ops(ops) if ops else []
        incoming_id = identity(norm) if norm else None
        with self._lock:
            conn = self._connect()
            try:
                self._crash_if("participant:abort:before", tid)
                conn.execute("BEGIN IMMEDIATE")
                row = self._get_txn(conn, tid)
                if row is None:
                    # prepare 可能没落盘（投票前崩溃）。补一条中止记录，
                    # 使重复 prepare 不可能再成功。
                    conn.execute(
                        "INSERT INTO txn_log(tid, state, ops, commit_count) "
                        "VALUES(?,?,?,0)",
                        (tid, ABORTED, json.dumps(norm)),
                    )
                else:
                    state, stored_ops_json, _ = row
                    if state == COMMITTED:
                        conn.rollback()
                        raise rpc.RPCError(
                            f"CONFLICT {tid} already committed on {self.name}"
                        )
                    stored_id = _ops_fingerprint(stored_ops_json)
                    if (
                        incoming_id is not None
                        and stored_id is not None
                        and stored_id != incoming_id
                    ):
                        # 同一个 tid 带着不同请求来要求中止：拒绝，
                        # 持久状态对应的是哪次约定不能被模糊掉。
                        conn.rollback()
                        self._reject_mismatch(tid, stored_id, incoming_id)
                    conn.execute(
                        "UPDATE txn_log SET state=? WHERE tid=?", (ABORTED, tid)
                    )
                conn.execute("DELETE FROM locks WHERE tid=?", (tid,))
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

        self._crash_if("participant:abort:after", tid)
        return {"state": ABORTED, "identity": incoming_id}

    def status(self, tid: str) -> Dict[str, Any]:
        conn = self._connect()
        try:
            row = self._get_txn(conn, tid)
            if row is None:
                return {"state": "unknown", "identity": None}
            state, ops_json, _ = row
            locks = [
                r[0]
                for r in conn.execute(
                    "SELECT key FROM locks WHERE tid=? ORDER BY key", (tid,)
                )
            ]
            return {
                "state": state,
                "locked_keys": locks,
                # 状态始终指向它实际持久的那一个完整请求，
                # 调用方可据此证明“执行的是哪次约定”。
                "identity": _ops_fingerprint(ops_json),
            }
        finally:
            conn.close()

    def get(self, key: str) -> Dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
            lock = conn.execute("SELECT tid FROM locks WHERE key=?", (key,)).fetchone()
            return {
                "key": key,
                # exists 显式区分“不存在”与“存在但值为空串”；
                # value 列为 NOT NULL，不存在时 value 为 null。
                "exists": row is not None,
                "value": row[0] if row else None,
                "locked_by": lock[0] if lock else None,
            }
        finally:
            conn.close()

    def dump(self) -> Dict[str, Any]:
        conn = self._connect()
        try:
            kv = dict(conn.execute("SELECT key, value FROM kv").fetchall())
            locks = dict(conn.execute("SELECT key, tid FROM locks").fetchall())
            txns = [
                {
                    "tid": t,
                    "state": s,
                    "commit_count": c,
                    "identity": _ops_fingerprint(o),
                }
                for t, s, o, c in conn.execute(
                    "SELECT tid, state, ops, commit_count FROM txn_log"
                )
            ]
            stat = conn.execute(
                "SELECT value FROM meta WHERE key='applied_commits'"
            ).fetchone()
            return {
                "name": self.name,
                "kv": kv,
                "locks": locks,
                "txns": txns,
                "applied_commits": int(stat[0]) if stat else 0,
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------ RPC

    def handle(
        self, method: str, params: Dict[str, Any], addr: tuple
    ) -> Dict[str, Any]:
        if method == "prepare":
            return self.prepare(params["tid"], params["ops"])
        if method == "commit":
            return self.commit(params["tid"])
        if method == "abort":
            return self.abort(params["tid"], params.get("ops"))
        if method == "status":
            return self.status(params["tid"])
        if method == "get":
            return self.get(params["key"])
        if method == "dump":
            return self.dump()
        raise rpc.RPCError(f"unknown method {method}")
