"""2PC 参与者。

每个参与者使用**独立的 SQLite 文件**，包含三类持久状态:

* ``kv``       —— 键值数据，只有已提交事务才能写入；``value`` 允许为
                  SQL NULL；“没有行”和“值为 NULL”是两种不同状态；
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

from . import conditions, rpc

# txn_log.state
PREPARED = "prepared"
COMMITTED = "committed"
ABORTED = "aborted"


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
            # 旧演示版本的 value 为 NOT NULL，无法保存真实 NULL。先原子迁移。
            kv_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='kv'"
            ).fetchone()
            needs_migration = False
            if kv_exists:
                value_col = next(
                    col for col in conn.execute("PRAGMA table_info(kv)")
                    if col[1] == "value"
                )
                needs_migration = bool(value_col[3])  # notnull flag
            if needs_migration:
                conn.execute("ALTER TABLE kv RENAME TO kv_old")
                conn.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
                conn.execute("INSERT INTO kv(key, value) SELECT key, value FROM kv_old")
                conn.execute("DROP TABLE kv_old")
            else:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)"
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
    def _canonical(ops: Any) -> List[Dict[str, Any]]:
        # 兼容更早日志中保存的 [key, value] 二元组表示。
        return conditions.coerce_stored_ops(ops)

    @classmethod
    def _stored_ops(cls, ops_json: str) -> List[Dict[str, Any]]:
        return cls._canonical(json.loads(ops_json))

    @staticmethod
    def _same_request(stored_json: str, ops: Any) -> bool:
        try:
            stored = json.loads(stored_json)
            return conditions.stored_identity(stored) == conditions.identity(ops)
        except (TypeError, ValueError):
            return False

    def _get_txn(self, conn: sqlite3.Connection, tid: str):
        return conn.execute(
            "SELECT state, ops, commit_count FROM txn_log WHERE tid=?", (tid,)
        ).fetchone()

    def prepare(self, tid: str, ops: List[Dict[str, Any]]) -> Dict[str, Any]:
        canonical = self._canonical(ops)
        keys = [o["key"] for o in canonical]
        with self._lock:
            conn = self._connect()
            try:
                # IMMEDIATE 保证：条件读取、Yes/No 落盘、键锁插入在同一个
                # 串行化的 SQLite 写事务里完成；提交前其它写事务无法修改
                # 刚检查过的键。
                conn.execute("BEGIN IMMEDIATE")
                row = self._get_txn(conn, tid)
                if row is not None:
                    state, ops_json, _ = row
                    if not self._same_request(ops_json, canonical):
                        conn.rollback()
                        raise rpc.RPCError(
                            f"REQUEST_MISMATCH {tid}: same transaction ID was "
                            "prepared with a different complete request"
                        )
                    # 幂等重试 / 恢复后协调者重发 prepare
                    vote = "yes" if state in (PREPARED, COMMITTED) else "no"
                    conn.commit()
                    return {
                        "vote": vote,
                        "state": state,
                        "duplicate": True,
                        "request_id": conditions.stored_identity(
                            json.loads(ops_json)
                        ),
                    }

                # 投票 No 也要持久记录：先检查键锁冲突。
                holders = conn.execute(
                    "SELECT key, tid FROM locks WHERE key IN (%s)"
                    % ",".join("?" * len(keys)),
                    keys,
                ).fetchall()
                if holders:
                    conn.execute(
                        "INSERT INTO txn_log(tid, state, ops, commit_count) "
                        "VALUES(?,?,?,0)",
                        (tid, ABORTED, json.dumps(canonical)),
                    )
                    conn.commit()
                    # 持久的 No 票已落盘再回复
                    self._crash_if("participant:vote:after", tid)
                    return {
                        "vote": "no",
                        "state": ABORTED,
                        "reason": "lock conflict",
                        "conflicts": [{"key": k, "tid": t} for k, t in holders],
                    }

                # 条件也在该写事务中检查；检查通过后，同一事务立即把
                # prepared 记录和全部键锁一起落盘，使观察值持续受约束。
                failed = conditions.condition_failure(conn, canonical)
                if failed is not None:
                    conn.execute(
                        "INSERT INTO txn_log(tid, state, ops, commit_count) "
                        "VALUES(?,?,?,0)",
                        (tid, ABORTED, json.dumps(canonical)),
                    )
                    conn.commit()
                    self._crash_if("participant:vote:after", tid)
                    return {
                        "vote": "no",
                        "state": ABORTED,
                        "reason": "condition mismatch",
                        "failed_condition": failed,
                    }

                # 持久记录 Yes 票并锁定涉及的键
                conn.execute(
                    "INSERT INTO txn_log(tid, state, ops, commit_count) "
                    "VALUES(?,?,?,0)",
                    (tid, PREPARED, json.dumps(canonical)),
                )
                conn.executemany(
                    "INSERT INTO locks(key, tid) VALUES(?,?)",
                    [(o["key"], tid) for o in canonical],
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

        # Yes 票已 fsync，模拟“投票后、协调者决定前”崩溃
        self._crash_if("participant:vote:after", tid)
        return {
            "vote": "yes",
            "state": PREPARED,
            "request_id": conditions.identity(canonical),
        }

    def commit(self, tid: str) -> Dict[str, Any]:
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
                if state == COMMITTED:
                    conn.commit()
                    return {
                        "state": COMMITTED,
                        "applied": False,
                        "request_id": conditions.stored_identity(
                            json.loads(ops_json)
                        ),
                    }
                if state == ABORTED:
                    conn.rollback()
                    raise rpc.RPCError(f"CONFLICT {tid} already aborted on {self.name}")

                # prepared -> 应用写入、记录提交、释放锁，原子完成
                ops = self._stored_ops(ops_json)
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
            "request_id": conditions.identity(ops),
        }

    def abort(self, tid: str, ops: Optional[Any] = None) -> Dict[str, Any]:
        canonical = self._canonical(ops) if ops is not None else None
        with self._lock:
            conn = self._connect()
            try:
                self._crash_if("participant:abort:before", tid)
                conn.execute("BEGIN IMMEDIATE")
                row = self._get_txn(conn, tid)
                if row is None:
                    # prepare 可能没落盘（投票前崩溃）。补一条中止记录，
                    # 使重复 prepare 不可能再成功。
                    stored = canonical if canonical is not None else []
                    conn.execute(
                        "INSERT INTO txn_log(tid, state, ops, commit_count) "
                        "VALUES(?,?,?,0)",
                        (tid, ABORTED, json.dumps(stored)),
                    )
                else:
                    state, ops_json, _ = row
                    if canonical is not None and not self._same_request(
                        ops_json, canonical
                    ):
                        conn.rollback()
                        raise rpc.RPCError(
                            f"REQUEST_MISMATCH {tid}: abort does not identify the "
                            "same complete request recorded on this participant"
                        )
                    if state == COMMITTED:
                        conn.rollback()
                        raise rpc.RPCError(
                            f"CONFLICT {tid} already committed on {self.name}"
                        )
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
        return {"state": ABORTED}

    def status(self, tid: str) -> Dict[str, Any]:
        conn = self._connect()
        try:
            row = self._get_txn(conn, tid)
            if row is None:
                return {"state": "unknown"}
            locks = [
                r[0]
                for r in conn.execute(
                    "SELECT key FROM locks WHERE tid=? ORDER BY key", (tid,)
                )
            ]
            return {
                "state": row[0],
                "locked_keys": locks,
                "request_id": conditions.stored_identity(json.loads(row[1])),
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
                {"tid": t, "state": s, "commit_count": c}
                for t, s, c in conn.execute(
                    "SELECT tid, state, commit_count FROM txn_log"
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
