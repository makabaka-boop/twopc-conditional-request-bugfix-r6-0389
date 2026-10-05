# 两阶段提交（2PC）演示

一个协调者 + 2～3 个使用**独立 SQLite 文件**的参与者，事务内容是向各参与者
写入同一组键值。包含崩溃注入与基于日志的崩溃恢复。

## 目录结构

```
twopc/
  rpc.py          行分隔 JSON 的 TCP RPC（每连接一个请求，天然支持幂等重试）
  participant.py  参与者：kv / txn_log / locks 三张表，投票先落盘再回复
  coordinator.py  协调者：事务日志 + 决定先落盘再通知 + 后台恢复扫描
  __main__.py     进程入口（participant / coordinator 两个子命令）
tests/
  test_2pc.py     真实子进程、真实崩溃(kill)重启、竞争事务交错的端到端测试
```

## 核心语义（与题面逐条对应）

| 要求 | 实现 |
| --- | --- |
| 参与者独立 SQLite 文件 | 每个 `participant --db X.db` 独立文件，WAL + `synchronous=FULL`，commit 返回即 fsync |
| prepare 阶段持久记录投票并锁键 | `txn_log` 写入 `prepared` 与 `locks` 行在**同一个事务**里提交，成功后才回 yes；no 票同样落盘 |
| 协调者先持久决定再通知 | `commit/abort` 决定先写入协调者日志（fsync），之后才向任何参与者发 commit/abort |
| 协调者不可达时参与者保持待决 | prepared 的参与者重启后仍是 prepared 且继续持锁，没有“超时自动提交/中止”，只能等协调者重发决定 |
| 相同事务 ID 重试不重复执行 | 协调者按 tid 串行化（进行中的并发重试直接返回 `pending`）；参与者 commit/abort 幂等，已提交事务 `commit_count` 恒为 1 |
| 同一 tid 必须是同一个完整请求 | 完整请求（全部 key/value/expected）规范化后取 sha256 作为 `identity`，随协调者日志与参与者 `txn_log` 一起落盘；重试/重启后值或条件不同一律返回 `REQUEST_MISMATCH` 错误，绝不按新内容执行 |
| 成功响应 ⇒ 全部参与者最终可恢复到提交 | 只有所有参与者 ack（本地提交已落盘）才返回 `committed`；ack 不全一律 `pending`，由后台 sweeper 补完 |
| 无法确定时返回待决 | 参与者连接失败/UNKNOWN_TXN 等一律不假装成功，返回 `pending` + `waiting_on` |
| 重启后按日志恢复 | 协调者启动与后台扫描：`committing→补 commit`、`aborting→补 abort`、残留 `preparing→推定中止`；通知进度按 `txn_ack` 断点续传 |
| 不发生部分提交 | 参与者本地“写 kv + 记 committed + 解锁”是一个原子事务；测试逐节点比对状态一致性 |
| 不丢锁 | 锁是磁盘表，崩溃不丢；仅在提交/中止的同一原子事务里释放 |
| 竞争事务顺序一致 | 协调者维护键所有权表，作为冲突事务的全局串行化点；参与者磁盘键锁兜底，避免不同参与者以相反顺序应用两个已提交事务 |

> 说明：经典 2PC 只保证原子性，不保证跨节点冲突可串行化。这里由协调者
> 额外充当冲突串行化点（内存键锁表 + 参与者磁盘键锁），保证竞争事务在
> 所有参与者上以相同顺序生效。协调者崩溃后键锁表清空，由恢复扫描按
> 日志顺序重新接回。

## 崩溃注入开关

通过 `--crash plan.json` 传入，进程命中对应点立即以退出码 99 退出
（`os._exit`，模拟被 kill -9），开关一次性，重启不带开关即正常恢复。

参与者注入点：

- `participant:vote:after`        投票(含锁)落盘后
- `participant:commit:before`     收到 commit、应用提交之前
- `participant:commit:after`      提交落盘后、ack 之前
- `participant:abort:before` / `participant:abort:after`

协调者注入点：

- `coordinator:prepare:after`                 prepare 扇出之后、决定之前
- `coordinator:commit:decision:before/after`  提交决定落盘的前/后
- `coordinator:commit:notify:before/after`    commit 通知的前/后
- `coordinator:abort:decision:before/after`
- `coordinator:abort:notify:before/after`

plan 文件示例：

```json
{"triggers": [{"point": "coordinator:commit:decision:after", "tid": "t1"}]}
```

## 运行

启动 2 个参与者和协调者：

```bash
python -m twopc participant --name p1 --port 9101 --db /tmp/p1.db
python -m twopc participant --name p2 --port 9102 --db /tmp/p2.db
python -m twopc coordinator --port 9100 --db /tmp/c.db \
  --participants p1=127.0.0.1:9101,p2=127.0.0.1:9102
```

提交事务（Python 示例）：

```python
from twopc import rpc
rpc.call(("127.0.0.1", 9100), "submit",
         {"tid": "t1", "ops": [{"key": "a", "value": "1"}]})
# -> {'tid': 't1', 'state': 'committed'}
rpc.call(("127.0.0.1", 9100), "status", {"tid": "t1"})
```

响应状态：`committed` / `aborted`（终态），`pending`（待决，需重试或等待
后台恢复）；重复 tid 的重试返回终态并带 `duplicate: true`。

## 测试

```bash
python tests/test_2pc.py        # 不需要 pytest
# 或
python -m pytest tests/test_2pc.py -v
```

18 个用例：10 个 2PC 基础/崩溃恢复用例（基本提交、锁冲突全组中止、
重复 tid 幂等、投票后崩溃持锁待决、commit 落盘后 ack 前崩溃不假成功、
收到 commit 前崩溃重启补提交、协调者决定落盘前/后崩溃、prepare 中崩溃
推定中止、8 个竞争事务线程交错 + 协调者中途被 kill 的压力场景），
以及 8 个条件写用例：

11. 节点原值不一致时只有一端不满足也整组中止，旧值/分叉保留，
    条件与无条件写混合同样全组不写；
12. 空字符串与键不存在三态可区分（`expected` 为字符串/null/缺省）；
13. 同一 tid 换值/换条件/增删 op 被 `REQUEST_MISMATCH` 拒绝，
    协调者重启后身份仍生效，原请求重试幂等且 `commit_count==1`；
14. 条件事务投票后崩溃：参与者磁盘上保留的是完整条件请求，重启补
    中止、旧值保留，换请求的 prepare 被拒；
15a–d（参与者进程内单元用例）检查与持锁同一原子事务（持锁待决期间
    竞争者只能拿 lock-conflict No）、条件失败投持久 No 不持锁不写、
    空串/不存在/期望值三态、同一 tid 不同 prepare 被拒。

端到端用例校验：

1. 每个事务在所有参与者上的终态与协调者一致（无部分提交）；
2. 无残留/丢失锁；
3. 每个提交 `commit_count == 1`（无重复执行）；
4. 每个键在所有参与者上的最终值一致，且等于某个已提交事务写入的值。

## Conditional writes

A submit operation may include `expected` on each op:

- `expected` 为**字符串**：该参与者本地的键当前值必须逐字节等于它；
- `expected` 为 `null`：键必须**不存在**（`exists=false`；空字符串 `""`
  是与“不存在”不同的合法值）；
- 不写 `expected`：无条件 put。

语义与实现要点：

1. **每个参与者独立检查全部条件**。条件评估发生在参与者 `prepare` 的
   同一个 `BEGIN IMMEDIATE` 磁盘事务里：先插入本事务全部键的磁盘锁，
   再在持锁状态下读取当前值比对，然后才落盘投票。因此“判断条件 →
   形成持久决定”期间条件所约束的值一直被本事务锁住，不存在检查后被
   其它事务改写的窗口；协调者侧**不做**只读预检（那只会读到某一个
   节点且不持锁，节点原值不一致时漏判）。
2. **任一节点拒绝则整组不应用写入**。条件不满足投持久 No 票（不持锁、
   旧值原样保留），走既有 abort 路径；同一事务里条件写与无条件写
   混合时，无条件写也不会在条件失败的节点上生效。中止响应带
   `rejected_by`，列出每个拒绝节点及其 `violation`
   （`key/expected/actual/exists`）。
3. **空值与不存在可区分**。`value` 列 NOT NULL；参与者 `get` 返回
   `exists`（不存在时 `exists=false, value=null`），空串则
   `exists=true, value=""`。非字符串 value 在入口直接拒绝。
4. **同一事务编号永远对应同一个完整请求**。规范化后的请求身份
   `identity`（sha256）随协调者与各参与者日志持久化；协调者 `submit`
   与参与者 `prepare/abort` 都会比对身份，换值/换条件/增删 op 的重试
   或重启后请求得到 `REQUEST_MISMATCH`。所有终态响应、协调者/参与者
   `status`、`dump` 都带 `identity`，可证明状态实际对应的是哪次约定。
