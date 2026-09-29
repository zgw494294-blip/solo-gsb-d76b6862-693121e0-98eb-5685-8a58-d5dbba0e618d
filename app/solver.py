"""多副本离线写入的整批校验与向量时钟归并。

校验规则（任何一条不满足，整批拒绝）：
1. 每个副本的 seq 不得重复，且必须从 1 开始连续；
2. 向量时钟中“本副本分量”必须等于该事件的 seq；
3. 时钟任何分量指向的事件都必须存在（大于该副本最大 seq 即指向缺失事件；
   没有事件的副本分量只能为 0）；
4. 对当前事件 e（副本 r，seq=s）：
   - 同副本上一事件 (r, s-1)（s>1 时）；
   - 对每个其他副本 q，其时钟分量 c[q]>0 时指向的最新事件 (q, c[q])；
   这些前驱事件的时钟必须逐分量 <= c[e]，且至少有一个分量严格更小。

归并规则：对每个 key，按向量时钟偏序（a < b：逐分量 <= 且至少一项 <）
找极大事件集合。唯一 put 给值，唯一 delete 标记删除；多个极大事件即使
value 相同也报告冲突。
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from .models import EventIn, ResolveRequest


class SemanticValidationError(Exception):
    """语义校验失败：携带每个违规事件的定位信息。"""

    def __init__(self, errors: list[dict[str, Any]]):
        self.errors = errors
        super().__init__(f"{len(errors)} 个语义校验错误")


def _ref(event: EventIn, index: int | None = None) -> dict[str, Any]:
    """在 422 详情中定位一个事件。"""
    ref: dict[str, Any] = {
        "replica_id": event.replica_id,
        "seq": event.seq,
        "key": event.key,
    }
    if index is not None:
        ref["events_index"] = index
    return ref


def _leq(a: list[int], b: list[int]) -> bool:
    return all(x <= y for x, y in zip(a, b))


def resolve(req: ResolveRequest) -> list[dict[str, Any]]:
    """校验请求并返回按 key 排序的归并结果；校验失败抛 SemanticValidationError。"""
    replica_ids = req.replica_ids
    idx_of = {rid: i for i, rid in enumerate(replica_ids)}
    n = len(replica_ids)

    # (replica_id, seq) -> (事件, 请求数组下标)
    by_replica: dict[str, list[tuple[EventIn, int]]] = defaultdict(list)
    for index, event in enumerate(req.events):
        by_replica[event.replica_id].append((event, index))

    errors: list[dict[str, Any]] = []

    # ---- 阶段 1：每副本 seq 不得重复，且必须从 1 起连续 ----
    # 记录重复 seq，后续阶段跳过重复事件以免产生误导性错误
    duplicated: set[tuple[str, int]] = set()
    for rid, pairs in by_replica.items():
        pairs.sort(key=lambda p: p[0].seq)
        seen: set[int] = set()
        for event, index in pairs:
            if event.seq in seen:
                duplicated.add((rid, event.seq))
                errors.append(
                    {
                        **_ref(event, index),
                        "rule": "duplicate_seq",
                        "message": f"副本 {rid!r} 的 seq={event.seq} 重复",
                    }
                )
            seen.add(event.seq)

        # 从最小 seq 开始定位首个缺口；最小 seq 不为 1 时从首个事件报起
        unique_seqs = sorted(seen)
        if unique_seqs and unique_seqs[0] != 1:
            bad_event, bad_index = next(
                (e, i) for e, i in pairs if e.seq == unique_seqs[0]
            )
            errors.append(
                {
                    **_ref(bad_event, bad_index),
                    "rule": "seq_must_start_at_1",
                    "message": f"副本 {rid!r} 的 seq 必须从 1 开始，首个 seq 为 {unique_seqs[0]}",
                }
            )
        expected = unique_seqs[0] + 1 if unique_seqs else 1
        for s in unique_seqs[1:]:
            if s != expected:
                bad_event, bad_index = next(
                    (e, i) for e, i in pairs if e.seq == s
                )
                errors.append(
                    {
                        **_ref(bad_event, bad_index),
                        "rule": "seq_must_be_contiguous",
                        "message": (
                            f"副本 {rid!r} 的 seq 不连续：期望 {expected}，实际为 {s}"
                        ),
                    }
                )
                # 缺口之后不再逐个报连续性错误
                break
            expected += 1

    if errors:
        raise SemanticValidationError(errors)

    lookup: dict[tuple[str, int], EventIn] = {
        (rid, event.seq): event for rid, pairs in by_replica.items() for event, _ in pairs
    }

    # ---- 阶段 2：本副本分量 == seq；任何分量不得指向缺失事件 ----
    for index, event in enumerate(req.events):
        own = idx_of[event.replica_id]
        if event.clock[own] != event.seq:
            errors.append(
                {
                    **_ref(event, index),
                    "rule": "own_clock_component_must_equal_seq",
                    "message": (
                        f"本副本分量 clock[{own}]={event.clock[own]}，必须等于 seq={event.seq}"
                    ),
                }
            )
        for q, component in enumerate(event.clock):
            if component == 0:
                continue
            if (replica_ids[q], component) not in lookup:
                errors.append(
                    {
                        **_ref(event, index),
                        "rule": "clock_points_to_missing_event",
                        "message": (
                            f"clock[{q}]（副本 {replica_ids[q]!r}）"
                            f"= {component} 指向了不存在的事件"
                        ),
                    }
                )

    if errors:
        raise SemanticValidationError(errors)

    # ---- 阶段 3：前驱时钟必须严格小于当前时钟（逐分量 <=，且至少一项 <）----
    for index, event in enumerate(req.events):
        own = idx_of[event.replica_id]
        predecessors: list[EventIn] = []
        if event.seq > 1:
            predecessors.append(lookup[(event.replica_id, event.seq - 1)])
        for q, component in enumerate(event.clock):
            if q == own or component == 0:
                continue
            predecessors.append(lookup[(replica_ids[q], component)])

        for pred in predecessors:
            if not _leq(pred.clock, event.clock):
                errors.append(
                    {
                        **_ref(event, index),
                        "rule": "predecessor_clock_not_le",
                        "message": (
                            f"前驱事件 ({pred.replica_id!r}, seq={pred.seq}) 的时钟 "
                            f"{pred.clock} 不是当前时钟 {event.clock} 的逐分量 <="
                        ),
                    }
                )
            elif pred.clock == event.clock:
                errors.append(
                    {
                        **_ref(event, index),
                        "rule": "predecessor_clock_must_advance",
                        "message": (
                            f"前驱事件 ({pred.replica_id!r}, seq={pred.seq}) 的时钟 "
                            f"{pred.clock} 与当前时钟完全相同，至少一个分量必须严格增长"
                        ),
                    }
                )

    if errors:
        raise SemanticValidationError(errors)

    # ---- 阶段 4：按 key 分组，求向量时钟偏序下的极大事件 ----
    by_key: dict[str, list[EventIn]] = defaultdict(list)
    for event in req.events:
        by_key[event.key].append(event)

    results: list[dict[str, Any]] = []
    for key in sorted(by_key):
        events = by_key[key]
        maximals: list[EventIn] = []
        for event in events:
            dominated = any(
                other is not event
                and _leq(event.clock, other.clock)
                and event.clock != other.clock
                for other in events
            )
            if not dominated:
                maximals.append(event)

        maximals.sort(key=lambda e: (e.replica_id, e.seq))

        entry: dict[str, Any] = {"key": key, "events": [_event_out(e) for e in maximals]}
        if len(maximals) == 1:
            only = maximals[0]
            if only.op == "put":
                entry["status"] = "present"
                entry["value"] = only.value
            else:
                entry["status"] = "deleted"
                entry["value"] = None
        else:
            entry["status"] = "conflict"
            entry["value"] = None
        results.append(entry)

    return results


def _event_out(event: EventIn) -> dict[str, Any]:
    return {
        "replica_id": event.replica_id,
        "seq": event.seq,
        "clock": event.clock,
        "value": event.value if event.op == "put" else None,
    }
