"""核心校验 / 归并逻辑的单元测试，直接调用 solver（无需起 HTTP 服务）。"""
import pytest

from app.models import EventIn, ResolveRequest
from app.solver import SemanticValidationError, resolve


def ev(replica_id, seq, op, key, clock, value=None):
    data = {"replica_id": replica_id, "seq": seq, "op": op, "key": key, "clock": list(clock)}
    if op == "put":
        data["value"] = value
    return EventIn(**data)


def req(replica_ids, events):
    return ResolveRequest(replica_ids=replica_ids, events=events)


def _as_map(results):
    return {r["key"]: r for r in results}


def test_happy_path_present_deleted_and_merged():
    # A: put k1 v1, put k1 v3；B: delete k1，B 的第 2 条合并了 A 的全部两条 -> 最终 deleted
    r = req(
        ["A", "B"],
        [
            ev("A", 1, "put", "k1", [1, 0], value="v1"),
            ev("A", 2, "put", "k1", [2, 0], value="v3"),
            ev("B", 1, "delete", "k1", [0, 1]),
            ev("B", 2, "delete", "k1", [2, 2]),
        ],
    )
    out = _as_map(resolve(r))
    assert out["k1"]["status"] == "deleted"
    assert out["k1"]["value"] is None
    assert out["k1"]["events"] == [
        {"replica_id": "B", "seq": 2, "clock": [2, 2], "value": None}
    ]


def test_concurrent_puts_conflict_even_with_same_value():
    r = req(
        ["A", "B"],
        [
            ev("A", 1, "put", "k", [1, 0], value="same"),
            ev("B", 1, "put", "k", [0, 1], value="same"),
        ],
    )
    out = _as_map(resolve(r))["k"]
    assert out["status"] == "conflict"
    assert out["value"] is None
    assert [(e["replica_id"], e["seq"]) for e in out["events"]] == [("A", 1), ("B", 1)]


def test_results_sorted_by_key_and_conflict_events_sorted():
    r = req(
        ["r2", "r1"],  # 故意让数组顺序与字典序不同
        [
            ev("r2", 1, "put", "z", [1, 0], value="z"),
            ev("r1", 1, "put", "z", [0, 1], value="zz"),
            ev("r2", 2, "put", "a", [2, 0], value="a"),
        ],
    )
    results = resolve(r)
    assert [x["key"] for x in results] == ["a", "z"]
    z = results[1]
    assert [e["replica_id"] for e in z["events"]] == ["r1", "r2"]


def test_third_replica_only_in_clock_can_be_idle():
    # 副本 C 没有任何事件；A、B 的时钟 C 分量恒为 0
    r = req(
        ["A", "B", "C"],
        [
            ev("A", 1, "put", "k", [1, 0, 0], value="v"),
            ev("B", 1, "put", "k", [1, 1, 0], value="w"),
        ],
    )
    out = _as_map(resolve(r))["k"]
    assert out["status"] == "present"
    assert out["value"] == "w"


def test_seq_gap_rejected():
    r = req(["A", "B"], [ev("A", 2, "put", "k", [2, 0], value="v")])
    with pytest.raises(SemanticValidationError) as ei:
        resolve(r)
    rules = {e["rule"] for e in ei.value.errors}
    assert "seq_must_start_at_1" in rules


def test_duplicate_seq_rejected():
    r = req(
        ["A", "B"],
        [
            ev("A", 1, "put", "k", [1, 0], value="v1"),
            ev("A", 1, "put", "k", [1, 0], value="v2"),
        ],
    )
    with pytest.raises(SemanticValidationError) as ei:
        resolve(r)
    assert any(e["rule"] == "duplicate_seq" for e in ei.value.errors)


def test_own_clock_component_must_equal_seq():
    r = req(
        ["A", "B"],
        [
            ev("A", 1, "put", "k", [1, 0], value="v1"),
            ev("A", 2, "put", "k", [1, 0], value="v2"),  # 本分量应为 2
        ],
    )
    with pytest.raises(SemanticValidationError) as ei:
        resolve(r)
    assert any(
        e["rule"] == "own_clock_component_must_equal_seq" and e["seq"] == 2
        for e in ei.value.errors
    )


def test_clock_component_pointing_to_missing_event():
    r = req(
        ["A", "B"],
        [
            ev("A", 1, "put", "k", [1, 0], value="v1"),
            ev("A", 2, "put", "k", [2, 5], value="v2"),  # B 没有事件，分量 5 非法
        ],
    )
    with pytest.raises(SemanticValidationError) as ei:
        resolve(r)
    assert any(
        e["rule"] == "clock_points_to_missing_event" and e["seq"] == 2
        for e in ei.value.errors
    )


def test_predecessor_clock_must_be_componentwise_le():
    # B 第 2 条声称见过 A#2，但 A#2 自己又引用了 B#2 -> A#2 时钟不是 B#2 的 <=
    r = req(
        ["A", "B"],
        [
            ev("A", 1, "put", "k", [1, 0], value="a1"),
            ev("B", 1, "put", "k", [0, 1], value="b1"),
            ev("A", 2, "put", "k", [2, 2], value="a2"),
            ev("B", 2, "put", "k", [2, 2], value="b2"),
        ],
    )
    with pytest.raises(SemanticValidationError) as ei:
        resolve(r)
    assert any(
        e["rule"] in {"predecessor_clock_not_le", "predecessor_clock_must_advance"}
        for e in ei.value.errors
    )


def test_three_replica_diamond_delete_dominates():
    # A1 -> B1(合并 A) -> C1(合并 A、B)，C 的 delete 为唯一极大事件
    r = req(
        ["A", "B", "C"],
        [
            ev("A", 1, "put", "k", [1, 0, 0], value="a"),
            ev("B", 1, "put", "k", [1, 1, 0], value="b"),
            ev("C", 1, "delete", "k", [1, 1, 1]),
        ],
    )
    out = _as_map(resolve(r))["k"]
    assert out["status"] == "deleted"
    assert out["events"][0] == {"replica_id": "C", "seq": 1, "clock": [1, 1, 1], "value": None}


def test_delete_event_payload_never_exposes_value():
    r = req(
        ["A", "B"],
        [ev("A", 1, "delete", "k", [1, 0]), ev("B", 1, "put", "k", [1, 1], value="v")],
    )
    out = _as_map(resolve(r))["k"]
    assert out["status"] == "present"
    assert out["value"] == "v"
