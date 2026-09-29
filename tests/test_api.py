"""Tests for the multi-replica offline write reconciliation API."""

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def ev(replica, seq, key, put=None, delete=None, clock=None):
    e = {"replica_id": replica, "seq": seq, "key": key, "clock": clock or {}}
    if put is not None:
        e["put"] = put
    elif delete is not None:
        e["delete"] = delete
    else:
        e["delete"] = "delete"
    return e


def post(replica_ids, events):
    return client.post(
        "/api/resolve", json={"replica_ids": replica_ids, "events": events}
    )


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_concurrent_puts_conflict_even_when_values_equal():
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="x", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "conflict"
    assert [e["replica_id"] for e in item["events"]] == ["r1", "r2"]


def test_results_sorted_by_key_and_put_shadows_older_event():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "z", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "a", delete="delete", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "a", put="kept", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    results = r.json()["results"]
    assert [x["key"] for x in results] == ["a", "z"]
    assert results[0]["state"] == "put"
    assert results[0]["value"] == "kept"
    assert results[0]["event"]["replica_id"] == "r1"
    assert results[0]["event"]["seq"] == 2


def test_unique_maximal_delete_is_reported_deleted():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", delete="delete", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "deleted"
    assert item["event"]["op"] == "delete"
    assert item["event"]["value"] is None


def test_events_may_arrive_out_of_order():
    events = [
        ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 1}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1}),
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
    ]
    r = post(["r1", "r2"], events)
    assert r.status_code == 200, r.text
    k = [x for x in r.json()["results"] if x["key"] == "k"][0]
    assert k["state"] == "put"
    assert k["value"] == "c"


def test_empty_put_value_is_allowed():
    r = post(
        ["a", "b"],
        [ev("a", 1, "k", put="", clock={"a": 1, "b": 0})],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "put"
    assert item["value"] == ""


def test_conflict_events_sorted_by_replica_then_seq():
    # Concurrent branches after common predecessor r2/1:
    # r1/2 and r2/2 are incomparable (each advances a different component).
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "other", put="x", clock={"r1": 1, "r2": 0}),
            ev("r2", 2, "k", put="d", clock={"r1": 1, "r2": 2}),
            ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    item = [x for x in r.json()["results"] if x["key"] == "k"][0]
    assert item["state"] == "conflict"
    assert [(e["replica_id"], e["seq"]) for e in item["events"]] == [
        ("r1", 2),
        ("r2", 2),
    ]


def test_three_replicas_and_eventless_replica():
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0, "r3": 0}),
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["results"][0]["event"]["clock"] == {"r1": 1, "r2": 0, "r3": 0}


# ---------------------------------------------------------------------------
# Validation failures -> 422
# ---------------------------------------------------------------------------


def assert_422(r, code=None):
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    if code is not None:
        assert any(e.get("code") == code for e in detail)
    return detail


def test_duplicate_replica_ids_rejected():
    r = post(["r1", "r1"], [ev("r1", 1, "k", put="v", clock={"r1": 0})])
    assert_422(r, "duplicate_replica_id")


def test_unknown_event_replica_rejected():
    r = post(
        ["r1", "r2"],
        [ev("r9", 1, "k", put="v", clock={"r1": 0, "r2": 0})],
    )
    assert_422(r, "unknown_replica_id")


def test_clock_missing_and_extra_components():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r3": 0})],
    )
    assert_422(r, "clock_replica_coverage")


def test_clock_negative_component():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": -1})],
    )
    assert r.status_code == 422


def test_self_component_must_equal_seq():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
    )
    detail = assert_422(r, "clock_self_component")
    assert detail[0]["event_index"] == 0


def test_duplicate_seq_rejected():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 1, "k", put="b", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert_422(r, "duplicate_seq")


def test_gap_in_seq_rejected():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 3, "k", put="b", clock={"r1": 3, "r2": 0}),
        ],
    )
    assert_422(r, "non_contiguous_seq")


def test_seq_must_start_at_one():
    r = post(
        ["r1", "r2"],
        [ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 0})],
    )
    assert_422(r, "non_contiguous_seq")


def test_clock_pointing_to_missing_event():
    # r1/1 claims to know r3/1, but no event was produced by r3.
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0, "r3": 1}),
        ],
    )
    detail = assert_422(r, "clock_points_to_missing_event")
    assert detail[0]["event_index"] == 0


def test_other_replica_clock_equal_rejected():
    # Alias covered by test_equal_clock_events_are_causally_invalid:
    # re-verify the offending event index is reported.
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 0


def test_equal_clock_events_are_causally_invalid():
    # Cross-replica: two events with identical clocks cannot reference each
    # other (the referenced clock must be strictly smaller).
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert_422(r, "clock_causality")


def test_predecessor_clock_with_component_greater_rejected():
    # r1/1 knows r2/2 (clock r2=2), but r1/2 only knows r2/1 (r2=1):
    # the predecessor's clock is not <= the current one. Events arrive
    # out of order; the violation must still be pinpointed at r1/2.
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "x", put="x", clock={"r1": 0, "r2": 1}),
            ev("r2", 2, "x", put="x", clock={"r1": 0, "r2": 2}),
            ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 2}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 2


def test_other_component_referenced_clock_not_le_rejected():
    # Three replicas: r1/1 references r2/1 (r2 component = 1), but r2/1
    # already knows r3/1 while r1/1 does not -> referenced clock not <=.
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r3", 1, "z", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1, "r3": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1, "r3": 0}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 2


def test_put_and_delete_together_rejected():
    r = post(
        ["r1", "r2"],
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "k",
                "put": "v",
                "delete": "delete",
                "clock": {"r1": 1, "r2": 0},
            }
        ],
    )
    assert r.status_code == 422


def test_delete_field_wrong_value_rejected():
    r = post(
        ["r1", "r2"],
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "k",
                "delete": "remove",
                "clock": {"r1": 1, "r2": 0},
            }
        ],
    )
    assert r.status_code == 422


def test_boundary_replica_and_event_counts():
    # 8 replicas, single event
    ids = [f"r{i}" for i in range(8)]
    r = post(ids, [ev("r0", 1, "k", put="v", clock={rid: 0 for rid in ids} | {"r0": 1})])
    assert r.status_code == 200, r.text

    # 1 replica -> schema error
    r = post(["only"], [ev("only", 1, "k", put="v", clock={"only": 1})])
    assert r.status_code == 422

    # 121 events (one replica with 121 seqs) -> schema error
    many = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0}) for n in range(1, 122)
    ]
    r = post(["r1", "r2"], many)
    assert r.status_code == 422


def test_empty_events_rejected():
    r = client.post(
        "/api/resolve", json={"replica_ids": ["r1", "r2"], "events": []}
    )
    assert r.status_code == 422


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
