"""HTTP API for reconciling multi-replica offline writes.

A batch of events (each carrying a per-replica sequence number and a vector
clock covering every replica) is validated against the causal history it
declares, and then reduced to the effective version of every key under the
vector-clock partial order.

The service is stateless: request payloads are never persisted.
"""

from __future__ import annotations

from typing import Annotated, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictStr
from pydantic import model_validator

app = FastAPI(
    title="Multi-Replica Offline Write Reconciler",
    version="1.0.0",
    description=(
        "Validate a batch of offline write/delete events from multiple "
        "replicas and resolve the effective (maximal) version of each key."
    ),
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class Event(BaseModel):
    """A single put/delete event observed on one replica."""

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(min_length=1, description="Non-empty ID of the originating replica."),
    ]
    seq: Annotated[
        int,
        Field(
            strict=True,
            ge=1,
            description="Per-replica sequence number, starting at 1.",
        ),
    ]
    key: Annotated[str, Field(min_length=1, description="Non-empty key.")]
    put: Optional[StrictStr] = Field(
        default=None, description="Set when the event is a put (empty string allowed)."
    )
    delete: Optional[StrictStr] = Field(
        default=None, description="Must equal 'delete' when the event is a deletion."
    )
    clock: dict[
        Annotated[str, Field(min_length=1)],
        Annotated[int, Field(strict=True, ge=0)],
    ] = Field(
        ...,
        description=(
            "Vector clock covering every replica ID; non-negative integer "
            "component per replica."
        ),
    )

    @model_validator(mode="after")
    def _exactly_one_op(self) -> "Event":
        has_put = self.put is not None
        has_delete = self.delete is not None
        if has_put == has_delete:
            raise ValueError(
                "exactly one of 'put' (string value) or 'delete' (\"delete\") "
                "must be provided"
            )
        if has_delete and self.delete != "delete":
            raise ValueError("the 'delete' field must equal the string \"delete\"")
        return self


class ResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replica_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]],
        Field(
            min_length=2,
            max_length=8,
            description="2-8 non-empty, unique replica IDs.",
        ),
    ]
    events: Annotated[
        list[Event],
        Field(min_length=1, max_length=120, description="1-120 events, any order."),
    ]


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class BatchError(Exception):
    """A whole-batch semantic validation failure (HTTP 422)."""

    def __init__(self, errors: list[dict]):
        super().__init__("invalid event batch")
        self.errors = errors


def _err(code: str, message: str, event_index: int | None = None) -> dict:
    e: dict = {"code": code, "message": message}
    if event_index is not None:
        e["event_index"] = event_index
    return e


# ---------------------------------------------------------------------------
# Validation + resolution
# ---------------------------------------------------------------------------


def _clock_le(
    a: dict[str, int], b: dict[str, int], replicas: list[str]
) -> tuple[bool, bool]:
    """Return (<= under componentwise order, < under partial order).

    ``a <= b`` when every component of ``a`` is <= the corresponding
    component of ``b``; ``a < b`` when additionally some component is
    strictly smaller.
    """

    any_less = False
    for r in replicas:
        x, y = a[r], b[r]
        if x > y:
            return False, False
        if x < y:
            any_less = True
    return True, any_less


def validate_batch(
    replica_ids: list[str], events: list[Event]
) -> dict[tuple[str, int], Event]:
    """Validate the batch and index events by (replica_id, seq).

    Raises BatchError (-> 422) on any violation, pointing at event indices.
    """

    errors: list[dict] = []

    # Rule 1: replica IDs unique (cardinality/non-empty handled by schema).
    seen_replicas: set[str] = set()
    duplicate_replicas: set[str] = set()
    for rid in replica_ids:
        if rid in seen_replicas:
            duplicate_replicas.add(rid)
        seen_replicas.add(rid)
    for rid in sorted(duplicate_replicas):
        errors.append(
            _err("duplicate_replica_id", f"replica_id {rid!r} appears more than once")
        )

    # Per-replica structural checks, one pass.
    seen: dict[tuple[str, int], Event] = {}
    seen_index: dict[tuple[str, int], int] = {}
    seqs_by_replica: dict[str, list[int]] = {rid: [] for rid in replica_ids}

    for i, ev in enumerate(events):
        bad = False

        if ev.replica_id not in seen_replicas:
            errors.append(
                _err(
                    "unknown_replica_id",
                    f"event replica_id {ev.replica_id!r} is not listed in replica_ids",
                    i,
                )
            )
            bad = True
        else:
            seqs_by_replica[ev.replica_id].append(ev.seq)

        # Rule 3: clock covers exactly all replicas, non-negative integers.
        clock_keys = set(ev.clock)
        if clock_keys != seen_replicas:
            missing = sorted(seen_replicas - clock_keys)
            extra = sorted(clock_keys - seen_replicas)
            parts = []
            if missing:
                parts.append(f"missing components for {missing}")
            if extra:
                parts.append(f"unexpected components {extra}")
            errors.append(
                _err(
                    "clock_replica_coverage",
                    "clock must cover exactly all replica_ids: " + "; ".join(parts),
                    i,
                )
            )
            bad = True

        # Rule 4a: per-replica seq unique.
        key = (ev.replica_id, ev.seq)
        if key in seen:
            errors.append(
                _err(
                    "duplicate_seq",
                    f"replica {ev.replica_id!r} already has an event with seq {ev.seq} "
                    f"(event index {seen_index[key]})",
                    i,
                )
            )
            bad = True
        else:
            seen[key] = ev
            seen_index[key] = i

    # Rule 4b: per-replica seq must be a contiguous 1..N range.
    valid_seqs: dict[str, set[int]] = {}
    for rid, seqs in seqs_by_replica.items():
        if not seqs:
            continue
        uniq = set(seqs)
        valid_seqs[rid] = uniq
        expected = set(range(1, max(uniq) + 1))
        if len(uniq) != len(seqs) or uniq != expected:
            gaps = sorted(expected - uniq)
            errors.append(
                _err(
                    "non_contiguous_seq",
                    f"seqs for replica {rid!r} must be unique and start at 1 with no "
                    f"gaps; got sorted seqs {sorted(seqs)} (missing {gaps})",
                )
            )

    if errors:
        raise BatchError(errors)

    # From here the structure is sound; replica/seq/clock lookups are safe.

    # Rule 5: the clock's own component must equal this event's seq.
    for i, ev in enumerate(events):
        if ev.clock[ev.replica_id] != ev.seq:
            raise BatchError(
                [
                    _err(
                        "clock_self_component",
                        f"clock[{ev.replica_id!r}] = {ev.clock[ev.replica_id]} "
                        f"must equal seq = {ev.seq}",
                        i,
                    )
                ]
            )

    # Rules 6 + 7: referenced history must exist and its clocks must be
    # strictly smaller (componentwise <= with at least one strict <).
    for i, ev in enumerate(events):
        referenced: list[Event] = []

        # The immediately previous event on this replica.
        if ev.seq > 1:
            referenced.append(seen[(ev.replica_id, ev.seq - 1)])

        # For every other replica, the latest event its component points at;
        # component 0 means "nothing known".
        for rid in replica_ids:
            if rid == ev.replica_id:
                continue
            n = ev.clock[rid]
            if n > 0:
                if n not in valid_seqs.get(rid, set()):
                    raise BatchError(
                        [
                            _err(
                                "clock_points_to_missing_event",
                                f"clock[{rid!r}] = {n}, but replica {rid!r} has no "
                                f"event with seq {n}",
                                i,
                            )
                        ]
                    )
                referenced.append(seen[(rid, n)])

        for ref in referenced:
            le, any_less = _clock_le(ref.clock, ev.clock, replica_ids)
            if not le or not any_less:
                ref_idx = seen_index[(ref.replica_id, ref.seq)]
                kind = "not <= current clock" if not le else "equal to current clock"
                raise BatchError(
                    [
                        _err(
                            "clock_causality",
                            f"referenced event (index {ref_idx}, replica "
                            f"{ref.replica_id!r}, seq {ref.seq}) clock {ref.clock} is "
                            f"{kind} {ev.clock}; it must be componentwise <= with at "
                            f"least one strictly smaller component",
                            i,
                        )
                    ]
                )

    return seen


def resolve(events: list[Event], replica_ids: list[str]) -> list[dict]:
    """Compute maximal events per key under the vector-clock partial order."""

    by_key: dict[str, list[Event]] = {}
    for ev in events:
        by_key.setdefault(ev.key, []).append(ev)

    results: list[dict] = []
    for key in sorted(by_key):
        group = by_key[key]

        # Maximal = no *other* event in the group is strictly greater.
        maximals: list[Event] = []
        for ev in group:
            dominated = False
            for other in group:
                if other is ev:
                    continue
                le, any_less = _clock_le(ev.clock, other.clock, replica_ids)
                if le and any_less:
                    dominated = True
                    break
            if not dominated:
                maximals.append(ev)

        maximals.sort(key=lambda e: (e.replica_id, e.seq))

        def _shape(ev: Event) -> dict:
            return {
                "replica_id": ev.replica_id,
                "seq": ev.seq,
                "op": "put" if ev.put is not None else "delete",
                "value": ev.put,
                "clock": {rid: ev.clock[rid] for rid in replica_ids},
            }

        if len(maximals) == 1:
            m = maximals[0]
            if m.put is not None:
                results.append(
                    {
                        "key": key,
                        "state": "put",
                        "value": m.put,
                        "event": _shape(m),
                    }
                )
            else:
                results.append({"key": key, "state": "deleted", "event": _shape(m)})
        else:
            results.append(
                {
                    "key": key,
                    "state": "conflict",
                    "events": [_shape(m) for m in maximals],
                }
            )

    return results


# ---------------------------------------------------------------------------
# Routes / error handling
# ---------------------------------------------------------------------------


@app.exception_handler(BatchError)
async def batch_error_handler(request: Request, exc: BatchError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors},
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Keep FastAPI's 422 shape, but drop the non-JSON-serializable 'ctx'
    # payload (it holds the original ValueError object).
    safe_errors: list[dict] = []
    for e in exc.errors():
        safe = {k: v for k, v in e.items() if k != "ctx"}
        safe_errors.append(safe)
    return JSONResponse(status_code=422, content={"detail": safe_errors})


@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/resolve", tags=["resolve"], response_model=None)
async def resolve_batch(body: ResolveRequest) -> dict:
    """Validate the batch and return the effective version of each key."""

    validate_batch(body.replica_ids, body.events)
    return {"results": resolve(body.events, body.replica_ids)}
