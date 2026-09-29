"""HTTP API 的请求 / 响应模型（pydantic v2）。"""
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, model_validator

# 副本 ID 与 key 都要求是非空字符串
_NonEmpty = Annotated[str, Field(min_length=1)]
_NonNegInt = Annotated[int, Field(ge=0)]

MIN_REPLICAS = 2
MAX_REPLICAS = 8
MIN_EVENTS = 1
MAX_EVENTS = 120


class EventIn(BaseModel):
    """一条离线写入事件。"""

    replica_id: _NonEmpty
    seq: int = Field(ge=1, description="该副本内从 1 起的序列号")
    op: Literal["put", "delete"]
    key: _NonEmpty
    value: Optional[str] = Field(default=None, description="op=put 时必须给出字符串")
    clock: list[_NonNegInt] = Field(
        description="向量时钟，长度必须等于 replica_ids 的长度，各分量为非负整数"
    )

    @model_validator(mode="after")
    def _check_value_and_op(self) -> "EventIn":
        if self.op == "put" and self.value is None:
            raise ValueError("op=put 时必须提供字符串 value")
        if self.op == "delete" and self.value is not None:
            raise ValueError("op=delete 时不得提供 value")
        return self


class ResolveRequest(BaseModel):
    """整批校验 + 归并请求体。"""

    replica_ids: list[_NonEmpty] = Field(
        min_length=MIN_REPLICAS,
        max_length=MAX_REPLICAS,
        description="2~8 个非空且唯一的副本 ID",
    )
    events: list[EventIn] = Field(
        min_length=MIN_EVENTS,
        max_length=MAX_EVENTS,
        description="1~120 条事件，顺序可以任意（乱序提交）",
    )

    @model_validator(mode="after")
    def _check_request_shape(self) -> "ResolveRequest":
        if len(set(self.replica_ids)) != len(self.replica_ids):
            raise ValueError("replica_ids 必须唯一，不能有重复的副本 ID")

        replica_count = len(self.replica_ids)
        known = set(self.replica_ids)
        for i, event in enumerate(self.events):
            if event.replica_id not in known:
                raise ValueError(
                    f"events[{i}].replica_id={event.replica_id!r} 不在 replica_ids 中"
                )
            if len(event.clock) != replica_count:
                raise ValueError(
                    f"events[{i}] 的 clock 长度为 {len(event.clock)}，"
                    f"必须等于副本数 {replica_count}"
                )
        return self


class EventOut(BaseModel):
    replica_id: str
    seq: int
    clock: list[int]
    value: Optional[str] = None


class KeyResolution(BaseModel):
    key: str
    status: Literal["present", "deleted", "conflict"]
    value: Optional[str] = Field(
        default=None, description="status=present 时为存活值；deleted/conflict 时为 null"
    )
    events: list[EventOut] = Field(
        description="成为极大值的事件；present/deleted 恰有 1 条，conflict 至少 2 条"
    )


class ResolveResponse(BaseModel):
    results: list[KeyResolution]
