"""FastAPI 入口：多副本离线写入版本核对服务。"""
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .models import ResolveRequest, ResolveResponse
from .solver import SemanticValidationError, resolve

app = FastAPI(
    title="多副本离线写入版本核对 API",
    description=(
        "接收多个副本离线产生的 put/delete 事件（乱序提交），校验向量时钟一致性，"
        "并按向量时钟偏序归并出每个 key 的有效版本。请求数据不落库。"
    ),
    version="1.0.0",
)


@app.exception_handler(SemanticValidationError)
async def _semantic_validation_handler(_: Request, exc: SemanticValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": exc.errors})


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/resolve",
    response_model=ResolveResponse,
    tags=["resolve"],
    summary="校验整批事件并返回每个 key 的有效版本",
    responses={422: {"description": "请求结构或向量时钟语义校验失败，detail 指出违规事件"}},
)
async def resolve_versions(req: ResolveRequest) -> ResolveResponse:
    return ResolveResponse(results=resolve(req))
