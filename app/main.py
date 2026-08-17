"""FastAPI entrypoint and protocol routes for fandai."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .adapters import GatewayError, NormalizedRequest
from .auth import require_api_key
from .config import GatewayConfig, build_adapters, load_config
from .messages_adapter import message_body, messages_stream
from .models import (
    AnthropicMessagesRequest,
    ChatCompletionRequest,
    ResponsesRequest,
    normalize_chat,
    normalize_messages,
    normalize_responses,
)
from .openai_adapter import chat_response, chat_stream
from .responses_adapter import response_body, responses_stream

logger = logging.getLogger("fandai")
logging.basicConfig(
    level=os.getenv("FANDAI_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config()
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(60.0, connect=10.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    app.state.config = config
    app.state.http_client = client
    app.state.adapters = build_adapters(config, client)
    try:
        yield
    finally:
        await client.aclose()


app = FastAPI(
    title="fandai",
    version="0.2.0",
    description="OpenAI Chat/Responses and Anthropic Messages gateway for Tasklet AI.",
    lifespan=lifespan,
)


@app.exception_handler(GatewayError)
async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
    return _openai_error(exc)


def _openai_error(exc: GatewayError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"message": str(exc), "type": exc.code, "param": None, "code": exc.code}},
    )


def _anthropic_error(exc: GatewayError) -> JSONResponse:
    error_type = "invalid_request_error" if exc.status_code == 400 else "api_error"
    return JSONResponse(
        status_code=exc.status_code,
        content={"type": "error", "error": {"type": error_type, "message": str(exc)}},
    )


@app.get("/")
async def root() -> dict[str, str]:
    return {"name": "fandai", "version": "0.2.0", "status": "ok"}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz(request: Request) -> dict[str, str]:
    if not hasattr(request.app.state, "config") or not hasattr(request.app.state, "adapters"):
        raise HTTPException(status_code=503, detail="Gateway is not ready")
    return {"status": "ok"}


def _resolve(request: Request, model: str):
    config: GatewayConfig = request.app.state.config
    try:
        route = config.resolve_model(model)
    except KeyError as exc:
        raise GatewayError(f"Unknown model: {model}", status_code=404, code="model_not_found") from exc
    try:
        adapter = request.app.state.adapters[route.adapter]
    except KeyError as exc:
        raise GatewayError(f"Adapter is not available: {route.adapter}", status_code=500, code="adapter_unavailable") from exc
    return adapter, route


def _open_text_stream(request: Request, normalized: NormalizedRequest) -> AsyncIterator[str]:
    adapter, route = _resolve(request, normalized.model)

    async def generator() -> AsyncIterator[str]:
        upstream = await adapter.open_stream(normalized, route)
        try:
            async for fragment in upstream:
                yield fragment
        finally:
            await upstream.aclose()

    return generator()


async def _first_and_remainder(stream: AsyncIterator[str]):
    first = await anext(stream, None)
    return first, stream


async def _prepend(first: str | None, remainder: AsyncIterator[str]) -> AsyncIterator[str]:
    if first:
        yield first
    async for fragment in remainder:
        yield fragment


async def _collect(stream: AsyncIterator[str]) -> str:
    parts: list[str] = []
    async for fragment in stream:
        if fragment:
            parts.append(fragment)
    return "".join(parts)


@app.post("/v1/chat/completions", response_model=None, dependencies=[Depends(require_api_key)])
async def chat_completions(body: ChatCompletionRequest, request: Request):
    try:
        normalized = normalize_chat(body)
        stream = _open_text_stream(request, normalized)
        first, remainder = await _first_and_remainder(stream)
        if body.stream:
            return StreamingResponse(chat_stream(_prepend(first, remainder), model=body.model), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
        text = await _collect(_prepend(first, remainder))
        return chat_response(text, model=body.model)
    except GatewayError as exc:
        return _openai_error(exc)


@app.post("/v1/responses", response_model=None, dependencies=[Depends(require_api_key)])
async def responses(body: ResponsesRequest, request: Request):
    try:
        normalized = normalize_responses(body)
        stream = _open_text_stream(request, normalized)
        first, remainder = await _first_and_remainder(stream)
        if body.stream:
            return StreamingResponse(responses_stream(_prepend(first, remainder), model=body.model), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
        text = await _collect(_prepend(first, remainder))
        return response_body(text, model=body.model)
    except GatewayError as exc:
        return _openai_error(exc)


@app.post("/v1/messages", response_model=None, dependencies=[Depends(require_api_key)])
async def messages(body: AnthropicMessagesRequest, request: Request):
    try:
        normalized = normalize_messages(body)
        stream = _open_text_stream(request, normalized)
        first, remainder = await _first_and_remainder(stream)
        if body.stream:
            return StreamingResponse(messages_stream(_prepend(first, remainder), model=body.model), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
        text = await _collect(_prepend(first, remainder))
        return message_body(text, model=body.model)
    except GatewayError as exc:
        return _anthropic_error(exc)
