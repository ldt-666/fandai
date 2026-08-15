"""OpenAI Chat Completions output encoding."""

from __future__ import annotations

import json
import time
import uuid
from typing import AsyncIterator

from .adapters.base import GatewayError


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


def _event(payload: dict, *, event: str | None = None) -> bytes:
    prefix = f"event: {event}\n" if event else ""
    return f"{prefix}data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()


async def chat_stream(text: AsyncIterator[str], *, model: str) -> AsyncIterator[bytes]:
    completion_id = make_id("chatcmpl")
    created = int(time.time())
    yield _event({"id": completion_id, "object": "chat.completion.chunk", "created": created, "model": model, "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}}]})
    try:
        async for fragment in text:
            if fragment:
                yield _event({"id": completion_id, "object": "chat.completion.chunk", "created": created, "model": model, "choices": [{"index": 0, "delta": {"content": fragment}}]})
    except GatewayError as exc:
        yield _event({"error": {"message": str(exc), "type": "upstream_error", "code": exc.code}})
        yield b"data: [DONE]\n\n"
        return
    yield _event({"id": completion_id, "object": "chat.completion.chunk", "created": created, "model": model, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
    yield b"data: [DONE]\n\n"


def chat_response(text: str, *, model: str) -> dict:
    return {"id": make_id("chatcmpl"), "object": "chat.completion", "created": int(time.time()), "model": model, "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}
