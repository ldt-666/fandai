"""Anthropic Messages API output encoding."""

from __future__ import annotations

import json
import time
from typing import AsyncIterator

from .adapters.base import GatewayError
from .openai_adapter import make_id


def _event(name: str, payload: dict) -> bytes:
    return f"event: {name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()


def message_body(text: str, *, model: str) -> dict:
    return {"id": make_id("msg"), "type": "message", "role": "assistant", "model": model, "content": [{"type": "text", "text": text}], "stop_reason": "end_turn", "stop_sequence": None, "usage": {"input_tokens": 0, "output_tokens": 0}}


async def messages_stream(text: AsyncIterator[str], *, model: str) -> AsyncIterator[bytes]:
    message_id = make_id("msg")
    yield _event("message_start", {"type": "message_start", "message": {"id": message_id, "type": "message", "role": "assistant", "model": model, "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 0, "output_tokens": 0}}})
    yield _event("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})
    try:
        async for fragment in text:
            if fragment:
                yield _event("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": fragment}})
    except GatewayError as exc:
        yield _event("error", {"type": "error", "error": {"type": "api_error", "message": str(exc)}})
        return
    yield _event("content_block_stop", {"type": "content_block_stop", "index": 0})
    yield _event("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 0}})
    yield _event("message_stop", {"type": "message_stop"})
