"""OpenAI Responses API output encoding."""

from __future__ import annotations

import json
import time
from typing import AsyncIterator

from .openai_adapter import make_id
from .adapters.base import GatewayError


def _event(payload: dict, *, event: str) -> bytes:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()


def _base(response_id: str, model: str, status: str = "in_progress") -> dict:
    return {"id": response_id, "object": "response", "created_at": int(time.time()), "model": model, "status": status, "output": [], "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}}


def response_body(text: str, *, model: str, response_id: str | None = None) -> dict:
    response_id = response_id or make_id("resp")
    body = _base(response_id, model, "completed")
    body["output"] = [{"id": make_id("msg"), "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": text, "annotations": []}]}]
    return body


async def responses_stream(text: AsyncIterator[str], *, model: str) -> AsyncIterator[bytes]:
    response_id = make_id("resp")
    item_id = make_id("msg")
    part_id = make_id("part")
    yield _event({"type": "response.created", "response": _base(response_id, model)}, event="response.created")
    yield _event({"type": "response.in_progress", "response": _base(response_id, model)}, event="response.in_progress")
    yield _event({"type": "response.output_item.added", "response_id": response_id, "output_index": 0, "item": {"id": item_id, "type": "message", "status": "in_progress", "role": "assistant", "content": []}}, event="response.output_item.added")
    yield _event({"type": "response.content_part.added", "response_id": response_id, "item_id": item_id, "output_index": 0, "content_index": 0, "part": {"id": part_id, "type": "output_text", "text": "", "annotations": []}}, event="response.content_part.added")
    try:
        async for fragment in text:
            if fragment:
                yield _event({"type": "response.output_text.delta", "response_id": response_id, "item_id": item_id, "output_index": 0, "content_index": 0, "delta": fragment}, event="response.output_text.delta")
    except GatewayError as exc:
        yield _event({"type": "error", "error": {"message": str(exc), "type": "upstream_error", "code": exc.code}}, event="error")
        yield _event({"type": "response.failed", "response": {**_base(response_id, model, "failed"), "error": {"message": str(exc), "type": "upstream_error", "code": exc.code}}}, event="response.failed")
        return
    yield _event({"type": "response.output_text.done", "response_id": response_id, "item_id": item_id, "output_index": 0, "content_index": 0, "text": ""}, event="response.output_text.done")
    yield _event({"type": "response.content_part.done", "response_id": response_id, "item_id": item_id, "output_index": 0, "content_index": 0, "part": {"id": part_id, "type": "output_text", "text": "", "annotations": []}}, event="response.content_part.done")
    yield _event({"type": "response.output_item.done", "response_id": response_id, "output_index": 0, "item": {"id": item_id, "type": "message", "status": "completed", "role": "assistant", "content": []}}, event="response.output_item.done")
    yield _event({"type": "response.completed", "response": response_body("", model=model, response_id=response_id)}, event="response.completed")
