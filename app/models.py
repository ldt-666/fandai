"""Protocol request models and normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .adapters.base import GatewayError, NormalizedRequest, NormalizedTurn


class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")
    role: str
    content: Any


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    model: str
    messages: list[ChatMessage]
    stream: bool = False
    max_tokens: int | None = None


class ResponsesRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    model: str
    input: Any
    instructions: str | None = None
    stream: bool = False
    max_output_tokens: int | None = None
    previous_response_id: str | None = None
    background: bool | None = None
    tools: list[Any] | None = None


class AnthropicMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")
    role: Literal["user", "assistant"]
    content: Any


class AnthropicMessagesRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    model: str
    messages: list[AnthropicMessage]
    system: Any = None
    stream: bool = False
    max_tokens: int = Field(default=1024, gt=0)
    tools: list[Any] | None = None
    thinking: dict[str, Any] | None = None


def _text_content(content: Any) -> str:
    if isinstance(content, str):
        if not content.strip():
            raise GatewayError("Message content must not be empty", status_code=400, code="invalid_request")
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if not isinstance(part, dict) or part.get("type") not in ("text", "input_text"):
                raise GatewayError("Only text content is supported", status_code=400, code="unsupported_content")
            text = part.get("text")
            if not isinstance(text, str):
                raise GatewayError("Text content part is missing text", status_code=400, code="invalid_request")
            parts.append(text)
        value = "".join(parts)
        if not value.strip():
            raise GatewayError("Message content must not be empty", status_code=400, code="invalid_request")
        return value
    raise GatewayError("Only string or text-part content is supported", status_code=400, code="unsupported_content")


def _instructions(value: Any) -> str | None:
    if value is None:
        return None
    return _text_content(value)


def normalize_chat(request: ChatCompletionRequest) -> NormalizedRequest:
    turns: list[NormalizedTurn] = []
    instructions: list[str] = []
    for message in request.messages:
        role = message.role
        text = _text_content(message.content)
        if role in ("system", "developer"):
            instructions.append(text)
        elif role in ("user", "assistant"):
            turns.append(NormalizedTurn(role, text))
        else:
            raise GatewayError(f"Unsupported chat message role: {role}", status_code=400, code="unsupported_role")
    if not any(turn.role == "user" for turn in turns):
        raise GatewayError("At least one user message is required", status_code=400, code="invalid_request")
    return NormalizedRequest(request.model, "\n\n".join(instructions) or None, tuple(turns), request.stream, request.max_tokens)


def normalize_responses(request: ResponsesRequest) -> NormalizedRequest:
    if request.previous_response_id or request.background or request.tools:
        raise GatewayError("Responses state, background mode, and tools are not supported", status_code=400, code="unsupported_feature")
    raw = request.input
    turns: list[NormalizedTurn] = []
    if isinstance(raw, str):
        turns.append(NormalizedTurn("user", _text_content(raw)))
    elif isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict) or item.get("type") not in (None, "message"):
                raise GatewayError("Responses input supports message text only", status_code=400, code="unsupported_content")
            role = item.get("role", "user")
            if role not in ("user", "assistant"):
                raise GatewayError("Responses input supports user and assistant messages only", status_code=400, code="unsupported_role")
            turns.append(NormalizedTurn(role, _text_content(item.get("content"))))
    else:
        raise GatewayError("Responses input must be a string or message array", status_code=400, code="invalid_request")
    return NormalizedRequest(request.model, _instructions(request.instructions), tuple(turns), request.stream, request.max_output_tokens)


def normalize_messages(request: AnthropicMessagesRequest) -> NormalizedRequest:
    turns = tuple(NormalizedTurn(message.role, _text_content(message.content)) for message in request.messages)
    if not any(turn.role == "user" for turn in turns):
        raise GatewayError("At least one user message is required", status_code=400, code="invalid_request")
    return NormalizedRequest(request.model, _instructions(request.system), turns, request.stream, request.max_tokens)
