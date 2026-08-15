"""Common adapter contracts and provider-neutral request types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import AsyncIterator, Literal

import httpx


class GatewayError(Exception):
    """Sanitized failure used across adapters and API surfaces."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 502,
        code: str = "upstream_error",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


@dataclass(frozen=True)
class NormalizedTurn:
    role: Literal["user", "assistant"]
    text: str


@dataclass(frozen=True)
class NormalizedRequest:
    model: str
    instructions: str | None
    turns: tuple[NormalizedTurn, ...]
    stream: bool
    max_output_tokens: int | None = None


@dataclass(frozen=True)
class AdapterRoute:
    name: str
    adapter: str
    agent_id: str
    workspace_id: str
    timezone: str
    aliases: tuple[str, ...] = ()


class AdapterStream:
    """An upstream response whose HTTP status is already validated."""

    def __init__(
        self,
        response: httpx.Response,
        iterator: AsyncIterator[str],
    ) -> None:
        self._response = response
        self._iterator = iterator

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iterator

    async def aclose(self) -> None:
        await self._response.aclose()


class Adapter(ABC):
    @abstractmethod
    async def open_stream(
        self,
        request: NormalizedRequest,
        route: AdapterRoute,
    ) -> AdapterStream:
        """Open and validate the upstream request before returning its stream."""
