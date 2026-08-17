"""Common adapter contracts and provider-neutral request types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
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
        retryable_before_acceptance: bool = False,
        account_failure: bool = False,
        acceptance_unknown: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.retryable_before_acceptance = retryable_before_acceptance
        self.account_failure = account_failure
        self.acceptance_unknown = acceptance_unknown


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
        response: httpx.Response | None,
        iterator: AsyncIterator[str],
        on_close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._response = response
        self._iterator = iterator
        self._on_close = on_close
        self._closed = False

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iterator

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            close_iterator = getattr(self._iterator, "aclose", None)
            if close_iterator is not None:
                await close_iterator()
        finally:
            try:
                if self._response is not None:
                    await self._response.aclose()
            finally:
                if self._on_close is not None:
                    await self._on_close()


class Adapter(ABC):
    @abstractmethod
    async def open_stream(
        self,
        request: NormalizedRequest,
        route: AdapterRoute,
    ) -> AdapterStream:
        """Open and validate the upstream request before returning its stream."""
