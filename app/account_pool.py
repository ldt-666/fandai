"""In-process round-robin pool for complete Tasklet account bindings."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import AsyncIterator, Any

import httpx

from .adapters import Adapter, AdapterRoute, AdapterStream, GatewayError, NormalizedRequest, TaskletAdapter

DEFAULT_TASKLET_API_URL = "https://api.tasklet.ai/api/sendChatMessage"


@dataclass(frozen=True)
class AccountConfig:
    name: str
    token: str
    agent_id: str
    workspace_id: str
    timezone: str = "Asia/Singapore"
    api_url: str = DEFAULT_TASKLET_API_URL


_ACCOUNT_KEYS = {"name", "api_url", "token", "agent_id", "workspace_id", "timezone"}
_REQUIRED_ACCOUNT_KEYS = ("name", "token", "agent_id", "workspace_id")


def parse_accounts(raw_accounts: Any) -> tuple[AccountConfig, ...]:
    """Validate and convert a list of account mappings into AccountConfig.

    Shared by local ``config.yaml`` parsing and remote JSON loading so both
    sources enforce identical rules and error messages.
    """
    if not isinstance(raw_accounts, list) or not raw_accounts:
        raise ValueError("accounts configuration must be a non-empty list")
    accounts: list[AccountConfig] = []
    names: set[str] = set()
    for index, raw in enumerate(raw_accounts):
        label = f"account {index + 1}"
        if not isinstance(raw, dict):
            raise ValueError(f"{label} must be a mapping")
        unknown = set(raw) - _ACCOUNT_KEYS
        if unknown:
            raise ValueError(f"Unknown {label} keys: {', '.join(sorted(unknown))}")
        missing = [key for key in _REQUIRED_ACCOUNT_KEYS if not raw.get(key)]
        if missing:
            raise ValueError(f"Missing required {label} keys: {', '.join(missing)}")
        name = str(raw["name"])
        if name in names:
            raise ValueError(f"Duplicate account name: {name}")
        names.add(name)
        accounts.append(
            AccountConfig(
                name=name,
                api_url=str(raw.get("api_url") or DEFAULT_TASKLET_API_URL),
                token=str(raw["token"]),
                agent_id=str(raw["agent_id"]),
                workspace_id=str(raw["workspace_id"]),
                timezone=str(raw.get("timezone") or "Asia/Singapore"),
            )
        )
    return tuple(accounts)


@dataclass
class _AccountSlot:
    config: AccountConfig
    adapter: Adapter
    route: AdapterRoute
    busy: bool = False
    disabled: bool = False
    failure_count: int = 0
    available_at: float = 0.0
    last_error_code: str | None = None


class _ManagedIterator:
    def __init__(self, upstream: AdapterStream) -> None:
        self._upstream = upstream
        self._iterator = upstream.__aiter__()
        self.completed = False
        self.error: GatewayError | None = None
        self.unexpected_error = False

    def __aiter__(self) -> AsyncIterator[str]:
        return self

    async def __anext__(self) -> str:
        try:
            return await anext(self._iterator)
        except StopAsyncIteration:
            self.completed = True
            raise
        except GatewayError as exc:
            self.error = exc
            raise
        except asyncio.CancelledError:
            raise
        except Exception:
            self.unexpected_error = True
            raise

    async def aclose(self) -> None:
        await self._upstream.aclose()


class AccountManager(Adapter):
    """Select, lease, and monitor Tasklet accounts for complete requests."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        accounts: tuple[AccountConfig, ...],
        *,
        adapter_factory: Callable[..., Adapter] = TaskletAdapter,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not accounts:
            raise ValueError("AccountManager requires at least one account")
        self._condition = asyncio.Condition()
        self._clock = clock
        self._cursor = 0
        self._client = client
        self._adapter_factory = adapter_factory
        self._slots = [self._build_slot(account) for account in accounts]

    def _build_slot(self, account: AccountConfig) -> _AccountSlot:
        return _AccountSlot(
            config=account,
            adapter=self._adapter_factory(
                self._client, api_url=account.api_url, token=account.token
            ),
            route=AdapterRoute(
                name=account.name,
                adapter="accounts",
                agent_id=account.agent_id,
                workspace_id=account.workspace_id,
                timezone=account.timezone,
            ),
        )

    async def reload_accounts(self, accounts: tuple[AccountConfig, ...]) -> None:
        """Replace the account roster, preserving runtime state for unchanged
        accounts (matched by full config). New or changed accounts get a fresh
        slot; removed accounts are dropped. In-flight leases keep their original
        slot object, so a reload never disrupts a running request."""
        if not accounts:
            raise ValueError("reload requires at least one account")
        async with self._condition:
            existing = {slot.config: slot for slot in self._slots}
            self._slots = [
                existing[account] if account in existing else self._build_slot(account)
                for account in accounts
            ]
            self._cursor = 0
            self._condition.notify_all()

    async def open_stream(
        self,
        request: NormalizedRequest,
        route: AdapterRoute,
    ) -> AdapterStream:
        del route
        excluded: set[int] = set()
        last_error: GatewayError | None = None

        while True:
            try:
                index, slot = await self._acquire(excluded)
            except GatewayError:
                if last_error is not None:
                    raise last_error
                raise

            try:
                upstream = await slot.adapter.open_stream(request, slot.route)
            except asyncio.CancelledError:
                await self._finish(slot)
                raise
            except GatewayError as exc:
                await self._finish(slot, error=exc)
                excluded.add(index)
                last_error = exc
                if exc.retryable_before_acceptance:
                    continue
                raise
            except Exception:
                await self._finish(slot, unexpected_error=True)
                raise

            managed = _ManagedIterator(upstream)

            async def close_lease(
                selected: _AccountSlot = slot,
                iterator: _ManagedIterator = managed,
            ) -> None:
                await self._finish(
                    selected,
                    completed=iterator.completed,
                    error=iterator.error,
                    unexpected_error=iterator.unexpected_error,
                    stream_failure=iterator.error is not None or iterator.unexpected_error,
                )

            return AdapterStream(None, managed, close_lease)

    async def _acquire(self, excluded: set[int]) -> tuple[int, _AccountSlot]:
        async with self._condition:
            while True:
                now = self._clock()
                healthy_indexes = [
                    index
                    for index, slot in enumerate(self._slots)
                    if index not in excluded
                    and not slot.disabled
                    and slot.available_at <= now
                ]
                if not healthy_indexes:
                    raise GatewayError(
                        "No Tasklet accounts are currently available",
                        status_code=503,
                        code="accounts_unavailable",
                    )

                for offset in range(len(self._slots)):
                    index = (self._cursor + offset) % len(self._slots)
                    if index not in healthy_indexes:
                        continue
                    slot = self._slots[index]
                    if slot.busy:
                        continue
                    slot.busy = True
                    self._cursor = (index + 1) % len(self._slots)
                    return index, slot

                await self._condition.wait()

    async def _finish(
        self,
        slot: _AccountSlot,
        *,
        completed: bool = False,
        error: GatewayError | None = None,
        unexpected_error: bool = False,
        stream_failure: bool = False,
    ) -> None:
        async with self._condition:
            if error is not None:
                self._record_failure(
                    slot,
                    error.code,
                    error.account_failure or error.acceptance_unknown,
                    stream_failure=stream_failure,
                )
            elif unexpected_error:
                self._record_failure(
                    slot,
                    "upstream_error",
                    True,
                    stream_failure=stream_failure,
                )
            elif completed:
                slot.failure_count = 0
                slot.available_at = 0.0
                slot.last_error_code = None
            slot.busy = False
            self._condition.notify_all()

    def _record_failure(
        self,
        slot: _AccountSlot,
        code: str,
        account_failure: bool,
        *,
        stream_failure: bool,
    ) -> None:
        if not account_failure and not stream_failure:
            return
        slot.failure_count += 1
        slot.last_error_code = code
        if code in _PERMANENT_FAILURES or code.endswith("credits_exhausted"):
            slot.disabled = True
            slot.available_at = 0.0
            return
        delay = min(30.0 * (2 ** (slot.failure_count - 1)), 300.0)
        slot.available_at = self._clock() + delay

    def snapshot(self) -> list[dict[str, Any]]:
        now = self._clock()
        values: list[dict[str, Any]] = []
        for slot in self._slots:
            if slot.disabled:
                status = "disabled"
            elif slot.busy:
                status = "busy"
            elif slot.available_at > now:
                status = "cooldown"
            else:
                status = "available"
            values.append(
                {
                    "name": slot.config.name,
                    "status": status,
                    "last_error_code": slot.last_error_code,
                    "failure_count": slot.failure_count,
                }
            )
        return values


_PERMANENT_FAILURES = {"authentication_error", "agent/credits_exhausted"}
