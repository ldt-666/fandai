"""Remote account-pool source: fetch and refresh accounts from a URL.

Loads the same account structure the local ``config.yaml`` ``accounts:`` block
produces, but from a remote JSON document of the shape::

    {"accounts": [{"name": ..., "token": ..., "agent_id": ..., "workspace_id": ...}]}

The first fetch happens at startup and must succeed (otherwise startup fails).
A background task then refreshes on an interval; refresh failures are logged and
the previously loaded accounts keep serving.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import httpx

from .account_pool import AccountConfig, AccountManager, parse_accounts

logger = logging.getLogger(__name__)

DEFAULT_REFRESH_INTERVAL = 600


@dataclass(frozen=True)
class AccountsSource:
    url: str
    refresh_interval: int = DEFAULT_REFRESH_INTERVAL


def parse_accounts_source(raw: Any) -> AccountsSource:
    if not isinstance(raw, dict):
        raise ValueError("accounts_source must be a mapping")
    unknown = set(raw) - {"type", "url", "refresh_interval"}
    if unknown:
        raise ValueError(f"Unknown accounts_source keys: {', '.join(sorted(unknown))}")
    if raw.get("type") != "url":
        raise ValueError("accounts_source.type must be 'url'")
    url = raw.get("url")
    if not url or not isinstance(url, str):
        raise ValueError("accounts_source.url is required")
    interval_raw = raw.get("refresh_interval", DEFAULT_REFRESH_INTERVAL)
    try:
        interval = int(interval_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("accounts_source.refresh_interval must be an integer") from exc
    if interval <= 0:
        raise ValueError("accounts_source.refresh_interval must be positive")
    return AccountsSource(url=str(url), refresh_interval=interval)


def accounts_from_document(document: Any) -> tuple[AccountConfig, ...]:
    """Convert a fetched JSON document into AccountConfig entries."""
    if not isinstance(document, dict) or "accounts" not in document:
        raise ValueError("remote accounts document must contain an 'accounts' list")
    return parse_accounts(document["accounts"])


async def fetch_accounts(client: httpx.AsyncClient, url: str) -> tuple[AccountConfig, ...]:
    response = await client.get(url)
    response.raise_for_status()
    return accounts_from_document(response.json())


class RemoteAccountsLoader:
    """Fetch accounts once at startup, then refresh a manager on an interval."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        source: AccountsSource,
        *,
        adapter_factory=None,
    ) -> None:
        self._client = client
        self._source = source
        self._adapter_factory = adapter_factory
        self._manager: AccountManager | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> AccountManager:
        """Perform the initial (mandatory) fetch and build the manager.

        Raises if the first fetch fails — startup must not proceed without
        accounts.
        """
        accounts = await fetch_accounts(self._client, self._source.url)
        kwargs = {}
        if self._adapter_factory is not None:
            kwargs["adapter_factory"] = self._adapter_factory
        self._manager = AccountManager(self._client, accounts, **kwargs)
        logger.info(
            "Loaded %d account(s) from %s", len(accounts), self._source.url
        )
        self._task = asyncio.create_task(self._refresh_loop())
        return self._manager

    @property
    def manager(self) -> AccountManager | None:
        return self._manager

    async def _refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._source.refresh_interval)
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive; refresh_once handles most
                logger.exception("Unexpected error in account refresh loop")

    async def refresh_once(self) -> bool:
        """Fetch and apply accounts. On failure keep the current roster.

        Returns True if the roster was refreshed, False if the fetch failed.
        """
        assert self._manager is not None
        try:
            accounts = await fetch_accounts(self._client, self._source.url)
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning(
                "Account refresh from %s failed; keeping cached accounts: %s",
                self._source.url,
                exc,
            )
            return False
        await self._manager.reload_accounts(accounts)
        logger.info("Refreshed %d account(s) from %s", len(accounts), self._source.url)
        return True

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
