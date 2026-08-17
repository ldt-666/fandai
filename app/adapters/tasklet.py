"""Tasklet AI adapter.

Tasklet accepts a message over HTTP, then publishes agent state and blocks over
an authenticated WebSocket. This module translates that provider protocol into
the gateway's provider-neutral text stream.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator
from urllib.parse import urlsplit

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .base import Adapter, AdapterRoute, AdapterStream, GatewayError, NormalizedRequest

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SyncBaseline:
    content: dict[str, str]
    block_ids: frozenset[str]
    run_type: str | None
    queued_messages: Any


class TaskletAdapter(Adapter):
    def __init__(self, client: httpx.AsyncClient, *, api_url: str, token: str) -> None:
        self._client = client
        self._api_url = api_url
        self._token = token
        self._sync_url = _sync_url(api_url)

    async def open_stream(
        self,
        request: NormalizedRequest,
        route: AdapterRoute,
    ) -> AdapterStream:
        payload = {
            "agentId": route.agent_id,
            "message": compile_tasklet_message(request),
            "timezone": route.timezone,
            "fileIds": [],
            "workspaceId": route.workspace_id,
        }
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        try:
            websocket = await connect(
                self._sync_url,
                additional_headers={"Authorization": f"Bearer {self._token}"},
                open_timeout=15,
            )
        except (OSError, WebSocketException, asyncio.TimeoutError) as exc:
            raise GatewayError(
                f"Network error connecting to Tasklet sync: {exc}",
                code="network_error",
                retryable_before_acceptance=True,
                account_failure=True,
            ) from exc

        try:
            await _send_json(websocket, {"type": "connect", "sessionToken": self._token})
            connected = await _recv_json(websocket, timeout=20)
            if connected.get("type") != "connected":
                raise _sync_error(connected)
            await _send_json(websocket, {"type": "startSync", "agentId": route.agent_id})
            await _send_json(
                websocket,
                {"type": "subscribeBlocks", "runId": route.agent_id, "pageSize": 500},
            )
            baseline = await _wait_for_initial_state(websocket, route.agent_id)
        except (OSError, WebSocketException, asyncio.TimeoutError) as exc:
            await websocket.close()
            raise GatewayError(
                f"Tasklet sync request failed: {exc}",
                code="network_error",
                retryable_before_acceptance=True,
                account_failure=True,
            ) from exc
        except GatewayError as exc:
            await websocket.close()
            raise _mark_before_acceptance(exc) from exc

        try:
            response = await self._client.post(self._api_url, headers=headers, json=payload)
        except (httpx.TimeoutException, httpx.HTTPError) as exc:
            await websocket.close()
            raise GatewayError(
                f"Network error sending message to Tasklet: {exc}",
                code="network_error",
                account_failure=True,
                acceptance_unknown=True,
            ) from exc

        if response.status_code >= 400:
            body = (response.text or "")[:500]
            await response.aclose()
            await websocket.close()
            if response.status_code in (401, 403):
                raise GatewayError(
                    "Tasklet authentication failed; check the configured token",
                    status_code=response.status_code,
                    code="authentication_error",
                    retryable_before_acceptance=True,
                    account_failure=True,
                )
            retryable = response.status_code == 429 or response.status_code >= 500
            raise GatewayError(
                f"Tasklet returned HTTP {response.status_code}: {body.strip()}"
                if body.strip()
                else f"Tasklet returned HTTP {response.status_code}",
                status_code=response.status_code,
                code="upstream_error",
                retryable_before_acceptance=retryable,
                account_failure=retryable,
            )

        try:
            result = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            await response.aclose()
            await websocket.close()
            raise GatewayError(
                "Tasklet returned malformed trigger JSON",
                code="malformed_upstream",
                account_failure=True,
                acceptance_unknown=True,
            ) from exc
        await response.aclose()
        if not isinstance(result, dict) or result.get("agentId") != route.agent_id:
            await websocket.close()
            raise GatewayError(
                "Tasklet returned an unexpected agent id",
                code="malformed_upstream",
                account_failure=True,
                acceptance_unknown=True,
            )

        accepted_at = time.time_ns() // 1_000_000
        logger.debug(
            "Tasklet accepted message: agent_id=%s accepted_at=%d",
            route.agent_id,
            accepted_at,
        )
        return AdapterStream(
            None,
            self._iter_sync(websocket, route.agent_id, baseline),
            websocket.close,
        )

    async def _iter_sync(
        self,
        websocket: ClientConnection,
        agent_id: str,
        baseline: _SyncBaseline,
    ) -> AsyncIterator[str]:
        previous_content: dict[str, str] = dict(baseline.content)
        lifecycle_confirmed = False
        terminal_seen = False
        saw_agent_content = False
        current_run_type = baseline.run_type
        current_queued_messages = baseline.queued_messages
        try:
            while True:
                message = await _recv_json(websocket, timeout=120)
                message_type = message.get("type")
                if message_type == "blocksUpdate" and message.get("runId") == agent_id:
                    for block in _changed_blocks(message):
                        block_id = str(block.get("blockId", ""))
                        is_current = (
                            bool(block_id)
                            and block_id not in baseline.block_ids
                            and _block_is_current(block)
                        )
                        if is_current and block.get("type") in ("user_message", "agent_content"):
                            lifecycle_confirmed = True
                        if block.get("type") != "agent_content" or not lifecycle_confirmed or not is_current:
                            continue
                        content = block.get("content")
                        if not isinstance(content, str):
                            continue
                        old = previous_content.get(block_id, "")
                        previous_content[block_id] = content
                        if content.startswith(old):
                            fragment = content[len(old):]
                        else:
                            fragment = content
                        if fragment:
                            saw_agent_content = True
                            yield fragment
                    if terminal_seen and saw_agent_content:
                        return
                elif message_type == "syncUpdate" and message.get("agentId") == agent_id:
                    state = message.get("state")
                    if not isinstance(state, dict):
                        continue
                    run_state = state.get("runState")
                    if not isinstance(run_state, dict):
                        continue
                    run_type = run_state.get("type")
                    queued_messages = state.get("queuedMessages")
                    run_changed = _run_confirms_lifecycle(baseline.run_type, run_type)
                    queued_changed = queued_messages != current_queued_messages
                    if run_changed or queued_changed:
                        lifecycle_confirmed = True
                    current_run_type = run_type if isinstance(run_type, str) else current_run_type
                    current_queued_messages = queued_messages
                    if not lifecycle_confirmed:
                        continue
                    if run_type == "error":
                        raise GatewayError(
                            str(run_state.get("error") or "Tasklet agent failed"),
                            code=str(run_state.get("errorCode") or "upstream_error"),
                        )
                    if run_type in ("idle", "ready"):
                        terminal_seen = True
                        if saw_agent_content:
                            return
                elif message_type == "error":
                    raise _sync_error(message)
        except asyncio.TimeoutError as exc:
            raise GatewayError("Timed out while waiting for Tasklet response", status_code=504, code="timeout") from exc
        except (OSError, WebSocketException) as exc:
            raise GatewayError(f"Tasklet sync stream was interrupted: {exc}", code="network_error") from exc
        finally:
            await websocket.close()


def _sync_url(api_url: str) -> str:
    parsed = urlsplit(api_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Tasklet api_url must be an absolute HTTP(S) URL")
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}/api/sync"


async def _send_json(websocket: ClientConnection, payload: dict[str, Any]) -> None:
    await websocket.send(json.dumps(payload, ensure_ascii=False))


async def _recv_json(websocket: ClientConnection, *, timeout: float) -> dict[str, Any]:
    raw = await asyncio.wait_for(websocket.recv(), timeout)
    if not isinstance(raw, str):
        raise GatewayError("Tasklet returned a binary sync message", code="malformed_upstream")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise GatewayError("Tasklet returned malformed sync JSON", code="malformed_upstream") from exc
    if not isinstance(value, dict):
        raise GatewayError("Tasklet returned an invalid sync message", code="malformed_upstream")
    return value


async def _wait_for_initial_state(
    websocket: ClientConnection, agent_id: str
) -> _SyncBaseline:
    content: dict[str, str] = {}
    block_ids: set[str] = set()
    run_type: str | None = None
    queued_messages: Any = None
    saw_blocks = False
    saw_sync = False
    for _ in range(20):
        message = await _recv_json(websocket, timeout=20)
        message_type = message.get("type")
        if message_type == "blocksUpdate" and message.get("runId") == agent_id:
            saw_blocks = True
            for block in _changed_blocks(message):
                block_id = str(block.get("blockId", ""))
                if not block_id:
                    continue
                block_ids.add(block_id)
                value = block.get("content")
                content[block_id] = value if isinstance(value, str) else ""
        elif message_type == "syncUpdate" and message.get("agentId") == agent_id:
            state = message.get("state")
            if isinstance(state, dict):
                run_state = state.get("runState")
                if isinstance(run_state, dict):
                    run_type = run_state.get("type") if isinstance(run_state.get("type"), str) else None
                queued_messages = state.get("queuedMessages")
            saw_sync = True
        elif message_type == "error":
            raise _sync_error(message)
        if saw_blocks and saw_sync:
            return _SyncBaseline(content, frozenset(block_ids), run_type, queued_messages)
    raise GatewayError("Tasklet did not provide an initial sync snapshot", code="malformed_upstream")


def _run_confirms_lifecycle(baseline_type: str | None, current_type: Any) -> bool:
    if not isinstance(current_type, str) or current_type == baseline_type:
        return False
    if current_type == "running":
        return True
    return baseline_type in ("idle", "ready") and current_type in ("idle", "ready")


def _changed_blocks(message: dict[str, Any]) -> list[dict[str, Any]]:
    blocks = message.get("blocks")
    updates = message.get("updates")
    values: list[dict[str, Any]] = []
    if isinstance(blocks, list):
        values.extend(block for block in blocks if isinstance(block, dict))
    if isinstance(updates, dict):
        values.extend(block for block in updates.values() if isinstance(block, dict))
    return values


def _block_is_current(block: dict[str, Any]) -> bool:
    return bool(block.get("blockId"))


def _mark_before_acceptance(exc: GatewayError) -> GatewayError:
    return GatewayError(
        str(exc),
        status_code=exc.status_code,
        code=exc.code,
        retryable_before_acceptance=True,
        account_failure=True,
    )


def _sync_error(message: dict[str, Any]) -> GatewayError:
    code = str(message.get("code") or "upstream_error")
    return GatewayError(
        str(message.get("error") or message.get("message") or "Tasklet sync failed"),
        code=code,
    )


def compile_tasklet_message(request: NormalizedRequest) -> str:
    """Compile canonical turns without dropping system/history context."""
    if request.instructions is None and len(request.turns) == 1 and request.turns[0].role == "user":
        return request.turns[0].text
    lines: list[str] = []
    if request.instructions:
        lines.append(f"[system]\n{request.instructions}")
    for turn in request.turns:
        lines.append(f"[{turn.role}]\n{turn.text}")
    return "\n\n".join(lines)


def parse_tasklet_event(data_lines: list[str]) -> str | None:
    """Parse the legacy provider SSE shape for compatibility with callers."""
    if not data_lines:
        return None
    raw = "\n".join(data_lines).strip()
    if not raw or raw == "[DONE]":
        return None
    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise GatewayError("Tasklet returned malformed SSE JSON", code="malformed_upstream") from exc
    if not isinstance(payload, dict):
        return None
    if payload.get("error"):
        error = payload["error"]
        message = error.get("message") if isinstance(error, dict) else str(error)
        raise GatewayError(message or "Tasklet stream reported an error", code="upstream_error")
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return None
    delta = choices[0].get("delta")
    if not isinstance(delta, dict):
        return None
    content = delta.get("content")
    return content if isinstance(content, str) else None
