"""Client API-key authentication tests for the /v1/* endpoints.

These exercise the auth layer only; the Tasklet adapter and account pool are
replaced with a stub so no upstream is contacted.
"""

import os
import unittest
from typing import AsyncIterator
from unittest import mock

from fastapi.testclient import TestClient

from app import main
from app.adapters import AdapterRoute


class _StubAdapter:
    async def open_stream(self, normalized, route) -> AsyncIterator[str]:
        async def _gen() -> AsyncIterator[str]:
            yield "ok"

        return _gen()


class _StubConfig:
    def resolve_model(self, model: str) -> AdapterRoute:
        return AdapterRoute("tasklet", "stub", "agent", "workspace", "Asia/Singapore")


def _client(api_key: str | None) -> TestClient:
    env = {}
    if api_key is not None:
        env["FANDAI_API_KEY"] = api_key
    patcher = mock.patch.dict(os.environ, env, clear=False)
    patcher.start()
    if api_key is None:
        os.environ.pop("FANDAI_API_KEY", None)

    client = TestClient(main.app)
    client.__enter__()  # trigger lifespan
    # Replace the real config/adapters wired up by lifespan with stubs.
    main.app.state.config = _StubConfig()
    main.app.state.adapters = {"stub": _StubAdapter()}
    client._auth_patcher = patcher  # keep reference for teardown
    return client


CHAT_BODY = {"model": "tasklet", "messages": [{"role": "user", "content": "hi"}]}


class AuthTests(unittest.TestCase):
    def tearDown(self):
        # Best-effort cleanup of any lingering patcher.
        os.environ.pop("FANDAI_API_KEY", None)

    def _close(self, client: TestClient):
        client.__exit__(None, None, None)
        client._auth_patcher.stop()

    def test_missing_key_is_rejected(self):
        client = _client("secret-key")
        try:
            resp = client.post("/v1/chat/completions", json=CHAT_BODY)
            self.assertEqual(resp.status_code, 401)
        finally:
            self._close(client)

    def test_wrong_key_is_rejected(self):
        client = _client("secret-key")
        try:
            resp = client.post(
                "/v1/chat/completions",
                json=CHAT_BODY,
                headers={"Authorization": "Bearer wrong-key"},
            )
            self.assertEqual(resp.status_code, 401)
        finally:
            self._close(client)

    def test_correct_bearer_key_is_accepted(self):
        client = _client("secret-key")
        try:
            resp = client.post(
                "/v1/chat/completions",
                json=CHAT_BODY,
                headers={"Authorization": "Bearer secret-key"},
            )
            self.assertEqual(resp.status_code, 200)
        finally:
            self._close(client)

    def test_correct_x_api_key_is_accepted(self):
        client = _client("secret-key")
        try:
            for path, body in (
                ("/v1/chat/completions", CHAT_BODY),
                ("/v1/responses", {"model": "tasklet", "input": "hi"}),
                ("/v1/messages", {"model": "tasklet", "max_tokens": 16, "messages": [{"role": "user", "content": "hi"}]}),
            ):
                resp = client.post(path, json=body, headers={"x-api-key": "secret-key"})
                self.assertEqual(resp.status_code, 200, path)
        finally:
            self._close(client)

    def test_auth_disabled_when_key_unset(self):
        client = _client(None)
        try:
            resp = client.post("/v1/chat/completions", json=CHAT_BODY)
            self.assertEqual(resp.status_code, 200)
        finally:
            self._close(client)


if __name__ == "__main__":
    unittest.main()
