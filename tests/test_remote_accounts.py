"""Tests for remote account-pool loading and refresh."""

import unittest

import httpx

from app.account_pool import AccountConfig
from app.adapters.base import AdapterStream
from app.remote_accounts import (
    AccountsSource,
    RemoteAccountsLoader,
    accounts_from_document,
    parse_accounts_source,
)
from app.config import load_config

import tempfile
from pathlib import Path


DOC = {
    "accounts": [
        {"name": "acc1", "token": "t1", "agent_id": "a1", "workspace_id": "w1"},
        {
            "name": "acc2",
            "token": "t2",
            "agent_id": "a2",
            "workspace_id": "w2",
            "timezone": "Europe/London",
        },
    ]
}


def _factory(client, *, api_url, token):
    del client, api_url

    class _Stub:
        async def open_stream(self, request, route):
            async def gen():
                yield "ok"

            return AdapterStream(None, gen())

    return _Stub()


class ParseSourceTests(unittest.TestCase):
    def test_valid_source(self):
        source = parse_accounts_source(
            {"type": "url", "url": "https://x/y.json", "refresh_interval": 120}
        )
        self.assertEqual(source.url, "https://x/y.json")
        self.assertEqual(source.refresh_interval, 120)

    def test_default_interval(self):
        source = parse_accounts_source({"type": "url", "url": "https://x/y.json"})
        self.assertEqual(source.refresh_interval, 600)

    def test_rejects_wrong_type(self):
        with self.assertRaisesRegex(ValueError, "type must be 'url'"):
            parse_accounts_source({"type": "file", "url": "x"})

    def test_requires_url(self):
        with self.assertRaisesRegex(ValueError, "url is required"):
            parse_accounts_source({"type": "url"})

    def test_rejects_non_positive_interval(self):
        with self.assertRaisesRegex(ValueError, "positive"):
            parse_accounts_source({"type": "url", "url": "x", "refresh_interval": 0})


class DocumentConversionTests(unittest.TestCase):
    def test_converts_accounts(self):
        accounts = accounts_from_document(DOC)
        self.assertEqual(len(accounts), 2)
        self.assertEqual(accounts[0], AccountConfig("acc1", "t1", "a1", "w1"))
        self.assertEqual(accounts[1].timezone, "Europe/London")

    def test_missing_accounts_key(self):
        with self.assertRaisesRegex(ValueError, "must contain an 'accounts' list"):
            accounts_from_document({"foo": []})

    def test_rejects_incomplete_account(self):
        with self.assertRaisesRegex(ValueError, "token"):
            accounts_from_document({"accounts": [{"name": "x", "agent_id": "a", "workspace_id": "w"}]})


class ConfigSourceTests(unittest.TestCase):
    def load(self, content):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text(content, encoding="utf-8")
            return load_config(path)

    def test_accounts_source_config(self):
        config = self.load(
            """
version: 2
accounts_source:
  type: url
  url: https://tusi.example/fandai.json
  refresh_interval: 300
"""
        )
        self.assertIsNotNone(config.accounts_source)
        self.assertEqual(config.accounts_source.url, "https://tusi.example/fandai.json")
        self.assertEqual(config.accounts_source.refresh_interval, 300)
        self.assertFalse(config.accounts)

    def test_accounts_source_rejects_mixed(self):
        with self.assertRaisesRegex(ValueError, "accounts_source by itself"):
            self.load(
                """
accounts_source:
  type: url
  url: https://x/y.json
accounts:
  - name: a
    token: t
    agent_id: ag
    workspace_id: ws
"""
            )


class LoaderTests(unittest.IsolatedAsyncioTestCase):
    def _client(self, handler):
        transport = httpx.MockTransport(handler)
        return httpx.AsyncClient(transport=transport)

    async def test_initial_load_builds_manager(self):
        client = self._client(lambda req: httpx.Response(200, json=DOC))
        try:
            loader = RemoteAccountsLoader(
                client,
                AccountsSource("https://x/y.json", refresh_interval=3600),
                adapter_factory=_factory,
            )
            manager = await loader.start()
            self.assertEqual([s["name"] for s in manager.snapshot()], ["acc1", "acc2"])
        finally:
            await loader.aclose()
            await client.aclose()

    async def test_initial_load_failure_raises(self):
        client = self._client(lambda req: httpx.Response(500))
        try:
            loader = RemoteAccountsLoader(
                client, AccountsSource("https://x/y.json"), adapter_factory=_factory
            )
            with self.assertRaises(httpx.HTTPStatusError):
                await loader.start()
        finally:
            await client.aclose()

    async def test_refresh_failure_keeps_cached_accounts(self):
        calls = {"n": 0}

        def handler(req):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(200, json=DOC)
            return httpx.Response(503)

        client = self._client(handler)
        try:
            loader = RemoteAccountsLoader(
                client,
                AccountsSource("https://x/y.json", refresh_interval=3600),
                adapter_factory=_factory,
            )
            manager = await loader.start()
            refreshed = await loader.refresh_once()
            self.assertFalse(refreshed)
            # Cached accounts still present.
            self.assertEqual([s["name"] for s in manager.snapshot()], ["acc1", "acc2"])
        finally:
            await loader.aclose()
            await client.aclose()

    async def test_refresh_applies_new_accounts(self):
        new_doc = {
            "accounts": [
                {"name": "acc1", "token": "t1", "agent_id": "a1", "workspace_id": "w1"},
                {"name": "acc3", "token": "t3", "agent_id": "a3", "workspace_id": "w3"},
            ]
        }
        calls = {"n": 0}

        def handler(req):
            calls["n"] += 1
            return httpx.Response(200, json=DOC if calls["n"] == 1 else new_doc)

        client = self._client(handler)
        try:
            loader = RemoteAccountsLoader(
                client,
                AccountsSource("https://x/y.json", refresh_interval=3600),
                adapter_factory=_factory,
            )
            manager = await loader.start()
            refreshed = await loader.refresh_once()
            self.assertTrue(refreshed)
            self.assertEqual([s["name"] for s in manager.snapshot()], ["acc1", "acc3"])
        finally:
            await loader.aclose()
            await client.aclose()


if __name__ == "__main__":
    unittest.main()
