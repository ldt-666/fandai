import tempfile
import unittest
from pathlib import Path

import httpx

from app.account_pool import AccountManager, DEFAULT_TASKLET_API_URL
from app.config import build_adapters, load_config


class ConfigTests(unittest.TestCase):
    def load(self, content):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text(content, encoding="utf-8")
            return load_config(path)

    def test_legacy_single_account_is_compatible(self):
        config = self.load(
            """
tasklet:
  api_url: https://example.test/api/sendChatMessage
  token: legacy-token
  agent_id: legacy-agent
  workspace_id: legacy-workspace
"""
        )

        self.assertTrue(config.legacy)
        self.assertFalse(config.accounts)
        route = config.resolve_model("tasklet")
        self.assertEqual(route.agent_id, "legacy-agent")
        self.assertEqual(route.workspace_id, "legacy-workspace")

    def test_existing_v2_config_is_compatible(self):
        config = self.load(
            """
version: 2
adapters:
  primary:
    type: tasklet
    api_url: https://example.test/api/sendChatMessage
    token: v2-token
models:
  tasklet:
    adapter: primary
    agent_id: v2-agent
    workspace_id: v2-workspace
    aliases: [tasklet]
"""
        )

        self.assertFalse(config.legacy)
        self.assertFalse(config.accounts)
        route = config.resolve_model("tasklet")
        self.assertEqual(route.adapter, "primary")
        self.assertEqual(route.agent_id, "v2-agent")

    def test_accounts_config_builds_manager_with_defaults(self):
        config = self.load(
            """
version: 2
accounts:
  - name: account-a
    token: token-a
    agent_id: agent-a
    workspace_id: workspace-a
  - name: account-b
    token: token-b
    agent_id: agent-b
    workspace_id: workspace-b
    timezone: Europe/London
    api_url: https://tasklet.example/api/sendChatMessage
"""
        )

        self.assertEqual(len(config.accounts), 2)
        self.assertEqual(config.accounts[0].api_url, DEFAULT_TASKLET_API_URL)
        self.assertEqual(config.accounts[0].timezone, "Asia/Singapore")
        self.assertEqual(config.resolve_model("tasklet").adapter, "accounts")

        client = httpx.AsyncClient()
        try:
            adapters = build_adapters(config, client)
            self.assertIsInstance(adapters["accounts"], AccountManager)
            snapshot = adapters["accounts"].snapshot()
            self.assertEqual([item["name"] for item in snapshot], ["account-a", "account-b"])
            self.assertNotIn("token-a", repr(snapshot))
            self.assertNotIn("token-b", repr(snapshot))
        finally:
            import asyncio

            asyncio.run(client.aclose())

    def test_accounts_must_be_non_empty(self):
        with self.assertRaisesRegex(ValueError, "non-empty list"):
            self.load("accounts: []\n")

    def test_accounts_require_complete_bindings(self):
        with self.assertRaisesRegex(ValueError, "token"):
            self.load(
                """
accounts:
  - name: incomplete
    agent_id: agent
    workspace_id: workspace
"""
            )

    def test_accounts_reject_duplicate_names(self):
        with self.assertRaisesRegex(ValueError, "Duplicate account name"):
            self.load(
                """
accounts:
  - name: duplicate
    token: token-a
    agent_id: agent-a
    workspace_id: workspace-a
  - name: duplicate
    token: token-b
    agent_id: agent-b
    workspace_id: workspace-b
"""
            )

    def test_accounts_reject_mixed_modes(self):
        with self.assertRaisesRegex(ValueError, "Use accounts config by itself"):
            self.load(
                """
accounts:
  - name: account-a
    token: token-a
    agent_id: agent-a
    workspace_id: workspace-a
adapters: {}
models: {}
"""
            )

    def test_accounts_reject_unknown_keys(self):
        with self.assertRaisesRegex(ValueError, "Unknown account 1 keys"):
            self.load(
                """
accounts:
  - name: account-a
    token: token-a
    agent_id: agent-a
    workspace_id: workspace-a
    tokne: typo
"""
            )


if __name__ == "__main__":
    unittest.main()
