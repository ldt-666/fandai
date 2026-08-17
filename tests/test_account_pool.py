import asyncio
import unittest

from app.account_pool import AccountConfig, AccountManager
from app.adapters.base import AdapterStream, GatewayError, NormalizedRequest, NormalizedTurn


class ScriptedAdapter:
    def __init__(self, name, script, calls):
        self.name = name
        self.script = list(script)
        self.calls = calls

    async def open_stream(self, request, route):
        self.calls.append((self.name, route.agent_id, route.workspace_id, request.model))
        action = self.script.pop(0)
        if isinstance(action, Exception):
            raise action
        return AdapterStream(None, action())


async def text(value):
    yield value


async def stream_error(error):
    if False:
        yield ""
    raise error


class AccountManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.accounts = (
            AccountConfig("account-a", "token-a", "agent-a", "workspace-a"),
            AccountConfig("account-b", "token-b", "agent-b", "workspace-b"),
        )
        self.request = NormalizedRequest(
            model="tasklet",
            instructions=None,
            turns=(NormalizedTurn("user", "hello"),),
            stream=True,
        )
        self.calls = []

    def manager(self, scripts):
        def factory(client, *, api_url, token):
            del client, api_url
            return ScriptedAdapter(token, scripts[token], self.calls)

        return AccountManager(None, self.accounts, adapter_factory=factory)

    async def collect(self, stream):
        values = []
        try:
            async for value in stream:
                values.append(value)
        finally:
            await stream.aclose()
        return values

    async def test_round_robin_uses_complete_account_bindings(self):
        manager = self.manager(
            {
                "token-a": [lambda: text("A1"), lambda: text("A2")],
                "token-b": [lambda: text("B1")],
            }
        )

        results = []
        for _ in range(3):
            stream = await manager.open_stream(self.request, None)
            results.append(await self.collect(stream))

        self.assertEqual(results, [["A1"], ["B1"], ["A2"]])
        self.assertEqual(
            [(name, agent, workspace) for name, agent, workspace, _ in self.calls],
            [
                ("token-a", "agent-a", "workspace-a"),
                ("token-b", "agent-b", "workspace-b"),
                ("token-a", "agent-a", "workspace-a"),
            ],
        )

    async def test_retryable_preacceptance_failure_switches_accounts(self):
        failure = GatewayError(
            "bad token",
            status_code=401,
            code="authentication_error",
            retryable_before_acceptance=True,
            account_failure=True,
        )
        manager = self.manager(
            {
                "token-a": [failure],
                "token-b": [lambda: text("fallback")],
            }
        )

        stream = await manager.open_stream(self.request, None)
        self.assertEqual(await self.collect(stream), ["fallback"])
        self.assertEqual([call[0] for call in self.calls], ["token-a", "token-b"])
        self.assertEqual(manager.snapshot()[0]["status"], "disabled")

    async def test_retryable_upstream_failure_switches_accounts(self):
        failure = GatewayError(
            "upstream unavailable",
            status_code=503,
            code="upstream_error",
            retryable_before_acceptance=True,
            account_failure=True,
        )
        manager = self.manager(
            {
                "token-a": [failure],
                "token-b": [lambda: text("fallback")],
            }
        )

        stream = await manager.open_stream(self.request, None)
        self.assertEqual(await self.collect(stream), ["fallback"])
        self.assertEqual([call[0] for call in self.calls], ["token-a", "token-b"])
        self.assertEqual(manager.snapshot()[0]["status"], "cooldown")

    async def test_retryable_preacceptance_credits_failure_switches_accounts(self):
        failure = GatewayError(
            "credits exhausted",
            code="agent/credits_exhausted",
            retryable_before_acceptance=True,
            account_failure=True,
        )
        manager = self.manager(
            {
                "token-a": [failure],
                "token-b": [lambda: text("fallback")],
            }
        )

        stream = await manager.open_stream(self.request, None)
        self.assertEqual(await self.collect(stream), ["fallback"])
        self.assertEqual(manager.snapshot()[0]["status"], "disabled")

    async def test_unknown_acceptance_failure_does_not_switch_accounts(self):
        failure = GatewayError(
            "trigger timed out",
            code="network_error",
            account_failure=True,
            acceptance_unknown=True,
        )
        manager = self.manager(
            {
                "token-a": [failure],
                "token-b": [lambda: text("must not run")],
            }
        )

        with self.assertRaisesRegex(GatewayError, "trigger timed out"):
            await manager.open_stream(self.request, None)

        self.assertEqual([call[0] for call in self.calls], ["token-a"])
        self.assertEqual(manager.snapshot()[0]["status"], "cooldown")

    async def test_accepted_stream_failure_does_not_switch_accounts(self):
        failure = GatewayError(
            "credits exhausted",
            code="agent/credits_exhausted",
        )
        manager = self.manager(
            {
                "token-a": [lambda: stream_error(failure)],
                "token-b": [lambda: text("must not run")],
            }
        )

        stream = await manager.open_stream(self.request, None)
        with self.assertRaisesRegex(GatewayError, "credits exhausted"):
            await self.collect(stream)

        self.assertEqual([call[0] for call in self.calls], ["token-a"])
        self.assertEqual(manager.snapshot()[0]["status"], "disabled")

    async def test_single_account_lease_serializes_requests(self):
        accounts = (self.accounts[0],)
        started = asyncio.Event()

        async def held_stream():
            started.set()
            await asyncio.Event().wait()
            yield "unreachable"

        scripts = {"token-a": [held_stream, lambda: text("second")]}

        def factory(client, *, api_url, token):
            del client, api_url
            return ScriptedAdapter(token, scripts[token], self.calls)

        manager = AccountManager(None, accounts, adapter_factory=factory)
        first = await manager.open_stream(self.request, None)
        first_iterator = first.__aiter__()
        first_next = asyncio.create_task(anext(first_iterator))
        await started.wait()

        second_task = asyncio.create_task(manager.open_stream(self.request, None))
        await asyncio.sleep(0)
        self.assertFalse(second_task.done())
        self.assertEqual(len(self.calls), 1)

        first_next.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first_next
        await first.aclose()
        second = await asyncio.wait_for(second_task, timeout=1)
        self.assertEqual(await self.collect(second), ["second"])
        self.assertEqual(len(self.calls), 2)

    async def test_all_disabled_accounts_report_unavailable_on_next_request(self):
        failure = GatewayError(
            "bad token",
            status_code=401,
            code="authentication_error",
            retryable_before_acceptance=True,
            account_failure=True,
        )
        manager = self.manager(
            {
                "token-a": [failure],
                "token-b": [failure],
            }
        )

        with self.assertRaises(GatewayError):
            await manager.open_stream(self.request, None)
        with self.assertRaisesRegex(GatewayError, "No Tasklet accounts") as raised:
            await manager.open_stream(self.request, None)
        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.code, "accounts_unavailable")


if __name__ == "__main__":
    unittest.main()
