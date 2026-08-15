import asyncio
import unittest

from app.adapters.base import GatewayError
from app.adapters.tasklet import _SyncBaseline, TaskletAdapter


class FakeWebSocket:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.closed = False

    async def recv(self):
        try:
            return next(self.messages)
        except StopIteration:
            await asyncio.sleep(3600)

    async def close(self):
        self.closed = True


def message(value):
    import json

    return json.dumps(value)


class TaskletLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def collect(self, websocket, baseline):
        adapter = object.__new__(TaskletAdapter)
        values = []
        async for value in adapter._iter_sync(websocket, "agent-1", baseline):
            values.append(value)
        return values

    async def test_historical_credit_error_does_not_abort_new_reply(self):
        baseline = _SyncBaseline({}, frozenset({"old"}), "error", [])
        websocket = FakeWebSocket(
            [
                message(
                    {
                        "type": "blocksUpdate",
                        "runId": "agent-1",
                        "kind": "incremental",
                        "updates": {
                            "new-user": {
                                "type": "user_message",
                                "blockId": "new-user",
                                "startTime": 2001,
                            }
                        },
                    }
                ),
                message(
                    {
                        "type": "syncUpdate",
                        "agentId": "agent-1",
                        "state": {"runState": {"type": "idle"}},
                    }
                ),
                message(
                    {
                        "type": "blocksUpdate",
                        "runId": "agent-1",
                        "kind": "incremental",
                        "updates": {
                            "new-answer": {
                                "type": "agent_content",
                                "blockId": "new-answer",
                                "startTime": 2002,
                                "content": "Hello",
                            }
                        },
                    }
                ),
            ]
        )
        self.assertEqual(await self.collect(websocket, baseline), ["Hello"])
        self.assertTrue(websocket.closed)

    async def test_current_credit_error_after_lifecycle_is_failure(self):
        baseline = _SyncBaseline({}, frozenset(), "idle", [])
        websocket = FakeWebSocket(
            [
                message(
                    {
                        "type": "syncUpdate",
                        "agentId": "agent-1",
                        "state": {"runState": {"type": "running"}},
                    }
                ),
                message(
                    {
                        "type": "syncUpdate",
                        "agentId": "agent-1",
                        "state": {
                            "runState": {
                                "type": "error",
                                "error": "You have run out of credits.",
                                "errorCode": "agent/credits_exhausted",
                            }
                        },
                    }
                ),
            ]
        )
        with self.assertRaisesRegex(GatewayError, "run out of credits"):
            await self.collect(websocket, baseline)
        self.assertTrue(websocket.closed)


if __name__ == "__main__":
    unittest.main()
