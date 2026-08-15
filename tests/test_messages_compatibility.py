import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app
from app.models import AnthropicMessagesRequest, normalize_messages


async def text_stream():
    yield "Hello from Tasklet"


class AnthropicMessagesCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def normalize(self, **overrides):
        payload = {
            "model": "tasklet",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "Hello"}],
            **overrides,
        }
        return normalize_messages(AnthropicMessagesRequest.model_validate(payload))

    def assert_messages_response(self, **overrides):
        payload = {
            "model": "tasklet",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "Hello"}],
            **overrides,
        }
        with patch("app.main._open_text_stream", return_value=text_stream()):
            response = self.client.post("/v1/messages", json=payload)

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["type"], "message")
        self.assertEqual(body["content"], [{"type": "text", "text": "Hello from Tasklet"}])

    def test_plain_messages(self):
        request = self.normalize()

        self.assertEqual(request.model, "tasklet")
        self.assertEqual([(turn.role, turn.text) for turn in request.turns], [("user", "Hello")])
        self.assertEqual(request.max_output_tokens, 1024)
        self.assert_messages_response()

    def test_messages_with_tools_are_downgraded_to_text(self):
        tools = [
            {
                "name": "get_weather",
                "description": "Get the weather for a location",
                "input_schema": {
                    "type": "object",
                    "properties": {"location": {"type": "string"}},
                    "required": ["location"],
                },
            }
        ]
        request = self.normalize(tools=tools)

        self.assertEqual([(turn.role, turn.text) for turn in request.turns], [("user", "Hello")])
        self.assert_messages_response(tools=tools)

    def test_messages_with_thinking_are_downgraded_to_text(self):
        request = self.normalize(thinking={"type": "adaptive"})

        self.assertEqual([(turn.role, turn.text) for turn in request.turns], [("user", "Hello")])
        self.assert_messages_response(thinking={"type": "adaptive"})


if __name__ == "__main__":
    unittest.main()
