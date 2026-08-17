"""Static assertions on nginx.conf.example.

We can't run nginx in CI, so instead we verify that the example config the
deployment docs point at contains the directives required for correct SSE
streaming and header forwarding.
"""

import unittest
from pathlib import Path

CONF = (Path(__file__).resolve().parent.parent / "nginx.conf.example").read_text(encoding="utf-8")


class NginxConfigTests(unittest.TestCase):
    def test_sse_buffering_is_disabled(self):
        # SSE must not be buffered or cached by nginx.
        self.assertIn("proxy_buffering    off;", CONF)
        self.assertIn("X-Accel-Buffering no;", CONF)

    def test_all_three_endpoints_are_proxied(self):
        for path in ("/v1/chat/completions", "/v1/responses", "/v1/messages"):
            self.assertIn(f"location = {path} {{", CONF)

    def test_forwarded_headers_present(self):
        for header in ("Host", "X-Real-IP", "X-Forwarded-For"):
            self.assertIn(header, CONF)

    def test_proxies_to_loopback_upstream(self):
        self.assertIn("server 127.0.0.1:8000;", CONF)

    def test_generous_read_timeout(self):
        self.assertIn("proxy_read_timeout    600s;", CONF)


if __name__ == "__main__":
    unittest.main()
