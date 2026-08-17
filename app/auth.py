"""Optional client API-key authentication for the public inference endpoints.

This layer sits entirely in front of the request handlers. It does not touch the
Tasklet adapter, the account pool, or how upstream credentials are stored — it
only decides whether an incoming client request is allowed to reach a route.

Enable it by setting the ``FANDAI_API_KEY`` environment variable. When that
variable is unset or empty, authentication is disabled and every request passes
through (preserving the previous open behaviour for local development).

Two client header styles are accepted, for OpenAI / Codex and Anthropic / Claude
compatibility:

- ``Authorization: Bearer YOUR_KEY``
- ``x-api-key: YOUR_KEY``
"""

from __future__ import annotations

import hmac
import os

from fastapi import HTTPException, Request

API_KEY_ENV = "FANDAI_API_KEY"


def _expected_key() -> str | None:
    value = os.getenv(API_KEY_ENV)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _presented_key(request: Request) -> str | None:
    """Extract the client-supplied key from either supported header."""
    authorization = request.headers.get("authorization")
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer" and token.strip():
            return token.strip()
    api_key = request.headers.get("x-api-key")
    if api_key and api_key.strip():
        return api_key.strip()
    return None


async def require_api_key(request: Request) -> None:
    """FastAPI dependency guarding an endpoint with the configured key.

    When ``FANDAI_API_KEY`` is unset the dependency is a no-op. Otherwise a
    missing or mismatched key yields HTTP 401.
    """
    expected = _expected_key()
    if expected is None:
        return
    presented = _presented_key(request)
    if presented is None or not hmac.compare_digest(presented, expected):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
