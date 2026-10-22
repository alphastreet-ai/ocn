"""Tests for header handling in forward_request."""
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import Request

import proxy


async def _receive() -> dict:
    return {"type": "http.request", "body": b""}


async def test_forged_caller_header_not_forwarded() -> None:
    """A client-supplied x-ocn-caller must be stripped when unauthenticated."""
    request = Request({
        "type": "http",
        "method": "GET",
        "path": "/agent/x",
        "query_string": b"",
        "headers": [(b"x-ocn-caller", b"Zm9yZ2Vk")],
    }, _receive)
    upstream = MagicMock(status_code=200, content=b"", headers={})
    with patch.object(
        proxy._client, "request", new_callable=AsyncMock, return_value=upstream
    ) as mock_request:
        await proxy.forward_request("http://upstream", "x", request)
    assert "x-ocn-caller" not in mock_request.call_args.kwargs["headers"]
