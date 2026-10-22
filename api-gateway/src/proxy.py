"""Generic async HTTP proxy using httpx.AsyncClient."""
import base64
import json
from typing import Any

import httpx
from fastapi import Request
from fastapi.responses import Response

_client = httpx.AsyncClient(timeout=30.0)

_HOP_BY_HOP = frozenset([
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
])


async def forward_request(
    upstream_base: str,
    path: str,
    request: Request,
    caller: dict[str, Any] | None = None,
) -> Response:
    """Forward *request* to *upstream_base*/*path* and return the response.

    Copies method, headers (excluding hop-by-hop), body, and query
    params. Returns a 502 if the upstream is unreachable.

    Args:
        upstream_base: Base URL of the upstream service (no trailing
            slash), e.g. ``http://auth-service:8001``.
        path: Path segment to append, e.g. ``health``.
        request: The incoming FastAPI request to forward.
        caller: Authenticated caller dict (``sub``, ``role``,
            ``domains``). When provided, ``X-OCN-Caller`` is injected
            as a base64-encoded JSON header.

    Returns:
        A :class:`fastapi.responses.Response` with the upstream's
        status, headers, and body.
    """
    url = f"{upstream_base.rstrip('/')}/{path}"
    if request.url.query:
        url = f"{url}?{request.url.query}"

    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP and k.lower() != "x-ocn-caller"
    }
    if caller:
        payload = json.dumps({
            "sub": caller["sub"],
            "role": caller["role"],
            "domains": caller.get("domains", []),
        })
        headers["x-ocn-caller"] = base64.b64encode(
            payload.encode()
        ).decode()
    body = await request.body()

    try:
        upstream_response = await _client.request(
            method=request.method,
            url=url,
            headers=headers,
            content=body,
        )
    except httpx.RequestError:
        return Response(
            content=b'{"detail": "Upstream unreachable"}',
            status_code=502,
            media_type="application/json",
        )

    response_headers = {
        k: v
        for k, v in upstream_response.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }
    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers=response_headers,
        media_type=upstream_response.headers.get("content-type"),
    )
