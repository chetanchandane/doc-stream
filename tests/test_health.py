"""The workers' health endpoint.

Kubernetes probes this, so it needs to actually answer on a real socket — these
tests start the server and speak HTTP to it rather than calling the handler.
"""

from __future__ import annotations

import asyncio

import pytest

from docstream.common.health import start_health_server


async def _get(port: int, path: str) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
    await writer.drain()
    data = await asyncio.wait_for(reader.read(1024), timeout=5)
    writer.close()
    await writer.wait_closed()
    return data


@pytest.fixture
async def health_port(unused_tcp_port):
    server = await start_health_server(unused_tcp_port, host="127.0.0.1")
    try:
        yield unused_tcp_port
    finally:
        server.close()


@pytest.mark.parametrize("path", ["/healthz", "/readyz", "/health", "/"])
async def test_health_paths_return_200(health_port, path):
    body = await _get(health_port, path)
    assert b"200 OK" in body
    assert b'{"status":"ok"}' in body


async def test_unknown_path_returns_404(health_port):
    body = await _get(health_port, "/nope")
    assert b"404 Not Found" in body


async def test_server_handles_repeated_probes(health_port):
    """kubelet probes on an interval forever; the server must not leak or wedge."""
    for _ in range(20):
        assert b"200 OK" in await _get(health_port, "/healthz")


async def test_server_survives_a_client_that_hangs_up_early(health_port):
    """A probe that disconnects mid-request must not kill the server."""
    reader, writer = await asyncio.open_connection("127.0.0.1", health_port)
    writer.write(b"GET /healthz")  # no newline, then abandon it
    await writer.drain()
    writer.close()
    try:
        await writer.wait_closed()
    except (ConnectionError, RuntimeError):
        pass

    # Still serving.
    assert b"200 OK" in await _get(health_port, "/healthz")
