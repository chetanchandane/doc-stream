"""A minimal HTTP health endpoint for the workers.

The gateway and query service are FastAPI apps with a ``/healthz`` route, but the
three Kafka workers are plain consumer loops with no HTTP surface. Kubernetes
needs something to probe: without it a *crashed* worker restarts (the process
exits), but a *hung* one — stuck on a wedged connection, say — would sit there
consuming nothing while looking perfectly healthy.

Deliberately built on ``asyncio.start_server`` rather than a web framework: it
runs alongside the consumer loop in the same event loop, adds no dependency, and
stays small enough to read in one sitting. Phase 4 can extend ``/metrics`` here
for Prometheus.

Routes: ``/healthz`` (liveness) and ``/readyz`` (readiness) — both 200 while the
loop is running.
"""

from __future__ import annotations

import asyncio
import logging

log = logging.getLogger("docstream.health")

_RESPONSE = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Type: application/json\r\n"
    b"Content-Length: 15\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b'{"status":"ok"}'
)

_NOT_FOUND = (
    b"HTTP/1.1 404 Not Found\r\n"
    b"Content-Length: 0\r\n"
    b"Connection: close\r\n"
    b"\r\n"
)

_HEALTH_PATHS = {"/healthz", "/readyz", "/health", "/"}


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        # Only the request line matters; we never read a body.
        line = await asyncio.wait_for(reader.readline(), timeout=5)
        parts = line.decode("latin-1").split()
        path = parts[1] if len(parts) > 1 else "/"
        writer.write(_RESPONSE if path in _HEALTH_PATHS else _NOT_FOUND)
        await writer.drain()
    except (TimeoutError, asyncio.TimeoutError, ConnectionError):
        pass  # probe hung up or timed out; nothing useful to do
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, RuntimeError):
            pass


async def start_health_server(port: int, host: str = "0.0.0.0") -> asyncio.AbstractServer:
    """Start the health server and return it so the caller can close it.

    Binds 0.0.0.0 because kubelet probes the pod IP, not localhost.
    """
    server = await asyncio.start_server(_handle, host, port)
    log.info("health endpoint listening on %s:%d", host, port)
    return server
