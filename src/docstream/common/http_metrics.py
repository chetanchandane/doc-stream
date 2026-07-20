"""Shared Prometheus wiring for the two FastAPI services.

One helper both apps call, so the gateway and the query API report identical
metric names and labels — otherwise dashboards need per-service special cases.

Paths are taken from the matched ROUTE, not the raw URL: recording
``/jobs/{job_id}`` keeps the label set bounded, whereas raw paths would create a
new time series per document id and blow up cardinality.
"""

from __future__ import annotations

import time

from fastapi import FastAPI, Request, Response

from docstream.common import metrics


def _route_path(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"


def instrument(app: FastAPI, *, service: str) -> None:
    """Add request metrics and a /metrics endpoint to ``app``."""

    @app.middleware("http")
    async def _record(request: Request, call_next):
        # Don't measure the scrape itself.
        if request.url.path == "/metrics":
            return await call_next(request)

        started = time.perf_counter()
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        except Exception:
            # An unhandled exception still becomes a 500 to the client, so count
            # it as one rather than losing the request from the totals.
            status = 500
            raise
        finally:
            path = _route_path(request)
            elapsed = time.perf_counter() - started
            metrics.http_requests_total.labels(
                service=service,
                method=request.method,
                path=path,
                status=str(status),
            ).inc()
            metrics.http_request_seconds.labels(
                service=service, method=request.method, path=path
            ).observe(elapsed)

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        body, content_type = metrics.render()
        return Response(content=body, media_type=content_type)
