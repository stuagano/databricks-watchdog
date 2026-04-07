"""Standalone governance API — for development and testing.

Run with:

    cd ontos-adapter
    uvicorn watchdog_governance.app:app --reload

Requires environment variables for WatchdogProvider (see README.md).
For local development without a Databricks connection, the API will
start but endpoints will return 500s until a provider is available.
"""

from fastapi import FastAPI

from watchdog_governance.router import register_routes

app = FastAPI(
    title="Watchdog Governance API",
    description="Pluggable governance UI module — standalone mode",
    version="0.1.0",
)

register_routes(app)
