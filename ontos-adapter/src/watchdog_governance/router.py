"""Root governance router — mounts all sub-routers.

Integration with Ontos (or any FastAPI app):

    from watchdog_governance import register_routes
    register_routes(app)   # mounts at /api/governance/*

With a custom provider:

    from watchdog_governance import register_routes
    from watchdog_governance.providers import WatchdogProvider

    provider = WatchdogProvider(catalog="my_catalog", schema="my_schema", ...)
    register_routes(app, provider=provider)
"""

from __future__ import annotations

from fastapi import APIRouter, FastAPI

from watchdog_governance.provider import GovernanceProvider
from watchdog_governance.providers.watchdog import WatchdogProvider
from watchdog_governance.routers._deps import get_provider
from watchdog_governance.routers.exceptions import router as exceptions_router
from watchdog_governance.routers.ontology import router as ontology_router
from watchdog_governance.routers.policies import router as policies_router
from watchdog_governance.routers.violations import router as violations_router

root_router = APIRouter()
root_router.include_router(violations_router)
root_router.include_router(policies_router)
root_router.include_router(exceptions_router)
root_router.include_router(ontology_router)


@root_router.get("/health")
def health():
    """Liveness probe."""
    return {"status": "ok", "module": "governance"}


def register_routes(
    app: FastAPI,
    *,
    provider: GovernanceProvider | None = None,
    prefix: str = "/api/governance",
) -> None:
    """Mount governance routes into a FastAPI application.

    This is the primary integration point. Call once at startup:

        register_routes(app)                       # auto-configure from env
        register_routes(app, provider=my_provider)  # explicit provider

    Args:
        app: The FastAPI application to mount into.
        provider: A GovernanceProvider implementation. Defaults to
            ``WatchdogProvider.from_env()``.
        prefix: URL prefix for all governance routes.
    """
    if provider is None:
        provider = WatchdogProvider.from_env()

    app.dependency_overrides[get_provider] = lambda: provider
    app.include_router(root_router, prefix=prefix)
