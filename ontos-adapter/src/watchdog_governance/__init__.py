"""Watchdog Governance — pluggable governance UI module for Ontos.

Quick start (Ontos integration):

    from watchdog_governance import register_routes
    register_routes(app)   # mounts at /api/governance/*

Quick start (standalone):

    uvicorn watchdog_governance.app:app --reload
"""

from watchdog_governance.provider import GovernanceProvider
from watchdog_governance.router import register_routes

__all__ = ["GovernanceProvider", "register_routes"]
