"""Governance data providers.

The default provider (WatchdogProvider) reads from Delta tables written
by the Watchdog governance scanner. Custom providers can implement the
GovernanceProvider protocol for alternative backends.
"""

from watchdog_governance.providers.watchdog import WatchdogProvider

__all__ = ["WatchdogProvider"]
