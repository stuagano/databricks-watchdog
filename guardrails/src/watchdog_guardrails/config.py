"""Configuration for the AI guardrails MCP server.

All values from environment variables. No defaults that tie the server
to any specific customer or workspace.
"""

import os
from dataclasses import dataclass, field


@dataclass
class GuardrailsConfig:
    """Server configuration resolved from environment variables."""

    # Databricks workspace host — auto-detected in Databricks Apps
    host: str = field(default_factory=lambda: os.environ.get("DATABRICKS_HOST", ""))

    # Unity Catalog — default catalog context
    catalog: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_CATALOG", "")
    )

    # Watchdog schema (catalog.schema) for violation checks
    watchdog_schema: str = field(
        default_factory=lambda: os.environ.get("WATCHDOG_SCHEMA", "platform.watchdog")
    )

    # SQL warehouse for metadata queries
    warehouse_id: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    )

    server_name: str = "watchdog-guardrails"
    server_version: str = "1.0.0"
