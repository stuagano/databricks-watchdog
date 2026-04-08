"""Configuration for the Watchdog MCP managed service.

When running as a Databricks App, the app's service principal handles
lifecycle operations. Data access uses on-behalf-of auth — each request
runs as the calling user's identity with their UC permissions on the
platform.watchdog schema.
"""

import os
from dataclasses import dataclass, field


@dataclass
class WatchdogMcpConfig:
    """Server configuration resolved from environment variables."""

    # Databricks workspace host — auto-detected in Databricks Apps
    host: str = field(default_factory=lambda: os.environ.get("DATABRICKS_HOST", ""))

    # Unity Catalog — Watchdog tables live here
    catalog: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_CATALOG", "platform")
    )
    schema: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_SCHEMA", "watchdog")
    )

    # SQL warehouse for query execution
    warehouse_id: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    )

    # Optional default metastore filter — applies when callers omit metastore
    default_metastore_id: str = field(
        default_factory=lambda: os.environ.get("WATCHDOG_DEFAULT_METASTORE_ID", "")
    )

    # Server
    server_name: str = "watchdog-mcp"
    server_version: str = "0.4.0"

    @property
    def qualified_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"
