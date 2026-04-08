"""Watchdog engine configuration."""

import os
from dataclasses import dataclass, field


@dataclass
class WatchdogConfig:
    """Engine configuration from environment variables."""

    catalog: str = field(default_factory=lambda: os.environ.get("WATCHDOG_CATALOG", "platform"))
    schema: str = field(default_factory=lambda: os.environ.get("WATCHDOG_SCHEMA", "watchdog"))
    secret_scope: str = field(default_factory=lambda: os.environ.get("WATCHDOG_SECRET_SCOPE", ""))

    # Multi-metastore: comma-separated list of metastore IDs to scan.
    # Empty = scan current metastore only (default behavior).
    metastore_ids: list[str] = field(default_factory=lambda: [
        m.strip() for m in os.environ.get("WATCHDOG_METASTORE_IDS", "").split(",") if m.strip()
    ])

    @property
    def is_multi_metastore(self) -> bool:
        return len(self.metastore_ids) > 1

    @property
    def qualified_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"
