"""Configuration for the AI DevKit MCP managed service.

When running as a Databricks App, the app's service principal handles
lifecycle operations. Data access uses on-behalf-of auth — each request
runs as the calling user's identity with their UC permissions.
"""

import os
from dataclasses import dataclass, field


@dataclass
class AiDevkitConfig:
    """Server configuration resolved from environment variables."""

    # Databricks workspace host — auto-detected in Databricks Apps
    host: str = field(default_factory=lambda: os.environ.get("DATABRICKS_HOST", ""))

    # Unity Catalog — default catalog context for tools that don't specify one
    catalog: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_CATALOG", "")
    )

    # Watchdog schema (catalog.schema) — for governance violation checks
    watchdog_schema: str = field(
        default_factory=lambda: os.environ.get("WATCHDOG_SCHEMA", "platform.watchdog")
    )

    # Extra sensitive column patterns — extend built-in PII/PHI/export detection
    # Format: {"pii": ["pattern1"], "phi": ["pattern2"], "export": ["pattern3"]}
    extra_sensitive_patterns: dict = field(default_factory=dict)

    # SQL warehouse for query execution
    warehouse_id: str = field(
        default_factory=lambda: os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    )

    # Vector Search
    vector_search_endpoint: str = field(
        default_factory=lambda: os.environ.get("VECTOR_SEARCH_ENDPOINT", "ai_devkit_vs")
    )

    # Foundation Model API — default chat model
    default_fmai_model: str = field(
        default_factory=lambda: os.environ.get(
            "DEFAULT_FMAI_MODEL", "databricks-meta-llama-3-3-70b-instruct"
        )
    )

    # Foundation Model API — default embedding model
    default_embedding_model: str = field(
        default_factory=lambda: os.environ.get(
            "DEFAULT_EMBEDDING_MODEL", "databricks-bge-large-en"
        )
    )

    # External model endpoint (e.g. Azure OpenAI proxy)
    default_model_endpoint: str = field(
        default_factory=lambda: os.environ.get("DEFAULT_MODEL_ENDPOINT", "")
    )

    # Server
    server_name: str = "ai-devkit-mcp"
    server_version: str = "0.2.0"
