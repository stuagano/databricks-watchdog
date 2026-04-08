"""Governance audit logging for the AI DevKit MCP service.

Emits structured audit events for every tool invocation. These events
flow to the Databricks App's standard log output, which is captured by
Azure diagnostics → Log Analytics Workspace. Provides the compliance
trail for regulatory compliance and internal governance: who used which AI capability,
on what data, and when.

Audit events are structured JSON on a dedicated logger so they can be
parsed and queried separately from application logs.
"""

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

audit_logger = logging.getLogger("ai_devkit.audit")


@dataclass
class AuditEvent:
    """Immutable record of a single MCP tool invocation."""

    event_type: str = "tool_invocation"
    timestamp: str = ""
    user: str = ""
    tool: str = ""
    arguments_summary: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0
    success: bool = True
    error: str | None = None
    catalog_accessed: str | None = None
    schema_accessed: str | None = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


def log_tool_call(
    user: str,
    tool: str,
    arguments: dict[str, Any],
    start_time: float,
    success: bool = True,
    error: str | None = None,
) -> None:
    """Emit a structured audit event for a tool invocation.

    Sensitive values (query text, message content) are summarized,
    not logged verbatim — prevents PII leakage into audit logs.
    """
    summary = _summarize_arguments(tool, arguments)
    catalog = arguments.get("catalog")
    schema = arguments.get("schema") or arguments.get("schema_name")

    event = AuditEvent(
        user=user,
        tool=tool,
        arguments_summary=summary,
        duration_ms=int((time.monotonic() - start_time) * 1000),
        success=success,
        error=error,
        catalog_accessed=catalog,
        schema_accessed=schema,
    )

    audit_logger.info(json.dumps(asdict(event), default=str))


def _summarize_arguments(tool: str, args: dict[str, Any]) -> dict[str, Any]:
    """Redact sensitive content, keep structural metadata.

    SQL queries: log first 200 chars + whether it contains JOIN/WHERE.
    Chat messages: log role sequence + message count, not content.
    Embeddings: log text count, not text content.
    """
    summary: dict[str, Any] = {}

    if tool == "sql_query":
        query = args.get("query", "")
        summary["query_preview"] = query[:200] + ("..." if len(query) > 200 else "")
        summary["query_length"] = len(query)
        upper = query.upper()
        summary["has_join"] = "JOIN" in upper
        summary["has_where"] = "WHERE" in upper
        summary["catalog"] = args.get("catalog")
        summary["max_rows"] = args.get("max_rows")

    elif tool == "chat_completion":
        messages = args.get("messages", [])
        summary["message_count"] = len(messages)
        summary["roles"] = [m.get("role") for m in messages]
        summary["model"] = args.get("model")
        summary["max_tokens"] = args.get("max_tokens")

    elif tool == "generate_embeddings":
        texts = args.get("texts", [])
        summary["text_count"] = len(texts)
        summary["model"] = args.get("model")

    elif tool == "vector_search_query":
        summary["index_name"] = args.get("index_name")
        summary["num_results"] = args.get("num_results")
        summary["has_filters"] = bool(args.get("filters"))
        # Don't log query_text — may contain sensitive search terms

    elif tool == "query_model_endpoint":
        summary["endpoint_name"] = args.get("endpoint_name")

    else:
        # Discovery tools (list_*) — log all args, nothing sensitive
        summary = {k: v for k, v in args.items()}

    return summary


def log_session_start(user: str, session_id: str) -> None:
    event = {
        "event_type": "session_start",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user": user,
        "session_id": session_id,
    }
    audit_logger.info(json.dumps(event))


def log_session_end(user: str, session_id: str) -> None:
    event = {
        "event_type": "session_end",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user": user,
        "session_id": session_id,
    }
    audit_logger.info(json.dumps(event))


# Alias for backward compatibility with server.py imports
audit_log = log_tool_call
