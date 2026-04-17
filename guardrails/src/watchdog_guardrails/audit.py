"""Governance audit logging for the AI guardrails MCP service.

Emits structured audit events for every tool invocation. These events
flow to the app's standard log output. Audit events are structured JSON
on a dedicated logger so they can be parsed and queried separately.

Sensitive values (query text, message content) are summarized, not logged
verbatim — prevents PII leakage into audit logs.
"""

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

audit_logger = logging.getLogger("ai_devkit.audit")

_KNOWN_ROLES = {"user", "assistant", "system", "tool"}


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
    """Emit a structured audit event for a tool invocation."""
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
    """Redact sensitive content, keep structural metadata."""
    summary: dict[str, Any] = {}

    if tool == "sql_query":
        query = args.get("query", "")
        # Strip WHERE/HAVING clauses from preview to avoid leaking filter values or
        # column names used as predicates. Structural metadata is captured separately.
        upper = query.upper()
        where_pos = upper.find("WHERE")
        having_pos = upper.find("HAVING")
        cutoff = min(
            p for p in (where_pos, having_pos, len(query)) if p >= 0
        )
        safe_query = query[:cutoff].rstrip()
        preview_raw = safe_query[:200] + ("..." if len(safe_query) > 200 else "")
        summary["query_preview"] = preview_raw
        summary["query_length"] = len(query)
        summary["has_join"] = "JOIN" in upper
        summary["has_where"] = "WHERE" in upper
        summary["catalog"] = args.get("catalog")
        summary["max_rows"] = args.get("max_rows")

    elif tool == "chat_completion":
        messages = args.get("messages", [])
        summary["message_count"] = len(messages)
        summary["roles"] = [
            m.get("role") if m.get("role") in _KNOWN_ROLES else "unknown"
            for m in messages
        ]
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

    else:
        summary = {"arg_keys": list(args.keys())}

    return summary
