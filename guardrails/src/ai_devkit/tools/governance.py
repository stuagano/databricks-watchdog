"""Governance tools for AI developers — validate, discover, build safely.

Helps developers build AI features on governed data. validate_ai_query
enforces classification/risk policies and suggests alternatives when
blocked. suggest_safe_tables finds usable data for a given operation.
For viewing violations and scan history, use watchdog-mcp.

Runs as the calling user's identity (on-behalf-of). UC grants govern
what metadata the user can see.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from mcp.types import TextContent, Tool

from ai_devkit.audit import log_tool_call as audit_log
from ai_devkit.config import AiDevkitConfig
from ai_devkit.watchdog_client import (
    _esc,
    get_grants_for_resource,
    get_resource_governance,
    get_service_principal_governance,
    ResourceGovernanceState,
)

logger = logging.getLogger(__name__)

# ── Runtime agent session state (per server instance) ──────────────────────
_agent_sessions: dict[str, dict] = {}

TOOLS = [
    Tool(
        name="get_table_lineage",
        description=(
            "Get upstream and downstream lineage for a table from Unity Catalog. "
            "Shows what feeds into this table and what depends on it. "
            "Essential for impact analysis before modifying data."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name: catalog.schema.table",
                },
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="get_table_permissions",
        description=(
            "List who has access to a table and at what level (SELECT, MODIFY, "
            "ALL PRIVILEGES). Shows grants by group and principal. Useful for "
            "understanding the access boundary before sharing data through AI tools."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name: catalog.schema.table",
                },
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="describe_table",
        description=(
            "Get detailed metadata for a table: columns, types, comments, "
            "tags, properties, storage location, and row count. Richer than "
            "list_tables — use this when you need column-level detail."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name: catalog.schema.table",
                },
                "include_column_tags": {
                    "type": "boolean",
                    "description": "Include column-level tags (PII, classification). Default: true.",
                },
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="search_tables_by_tag",
        description=(
            "Find tables matching governance tags. Search by classification "
            "(e.g. 'pii=true', 'classification=confidential') or custom tags. "
            "Returns matching tables with their tag values."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tag_name": {
                    "type": "string",
                    "description": "Tag key to search for (e.g. 'pii', 'classification', 'data_owner').",
                },
                "tag_value": {
                    "type": "string",
                    "description": "Tag value to match. Omit to find all tables with this tag.",
                },
                "catalog": {
                    "type": "string",
                    "description": "Limit search to a specific catalog. Omit to search all accessible catalogs.",
                },
            },
            "required": ["tag_name"],
        },
    ),
    Tool(
        name="validate_ai_query",
        description=(
            "Pre-flight governance check before an AI operation. Validates one or "
            "more tables against classification tags, Watchdog violations, and access "
            "permissions for a specific operation type (query, embed, chat_context, "
            "train). Returns a verdict (proceed, warning, blocked) with per-table "
            "findings. When blocked, suggests alternative tables in the same schema "
            "that are safe for the requested operation. Call this BEFORE running "
            "sql_query, generate_embeddings, or chat_completion with data context."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Fully qualified table names to validate "
                        "(e.g. ['catalog.schema.table1', 'catalog.schema.table2'])."
                    ),
                },
                "operation": {
                    "type": "string",
                    "enum": ["query", "embed", "chat_context", "train"],
                    "description": (
                        "Intended operation: 'query' (SQL read), 'embed' (generate embeddings), "
                        "'chat_context' (feed to LLM as context), 'train' (use for model training). "
                        "Higher-risk operations (embed, train) trigger stricter checks on classified data."
                    ),
                },
                "purpose": {
                    "type": "string",
                    "description": (
                        "Brief description of why this data is needed. Logged for audit trail."
                    ),
                },
            },
            "required": ["tables", "operation"],
        },
    ),
    Tool(
        name="suggest_safe_tables",
        description=(
            "Find tables you can safely use for a given AI operation. Searches "
            "a catalog or schema for tables whose classification level is compatible "
            "with your intended operation (query, embed, chat_context, train). "
            "Use this when you need data for a feature but aren't sure what's "
            "available within governance limits, or when validate_ai_query blocks "
            "a table and you need an alternative."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": ["query", "embed", "chat_context", "train"],
                    "description": "Intended operation — determines max classification level allowed.",
                },
                "schema_name": {
                    "type": "string",
                    "description": (
                        "Schema to search (catalog.schema). Searches sibling tables. "
                        "If omitted, searches the default catalog."
                    ),
                },
                "keyword": {
                    "type": "string",
                    "description": "Optional keyword to filter table names or comments (e.g. 'revenue', 'customer').",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results. Default: 20.",
                },
            },
            "required": ["operation"],
        },
    ),
    Tool(
        name="preview_data",
        description=(
            "Peek at sample rows from a table. Shows actual data values so you "
            "can understand shape, content, and quality before building a pipeline. "
            "Respects UC grants — you only see what you have access to. Limits to "
            "10 rows by default. Use this as your first step when exploring data."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name: catalog.schema.table",
                },
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific columns to preview. Omit for all columns.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of rows. Default: 10, max: 50.",
                },
                "where": {
                    "type": "string",
                    "description": "Optional WHERE clause to filter rows (e.g. \"region = 'EMEA'\").",
                },
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="safe_columns",
        description=(
            "For a table that's partially restricted, find which columns are safe "
            "for your operation. Returns columns grouped by safety: 'safe' (no "
            "sensitive tags), 'warning' (sensitive but allowed for this operation), "
            "and 'blocked' (PII/PHI/export-controlled columns to exclude). "
            "Use this when validate_ai_query warns or blocks a table — often you "
            "can still use the table by excluding specific columns."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name: catalog.schema.table",
                },
                "operation": {
                    "type": "string",
                    "enum": ["query", "embed", "chat_context", "train"],
                    "description": "Intended operation — determines which columns are safe.",
                },
            },
            "required": ["table_name", "operation"],
        },
    ),
    Tool(
        name="estimate_cost",
        description=(
            "Estimate the DBU cost of an AI operation on a table before running it. "
            "Uses row count and column count to estimate tokens, then maps to DBU "
            "cost for the operation type. Prevents expensive surprises — check this "
            "before embedding large tables or running bulk inference."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name: catalog.schema.table",
                },
                "operation": {
                    "type": "string",
                    "enum": ["embed", "chat_context", "train", "query"],
                    "description": "Operation type — affects cost multiplier.",
                },
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Columns to include. Omit for all columns (worst case estimate).",
                },
                "row_limit": {
                    "type": "integer",
                    "description": "If you plan to process a subset, specify row count for tighter estimate.",
                },
            },
            "required": ["table_name", "operation"],
        },
    ),
    # ── Runtime governance tools (Phase 5D) ────────────────────────────────
    Tool(
        name="check_before_access",
        description=(
            "Runtime governance check — call BEFORE an agent accesses a table. "
            "Returns allow/deny based on the table's classification, the agent's "
            "governance status, and applicable policies. Includes suggested "
            "alternatives when access is denied (e.g., a masked view)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "agent_id": {
                    "type": "string",
                    "description": "Identifier of the calling agent.",
                },
                "table": {
                    "type": "string",
                    "description": "Fully qualified table name (catalog.schema.table).",
                },
                "operation": {
                    "type": "string",
                    "enum": ["SELECT", "INSERT", "UPDATE", "DELETE"],
                    "description": "Operation the agent intends to perform. Default: SELECT.",
                },
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific columns the agent will access. If omitted, checks all columns.",
                },
            },
            "required": ["agent_id", "table"],
        },
    ),
    Tool(
        name="log_agent_action",
        description=(
            "Log an agent action for governance audit trail. Call this after "
            "each significant action (data access, external API call, data export). "
            "Logged to the Watchdog audit tables for compliance review."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "agent_id": {"type": "string", "description": "Agent identifier."},
                "action": {
                    "type": "string",
                    "enum": ["data_access", "data_export", "external_api_call", "model_invocation", "tool_call"],
                    "description": "Type of action being logged.",
                },
                "target": {"type": "string", "description": "What was accessed (table name, API URL, endpoint name)."},
                "details": {"type": "object", "description": "Additional action context (columns, row_count, response_status)."},
                "classification": {"type": "string", "description": "Data classification of the target (if known)."},
            },
            "required": ["agent_id", "action", "target"],
        },
    ),
    Tool(
        name="get_agent_compliance",
        description=(
            "Get the current compliance status of an agent. Returns how many "
            "governance checks passed/failed in the current session, which "
            "data classifications were accessed, and overall risk assessment."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "agent_id": {"type": "string", "description": "Agent identifier."},
            },
            "required": ["agent_id"],
        },
    ),
    Tool(
        name="report_agent_execution",
        description=(
            "Generate a post-execution compliance report for an agent. "
            "Summarizes all governance checks, data accessed, policies "
            "triggered, and overall compliance assessment. Call when the "
            "agent finishes its task."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "agent_id": {"type": "string", "description": "Agent identifier."},
                "execution_summary": {"type": "string", "description": "Brief description of what the agent did."},
            },
            "required": ["agent_id"],
        },
    ),
]


async def handle(
    name: str, arguments: dict[str, Any], w: WorkspaceClient, config: AiDevkitConfig
) -> list[TextContent]:
    if name == "get_table_lineage":
        return await _get_table_lineage(w, config, arguments)
    elif name == "get_table_permissions":
        return await _get_table_permissions(w, config, arguments)
    elif name == "describe_table":
        return await _describe_table(w, config, arguments)
    elif name == "search_tables_by_tag":
        return await _search_tables_by_tag(w, config, arguments)
    elif name == "validate_ai_query":
        return await _validate_ai_query(w, config, arguments)
    elif name == "suggest_safe_tables":
        return await _suggest_safe_tables(w, config, arguments)
    elif name == "preview_data":
        return await _preview_data(w, config, arguments)
    elif name == "safe_columns":
        return await _safe_columns(w, config, arguments)
    elif name == "estimate_cost":
        return await _estimate_cost(w, config, arguments)
    elif name == "check_before_access":
        return await _check_before_access(w, config, arguments)
    elif name == "log_agent_action":
        return await _log_agent_action(w, config, arguments)
    elif name == "get_agent_compliance":
        return await _get_agent_compliance(w, config, arguments)
    elif name == "report_agent_execution":
        return await _report_agent_execution(w, config, arguments)
    raise ValueError(f"Unknown governance tool: {name}")


def _parse_table_name(table_name: str) -> tuple[str, str, str]:
    """Split catalog.schema.table into parts."""
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Table name must be fully qualified (catalog.schema.table), got: {table_name}"
        )
    return parts[0], parts[1], parts[2]


async def _get_table_lineage(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    table_name = args["table_name"]

    try:
        lineage = w.api_client.do(
            "GET",
            "/api/2.0/lineage-tracking/table-lineage",
            query={"table_name": table_name},
        )

        upstream = []
        for item in lineage.get("upstreams", []):
            table_info = item.get("tableInfo", {})
            upstream.append({
                "table": table_info.get("name"),
                "catalog": table_info.get("catalog_name"),
                "schema": table_info.get("schema_name"),
            })

        downstream = []
        for item in lineage.get("downstreams", []):
            table_info = item.get("tableInfo", {})
            downstream.append({
                "table": table_info.get("name"),
                "catalog": table_info.get("catalog_name"),
                "schema": table_info.get("schema_name"),
            })

        result = {
            "table": table_name,
            "upstream_count": len(upstream),
            "downstream_count": len(downstream),
            "upstream": upstream,
            "downstream": downstream,
        }
    except Exception as exc:
        result = {
            "table": table_name,
            "error": f"Lineage not available: {exc}",
        }

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_table_permissions(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    table_name = args["table_name"]

    try:
        perms = w.grants.get(securable_type="TABLE", full_name=table_name)

        grants = []
        for assignment in perms.privilege_assignments or []:
            grants.append({
                "principal": assignment.principal,
                "privileges": [str(p.privilege) for p in (assignment.privileges or [])],
            })

        result = {
            "table": table_name,
            "grant_count": len(grants),
            "grants": grants,
        }
    except Exception as exc:
        result = {
            "table": table_name,
            "error": f"Cannot read permissions: {exc}",
        }

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _describe_table(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    table_name = args["table_name"]
    include_tags = args.get("include_column_tags", True)

    try:
        table_info = w.tables.get(full_name=table_name)

        columns = []
        for col in table_info.columns or []:
            col_info: dict[str, Any] = {
                "name": col.name,
                "type": str(col.type_name) if col.type_name else None,
                "comment": col.comment,
                "nullable": col.nullable,
                "position": col.position,
            }
            if include_tags and col.comment:
                # Surface column-level tags from comments or properties
                pass  # Tags come from UC governed tags API in Phase 2
            columns.append(col_info)

        result: dict[str, Any] = {
            "table": table_name,
            "owner": table_info.owner,
            "table_type": str(table_info.table_type) if table_info.table_type else None,
            "data_source_format": str(table_info.data_source_format) if table_info.data_source_format else None,
            "comment": table_info.comment,
            "properties": dict(table_info.properties) if table_info.properties else {},
            "storage_location": table_info.storage_location,
            "column_count": len(columns),
            "columns": columns,
        }

        # Row count from properties if available
        props = table_info.properties or {}
        if "spark.sql.statistics.numRows" in props:
            result["row_count"] = int(props["spark.sql.statistics.numRows"])

    except Exception as exc:
        result = {"table": table_name, "error": str(exc)}

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _search_tables_by_tag(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    tag_name = args["tag_name"]
    tag_value = args.get("tag_value")
    catalog = args.get("catalog")

    # Use information_schema to find tagged tables
    where_parts = [f"tag_name = '{_esc(tag_name)}'"]
    if tag_value:
        where_parts.append(f"tag_value = '{_esc(tag_value)}'")

    where = " AND ".join(where_parts)

    if catalog:
        # Validate catalog name is a safe identifier (alphanumeric + underscores)
        import re as _re
        if not _re.match(r'^[\w-]+$', catalog):
            return [TextContent(type="text", text=json.dumps(
                {"error": f"Invalid catalog name: {catalog}"}, indent=2
            ))]
        query = f"""
            SELECT catalog_name, schema_name, table_name, tag_name, tag_value
            FROM `{catalog}`.information_schema.table_tags
            WHERE {where}
            ORDER BY schema_name, table_name
        """
    else:
        # Search system.information_schema for account-wide tags
        query = f"""
            SELECT catalog_name, schema_name, table_name, tag_name, tag_value
            FROM system.information_schema.table_tags
            WHERE {where}
            ORDER BY catalog_name, schema_name, table_name
            LIMIT 100
        """

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=query,
            wait_timeout="30s",
        )

        if response.status and response.status.state and response.status.state.value == "FAILED":
            error_msg = response.status.error.message if response.status.error else "Unknown"
            return [TextContent(type="text", text=json.dumps(
                {"error": error_msg, "tag": tag_name}, indent=2
            ))]

        columns = [c.name for c in response.manifest.schema.columns] if response.manifest else []
        rows = response.result.data_array if response.result else []
        records = [dict(zip(columns, r)) for r in rows]

        result = {
            "tag": tag_name,
            "tag_value": tag_value,
            "match_count": len(records),
            "tables": records,
        }
    except Exception as exc:
        result = {"tag": tag_name, "error": str(exc)}

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# --- Risk matrix for operation × classification combinations ---
# Higher numbers = higher risk. Operations that persist or externalize data
# (embed, train) are riskier than read-only queries.
_OPERATION_RISK: dict[str, int] = {
    "query": 1,
    "chat_context": 2,
    "embed": 3,
    "train": 4,
}

_CLASSIFICATION_RISK: dict[str, int] = {
    "public": 0,
    "internal": 1,
    "confidential": 2,
    "restricted": 3,
    "secret": 4,
}

# Tags that flag sensitive data categories
_SENSITIVE_TAGS = {"pii", "phi", "export_control", "itar", "ear"}

# Max classification level allowed per operation (combined risk < 7 = allowed)
# query(1)+restricted(3)=4 ok, embed(3)+confidential(2)=5 warning, train(4)+restricted(3)=7 blocked
_MAX_CLASSIFICATION: dict[str, str] = {
    "query": "restricted",       # risk 1 — can read almost anything
    "chat_context": "confidential",  # risk 2 — feeding to LLM
    "embed": "internal",         # risk 3 — persisting representations
    "train": "internal",         # risk 4 — baking into model weights
}


async def _validate_ai_query(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    tables = args["tables"]
    operation = args["operation"]
    purpose = args.get("purpose", "")
    op_risk = _OPERATION_RISK.get(operation, 2)

    if not tables:
        return [TextContent(type="text", text=json.dumps(
            {"verdict": "blocked", "reason": "No tables specified"}, indent=2
        ))]

    findings: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []

    for table_name in tables:
        finding: dict[str, Any] = {"table": table_name, "issues": []}

        # --- 1. Watchdog governance state (primary source) ---
        gov = get_resource_governance(w, config, table_name)
        finding["watchdog_available"] = gov.watchdog_available
        finding["ontology_classes"] = gov.classes

        # --- 2. Table metadata + classification tags (fallback) ---
        try:
            _parse_table_name(table_name)
            table_info = w.tables.get(full_name=table_name)
            finding["owner"] = table_info.owner
            props = table_info.properties or {}

            # Extract governance-relevant tags
            tag_classification = None
            sensitive_flags: list[str] = []
            for key, val in props.items():
                key_lower = key.lower()
                if key_lower in ("classification", "tag_classification"):
                    tag_classification = val.lower()
                if any(tag in key_lower for tag in _SENSITIVE_TAGS):
                    if val.lower() in ("true", "yes", "1"):
                        sensitive_flags.append(key)

            # Use Watchdog classification if available, fall back to tags
            if gov.watchdog_available and gov.classes:
                classification = gov.inferred_classification
                finding["classification_source"] = "watchdog"
            else:
                classification = tag_classification or "unclassified"
                finding["classification_source"] = "tags"

            finding["classification"] = classification
            finding["sensitive_flags"] = sensitive_flags

            # --- 3. Classification × operation risk ---
            class_risk = _CLASSIFICATION_RISK.get(classification, 1)
            combined_risk = op_risk + class_risk

            if combined_risk >= 7:
                msg = (
                    f"{table_name}: {operation} on {classification} data is blocked. "
                    f"This combination exceeds the risk threshold."
                )
                finding["issues"].append({"severity": "blocker", "message": msg})
                blockers.append(msg)
            elif combined_risk >= 5:
                msg = (
                    f"{table_name}: {operation} on {classification} data — "
                    f"high risk. Review classification and confirm necessity."
                )
                finding["issues"].append({"severity": "warning", "message": msg})
                warnings.append(msg)

            # --- 4. Ontology-aware checks (from Watchdog classifications) ---
            if gov.is_export_controlled and op_risk >= 2:
                msg = (
                    f"{table_name}: classified as export-controlled "
                    f"({', '.join(c for c in gov.classes if 'Export' in c or 'Itar' in c or 'Ear' in c)}). "
                    f"'{operation}' operations on export-controlled data require "
                    f"an approved exception."
                )
                finding["issues"].append({"severity": "blocker", "message": msg})
                blockers.append(msg)

            if gov.is_pii and operation == "train":
                msg = (
                    f"{table_name}: classified as PII by Watchdog. "
                    f"Model training must not use PII data without an approved exception."
                )
                finding["issues"].append({"severity": "blocker", "message": msg})
                blockers.append(msg)
            elif sensitive_flags and operation == "train":
                flag_str = ", ".join(sensitive_flags)
                msg = (
                    f"{table_name}: training on {flag_str} data is blocked. "
                    f"Model training must not use PII/PHI/export-controlled data "
                    f"without an approved exception."
                )
                finding["issues"].append({"severity": "blocker", "message": msg})
                blockers.append(msg)

            if sensitive_flags and op_risk >= 3 and operation != "train":
                flag_str = ", ".join(sensitive_flags)
                msg = (
                    f"{table_name}: contains {flag_str} — "
                    f"{operation} operations on sensitive data require justification."
                )
                finding["issues"].append({"severity": "warning", "message": msg})
                warnings.append(msg)

        except ValueError as exc:
            finding["issues"].append({"severity": "blocker", "message": str(exc)})
            blockers.append(str(exc))
        except Exception as exc:
            err = str(exc)
            if "PERMISSION_DENIED" in err or "ACCESS_DENIED" in err:
                msg = f"{table_name}: you do not have access to this table."
                finding["issues"].append({"severity": "blocker", "message": msg})
                blockers.append(msg)
            else:
                finding["issues"].append(
                    {"severity": "warning", "message": f"Metadata unavailable: {err}"}
                )
                warnings.append(f"{table_name}: could not read metadata — {err}")

        # --- 5. Watchdog violations ---
        if gov.open_violations:
            for v in gov.open_violations:
                sev = v.get("severity", "medium")
                policy = v.get("policy_name", v.get("policy_id", "unknown"))
                # Check if violation has an approved exception
                excepted = any(
                    e.get("policy_id") == v.get("policy_id")
                    for e in gov.active_exceptions
                )
                if excepted:
                    msg = (
                        f"{table_name}: {sev} violation ({policy}) — "
                        f"exception approved, proceeding."
                    )
                    finding["issues"].append({"severity": "info", "message": msg})
                elif sev in ("critical", "high"):
                    msg = (
                        f"{table_name}: open {sev} violation ({policy}). "
                        f"Resolve before using in AI workflows."
                    )
                    finding["issues"].append({"severity": "blocker", "message": msg})
                    blockers.append(msg)
                else:
                    msg = f"{table_name}: open {sev} violation ({policy})."
                    finding["issues"].append({"severity": "warning", "message": msg})
                    warnings.append(msg)

        # --- 6. Grant-related violations (access governance) ---
        grant_violations = gov.grant_violations
        if grant_violations:
            policy_ids = [v.get("policy_id", "") for v in grant_violations]
            msg = (
                f"{table_name}: {len(grant_violations)} open access governance "
                f"violation(s) ({', '.join(policy_ids)}). "
                f"Review grant hygiene before using in AI workflows."
            )
            finding["issues"].append({"severity": "warning", "message": msg})
            warnings.append(msg)
            finding["grant_violations"] = grant_violations

        findings.append(finding)

    # --- Compute verdict ---
    if blockers:
        verdict = "blocked"
    elif warnings:
        verdict = "warning"
    else:
        verdict = "proceed"

    result: dict[str, Any] = {
        "verdict": verdict,
        "operation": operation,
        "purpose": purpose,
        "tables_checked": len(tables),
        "blockers": blockers,
        "warnings": warnings,
        "findings": findings,
    }

    if verdict == "blocked":
        max_class = _MAX_CLASSIFICATION.get(operation, "internal")
        # Find alternative tables in the same schemas
        blocked_schemas = set()
        for t in tables:
            try:
                cat, sch, _ = _parse_table_name(t)
                blocked_schemas.add(f"{cat}.{sch}")
            except ValueError:
                pass

        alternatives = []
        for schema_fqn in blocked_schemas:
            try:
                alts = await _find_safe_tables_in_schema(
                    w, config, schema_fqn, operation, limit=5
                )
                alternatives.extend(alts)
            except Exception:
                pass

        result["alternatives"] = alternatives
        result["guidance"] = (
            f"Blocked. For '{operation}', max classification is '{max_class}'. "
            f"Options: (1) use suggest_safe_tables to find compatible data, "
            f"(2) use a lower-risk operation (e.g. 'query' instead of '{operation}'), "
            f"(3) request a governance exception via Watchdog MCP (get_exceptions)."
        )
        if alternatives:
            alt_names = [a["table"] for a in alternatives[:3]]
            result["guidance"] += (
                f" Possible alternatives in the same schema: {', '.join(alt_names)}"
            )
    elif verdict == "warning":
        result["guidance"] = (
            "Passed with warnings — review before proceeding. "
            "Your data access is audit-logged."
        )
    else:
        result["guidance"] = "All tables cleared. Proceed."

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# ---------------------------------------------------------------------------
# Helpers for finding safe alternatives
# ---------------------------------------------------------------------------

async def _find_safe_tables_in_schema(
    w: WorkspaceClient,
    config: AiDevkitConfig,
    schema_fqn: str,
    operation: str,
    limit: int = 5,
    keyword: str | None = None,
) -> list[dict[str, Any]]:
    """Find tables in a schema that are safe for the given operation."""
    op_risk = _OPERATION_RISK.get(operation, 2)

    cat, sch = schema_fqn.split(".", 1)
    tables_list = w.tables.list(catalog_name=cat, schema_name=sch)

    safe: list[dict[str, Any]] = []
    for tbl in tables_list:
        if keyword and keyword.lower() not in (tbl.name or "").lower():
            if keyword.lower() not in (tbl.comment or "").lower():
                continue

        props = tbl.properties or {}
        classification = None
        sensitive_flags = []
        for key, val in props.items():
            key_lower = key.lower()
            if key_lower in ("classification", "tag_classification"):
                classification = val.lower()
            if any(tag in key_lower for tag in _SENSITIVE_TAGS):
                if val.lower() in ("true", "yes", "1"):
                    sensitive_flags.append(key)

        class_risk = _CLASSIFICATION_RISK.get(classification or "internal", 1)
        combined = op_risk + class_risk

        if combined >= 7:
            continue
        if sensitive_flags and operation == "train":
            continue

        safe.append({
            "table": f"{cat}.{sch}.{tbl.name}",
            "classification": classification or "unclassified",
            "sensitive_flags": sensitive_flags,
            "risk_score": combined,
            "comment": tbl.comment,
        })

        if len(safe) >= limit:
            break

    safe.sort(key=lambda x: x["risk_score"])
    return safe


async def _suggest_safe_tables(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    operation = args["operation"]
    schema_name = args.get("schema_name")
    keyword = args.get("keyword")
    limit = args.get("limit", 20)

    max_class = _MAX_CLASSIFICATION.get(operation, "internal")

    if schema_name:
        schemas_to_search = [schema_name]
    else:
        default_cat = config.catalog
        try:
            schema_list = w.schemas.list(catalog_name=default_cat)
            schemas_to_search = [
                f"{default_cat}.{s.name}" for s in schema_list
                if s.name != "information_schema"
            ]
        except Exception as exc:
            return [TextContent(type="text", text=json.dumps(
                {"error": f"Cannot list schemas in {default_cat}: {exc}"}, indent=2
            ))]

    all_safe: list[dict[str, Any]] = []
    for schema_fqn in schemas_to_search:
        try:
            safe = await _find_safe_tables_in_schema(
                w, config, schema_fqn, operation,
                limit=limit - len(all_safe), keyword=keyword,
            )
            all_safe.extend(safe)
        except Exception:
            continue
        if len(all_safe) >= limit:
            break

    result: dict[str, Any] = {
        "operation": operation,
        "max_classification": max_class,
        "keyword": keyword,
        "tables_found": len(all_safe),
        "tables": all_safe[:limit],
    }

    if not all_safe:
        result["guidance"] = (
            f"No tables found safe for '{operation}'"
            + (f" matching '{keyword}'" if keyword else "")
            + f". Max classification for this operation is '{max_class}'. "
            f"Try a broader search or a lower-risk operation."
        )
    else:
        result["guidance"] = (
            f"Found {len(all_safe)} table(s) safe for '{operation}'. "
            f"Sorted by risk score (lower = safer)."
        )

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# ---------------------------------------------------------------------------
# preview_data — peek at actual rows
# ---------------------------------------------------------------------------

async def _preview_data(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    table_name = args["table_name"]
    columns = args.get("columns")
    limit = min(args.get("limit", 10), 50)
    where = args.get("where")

    # Validate table_name is a proper 3-part identifier
    _parse_table_name(table_name)  # raises ValueError if malformed

    col_clause = ", ".join(f"`{_esc(c)}`" for c in columns) if columns else "*"

    # Safety: reject write operations in WHERE clause
    if where:
        import re as _re
        where_upper = where.strip().upper()
        dangerous = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
                     "MERGE", "TRUNCATE", "GRANT", "REVOKE"}
        where_tokens = set(_re.findall(r'\b[A-Z_]+\b', where_upper))
        if where_tokens & dangerous:
            return [TextContent(type="text", text=json.dumps(
                {"error": "WHERE clause contains forbidden keywords."}, indent=2
            ))]
        if ';' in where:
            return [TextContent(type="text", text=json.dumps(
                {"error": "WHERE clause must not contain semicolons."}, indent=2
            ))]

    where_clause = f"WHERE {where}" if where else ""

    query = f"SELECT {col_clause} FROM `{_esc(table_name)}` {where_clause} LIMIT {limit}"

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=query,
            wait_timeout="30s",
        )

        if response.status and response.status.state and response.status.state.value == "FAILED":
            error_msg = response.status.error.message if response.status.error else "Unknown"
            return [TextContent(type="text", text=json.dumps(
                {"table": table_name, "error": error_msg}, indent=2
            ))]

        col_names = [c.name for c in response.manifest.schema.columns] if response.manifest else []
        rows = response.result.data_array if response.result else []
        records = [dict(zip(col_names, r)) for r in rows]

        result = {
            "table": table_name,
            "columns": col_names,
            "row_count": len(records),
            "rows": records,
        }
    except Exception as exc:
        result = {"table": table_name, "error": str(exc)}

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# ---------------------------------------------------------------------------
# safe_columns — column-level governance for partially restricted tables
# ---------------------------------------------------------------------------

async def _safe_columns(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    table_name = args["table_name"]
    operation = args["operation"]
    op_risk = _OPERATION_RISK.get(operation, 2)

    try:
        table_info = w.tables.get(full_name=table_name)
    except Exception as exc:
        return [TextContent(type="text", text=json.dumps(
            {"table": table_name, "error": str(exc)}, indent=2
        ))]

    safe: list[dict[str, Any]] = []
    warning: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []

    # Name-based heuristics for sensitive columns — extend via config
    pii_patterns = {
        "ssn", "social_security", "tax_id", "passport", "drivers_license",
        "date_of_birth", "dob", "home_address", "personal_email",
        "phone_number", "mobile", "salary", "compensation",
    }
    phi_patterns = {
        "diagnosis", "treatment", "medical", "patient_id", "health_record",
        "prescription", "dosage",
    }
    export_patterns = {
        "itar", "ear", "export_class",
    }
    # Add any custom sensitive patterns from config
    if hasattr(config, "extra_sensitive_patterns"):
        for category, patterns in (config.extra_sensitive_patterns or {}).items():
            if category == "pii":
                pii_patterns.update(patterns)
            elif category == "phi":
                phi_patterns.update(patterns)
            elif category == "export":
                export_patterns.update(patterns)

    for col in table_info.columns or []:
        col_entry: dict[str, Any] = {
            "name": col.name,
            "type": str(col.type_name) if col.type_name else None,
        }

        name_lower = (col.name or "").lower()
        comment_lower = (col.comment or "").lower()

        sensitive_matches = []
        for pattern in pii_patterns:
            if pattern in name_lower or pattern in comment_lower:
                sensitive_matches.append(f"pii:{pattern}")
        for pattern in phi_patterns:
            if pattern in name_lower or pattern in comment_lower:
                sensitive_matches.append(f"phi:{pattern}")
        for pattern in export_patterns:
            if pattern in name_lower or pattern in comment_lower:
                sensitive_matches.append(f"export:{pattern}")

        col_entry["sensitive_matches"] = sensitive_matches

        if sensitive_matches and operation == "train":
            col_entry["reason"] = "Sensitive column blocked for model training"
            blocked.append(col_entry)
        elif sensitive_matches and op_risk >= 3:
            col_entry["reason"] = "Sensitive — use with caution for this operation"
            warning.append(col_entry)
        elif sensitive_matches:
            col_entry["reason"] = "Contains sensitive data but allowed for read queries"
            warning.append(col_entry)
        else:
            safe.append(col_entry)

    safe_names = [c["name"] for c in safe]

    result: dict[str, Any] = {
        "table": table_name,
        "operation": operation,
        "total_columns": len(safe) + len(warning) + len(blocked),
        "safe_columns": safe,
        "warning_columns": warning,
        "blocked_columns": blocked,
        "safe_column_list": safe_names,
    }

    if blocked:
        result["guidance"] = (
            f"{len(blocked)} column(s) blocked for '{operation}'. "
            f"Use safe_column_list to build your SELECT: "
            f"SELECT {', '.join(safe_names[:5])}{'...' if len(safe_names) > 5 else ''} "
            f"FROM {table_name}"
        )
    elif warning:
        result["guidance"] = (
            f"All columns usable but {len(warning)} have sensitive data. "
            f"Review warning_columns before including in AI workflows."
        )
    else:
        result["guidance"] = "All columns safe. No restrictions."

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# ---------------------------------------------------------------------------
# estimate_cost — DBU cost estimation before expensive operations
# ---------------------------------------------------------------------------

# Rough token-per-row estimates by column type
_TOKENS_PER_VALUE: dict[str, float] = {
    "STRING": 15.0,
    "LONG": 2.0,
    "INT": 2.0,
    "DOUBLE": 2.0,
    "FLOAT": 2.0,
    "BOOLEAN": 1.0,
    "DATE": 3.0,
    "TIMESTAMP": 4.0,
    "BINARY": 20.0,
}

# DBU cost per 1M tokens by operation (approximate, Foundation Model API pricing)
_DBU_PER_MILLION_TOKENS: dict[str, float] = {
    "embed": 0.5,
    "chat_context": 5.0,
    "train": 15.0,
    "query": 0.0,
}


async def _estimate_cost(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    table_name = args["table_name"]
    operation = args["operation"]
    selected_columns = args.get("columns")
    row_limit = args.get("row_limit")

    try:
        table_info = w.tables.get(full_name=table_name)
    except Exception as exc:
        return [TextContent(type="text", text=json.dumps(
            {"table": table_name, "error": str(exc)}, indent=2
        ))]

    # Get row count
    props = table_info.properties or {}
    row_count = int(props.get("spark.sql.statistics.numRows", 0))
    if row_count == 0:
        try:
            response = w.statement_execution.execute_statement(
                warehouse_id=config.warehouse_id,
                statement=f"SELECT COUNT(*) FROM {table_name}",
                wait_timeout="15s",
            )
            if response.result and response.result.data_array:
                row_count = int(response.result.data_array[0][0])
        except Exception:
            pass

    if row_limit:
        row_count = min(row_count, row_limit)

    # Estimate tokens per row
    columns = table_info.columns or []
    if selected_columns:
        columns = [c for c in columns if c.name in selected_columns]

    tokens_per_row = 0.0
    for col in columns:
        type_str = str(col.type_name).upper() if col.type_name else "STRING"
        tokens_per_row += _TOKENS_PER_VALUE.get(type_str, 10.0)

    total_tokens = row_count * tokens_per_row
    dbu_rate = _DBU_PER_MILLION_TOKENS.get(operation, 1.0)
    estimated_dbus = (total_tokens / 1_000_000) * dbu_rate

    result: dict[str, Any] = {
        "table": table_name,
        "operation": operation,
        "row_count": row_count,
        "column_count": len(columns),
        "estimated_tokens_per_row": round(tokens_per_row, 1),
        "estimated_total_tokens": int(total_tokens),
        "estimated_dbus": round(estimated_dbus, 2),
    }

    if operation == "query":
        result["guidance"] = "SQL queries are billed by warehouse compute, not tokens."
    elif estimated_dbus < 1:
        result["guidance"] = f"Low cost (~{estimated_dbus:.2f} DBUs). Proceed."
    elif estimated_dbus < 10:
        result["guidance"] = f"Moderate cost (~{estimated_dbus:.1f} DBUs). Reasonable for most use cases."
    elif estimated_dbus < 100:
        result["guidance"] = (
            f"Significant cost (~{estimated_dbus:.0f} DBUs). "
            f"Consider limiting rows or columns to reduce cost."
        )
    else:
        result["guidance"] = (
            f"High cost (~{estimated_dbus:.0f} DBUs). "
            f"Strongly recommend sampling (row_limit) or selecting fewer columns. "
            f"At {row_count:,} rows x {len(columns)} columns, this is a large operation."
        )

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# ---------------------------------------------------------------------------
# Runtime governance tools (Phase 5D) — agent-time checks and audit
# ---------------------------------------------------------------------------


def _init_agent_session(agent_id: str) -> dict:
    """Initialize or return an existing agent session."""
    if agent_id not in _agent_sessions:
        _agent_sessions[agent_id] = {
            "agent_id": agent_id,
            "checks_passed": 0,
            "checks_denied": 0,
            "checks_warned": 0,
            "tables_accessed": [],
            "pii_tables_accessed": [],
            "classifications_seen": set(),
            "actions_logged": 0,
            "risk_level": "low",
            "session_start": datetime.now(timezone.utc).isoformat(),
        }
    return _agent_sessions[agent_id]


def _calculate_risk_level(session: dict) -> str:
    """Calculate risk level from session state.

    - critical: accessed PII with denied checks, or exported data without approval
    - high: accessed PII with warnings, or has denied checks
    - medium: warnings but no denials
    - low: all checks passed
    """
    has_pii = len(session.get("pii_tables_accessed", [])) > 0
    denied = session.get("checks_denied", 0)
    warned = session.get("checks_warned", 0)

    if has_pii and denied > 0:
        return "critical"
    if has_pii and warned > 0:
        return "high"
    if denied > 0:
        return "high"
    if warned > 0:
        return "medium"
    return "low"


async def _check_before_access(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    agent_id = args["agent_id"]
    table = args["table"]
    operation = args.get("operation", "SELECT")
    columns = args.get("columns")

    # Initialize session tracking
    session = _init_agent_session(agent_id)

    # Look up governance state
    gov = get_resource_governance(w, config, table)

    reasons: list[str] = []
    alternatives: list[str] = []
    decision = "allow"

    # Check for critical violations
    if gov.has_critical_violations:
        reasons.append(
            f"Table has {sum(1 for v in gov.open_violations if v.get('severity') == 'critical')} "
            f"critical violation(s)."
        )
        decision = "deny"
        # Suggest contacting owner
        owner = None
        try:
            table_info = w.tables.get(full_name=table)
            owner = table_info.owner
        except Exception:
            pass
        if owner:
            alternatives.append(f"Resolve violations first, contact {owner}.")
        else:
            alternatives.append("Resolve critical violations before access.")

    # Check for PII + ungoverned agent
    if gov.is_pii:
        reasons.append("Table is classified as PII.")
        decision = "deny"
        # Suggest masked view
        try:
            cat, sch, tbl = _parse_table_name(table)
            masked_view = f"{cat}.{sch}.{tbl}_masked"
            alternatives.append(f"Use masked view if available: {masked_view}")
        except ValueError:
            pass

    # Check for restricted classification
    if gov.is_restricted and decision != "deny":
        reasons.append("Table is classified as restricted.")
        decision = "deny"
        alternatives.append("Request a governance exception via Watchdog.")

    # Check for high violations (warn)
    if gov.has_high_violations and decision != "deny":
        reasons.append(
            f"Table has {sum(1 for v in gov.open_violations if v.get('severity') == 'high')} "
            f"open high-severity violation(s)."
        )
        if decision != "deny":
            decision = "warn"

    # Check for confidential + sensitive columns (warn)
    if gov.is_confidential and columns and decision != "deny":
        pii_patterns = {
            "ssn", "social_security", "tax_id", "passport", "drivers_license",
            "date_of_birth", "dob", "home_address", "personal_email",
            "phone_number", "mobile", "salary", "compensation",
        }
        sensitive_cols = [
            c for c in columns
            if any(p in c.lower() for p in pii_patterns)
        ]
        if sensitive_cols:
            reasons.append(
                f"Confidential table with sensitive column(s): {', '.join(sensitive_cols)}."
            )
            if decision != "deny":
                decision = "warn"

    # Update session state
    if decision == "allow":
        session["checks_passed"] += 1
    elif decision == "deny":
        session["checks_denied"] += 1
    elif decision == "warn":
        session["checks_warned"] += 1

    if table not in session["tables_accessed"]:
        session["tables_accessed"].append(table)
    if gov.is_pii and table not in session["pii_tables_accessed"]:
        session["pii_tables_accessed"].append(table)
    for cls in gov.classes:
        session["classifications_seen"].add(cls)

    session["risk_level"] = _calculate_risk_level(session)

    result = {
        "decision": decision,
        "table": table,
        "agent_id": agent_id,
        "operation": operation,
        "classifications": gov.classes,
        "open_violations": len(gov.open_violations),
        "critical_violations": gov.has_critical_violations,
        "reasons": reasons,
        "alternatives": alternatives,
        "policies_checked": len(gov.policies_applied),
    }

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _log_agent_action(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    agent_id = args["agent_id"]
    action = args["action"]
    target = args["target"]
    details = args.get("details", {})
    classification = args.get("classification")

    event_id = str(uuid.uuid4())

    # Update session state
    session = _init_agent_session(agent_id)
    session["actions_logged"] += 1

    # Emit structured audit event
    audit_event = {
        "event_id": event_id,
        "event_type": "agent_action",
        "agent_id": agent_id,
        "action": action,
        "target": target,
        "details": details,
        "classification": classification,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(json.dumps(audit_event, default=str))

    result = {
        "status": "logged",
        "event_id": event_id,
        "agent_id": agent_id,
        "action": action,
        "target": target,
    }

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_agent_compliance(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    agent_id = args["agent_id"]

    session = _init_agent_session(agent_id)

    result = {
        "agent_id": agent_id,
        "checks_passed": session["checks_passed"],
        "checks_denied": session["checks_denied"],
        "checks_warned": session["checks_warned"],
        "tables_accessed": session["tables_accessed"],
        "pii_tables_accessed": session["pii_tables_accessed"],
        "classifications_seen": list(session["classifications_seen"]),
        "actions_logged": session["actions_logged"],
        "risk_level": session["risk_level"],
        "session_start": session["session_start"],
    }

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _report_agent_execution(
    w: WorkspaceClient, config: AiDevkitConfig, args: dict[str, Any]
) -> list[TextContent]:
    agent_id = args["agent_id"]
    execution_summary = args.get("execution_summary", "")

    session = _agent_sessions.get(agent_id)
    if session is None:
        # No session tracked — return minimal report
        result = {
            "agent_id": agent_id,
            "execution_summary": execution_summary,
            "compliance_status": "compliant",
            "risk_level": "low",
            "governance_checks": {"total": 0, "passed": 0, "denied": 0, "warned": 0},
            "data_access": {
                "tables_accessed": [],
                "pii_tables_accessed": [],
                "classifications_seen": [],
            },
            "actions_logged": 0,
            "policy_violations": [],
            "recommendations": ["No governance activity recorded for this agent."],
        }
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    passed = session["checks_passed"]
    denied = session["checks_denied"]
    warned = session["checks_warned"]
    risk_level = session["risk_level"]

    # Determine compliance status
    if denied > 0 or risk_level == "critical":
        compliance_status = "non_compliant"
    elif warned > 0:
        compliance_status = "needs_review"
    else:
        compliance_status = "compliant"

    # Calculate duration
    try:
        start = datetime.fromisoformat(session["session_start"])
        duration_seconds = (datetime.now(timezone.utc) - start).total_seconds()
        duration = f"{duration_seconds:.1f}s"
    except Exception:
        duration = "unknown"

    # Build recommendations
    recommendations: list[str] = []
    if denied > 0:
        recommendations.append(
            f"{denied} access check(s) were denied. Review denied tables and "
            f"resolve governance violations before re-running."
        )
    if session["pii_tables_accessed"]:
        recommendations.append(
            f"PII tables accessed: {', '.join(session['pii_tables_accessed'])}. "
            f"Ensure data handling complies with privacy policies."
        )
    if risk_level in ("high", "critical"):
        recommendations.append(
            "High/critical risk level detected. Consider adding audit logging "
            "and governance metadata to this agent."
        )
    if not recommendations:
        recommendations.append("Agent execution was fully compliant. No action needed.")

    report = {
        "agent_id": agent_id,
        "execution_summary": execution_summary,
        "compliance_status": compliance_status,
        "risk_level": risk_level,
        "duration": duration,
        "governance_checks": {
            "total": passed + denied + warned,
            "passed": passed,
            "denied": denied,
            "warned": warned,
        },
        "data_access": {
            "tables_accessed": session["tables_accessed"],
            "pii_tables_accessed": session["pii_tables_accessed"],
            "classifications_seen": list(session["classifications_seen"]),
        },
        "actions_logged": session["actions_logged"],
        "policy_violations": [],
        "recommendations": recommendations,
    }

    # Clear session state after report
    del _agent_sessions[agent_id]

    return [TextContent(type="text", text=json.dumps(report, indent=2, default=str))]
