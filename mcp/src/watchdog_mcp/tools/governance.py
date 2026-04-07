"""Watchdog governance tools — violations, policies, scan operations.

Primary tools for the Watchdog MCP server. Expose governance state
tracked in platform.watchdog Delta tables. Runs as the calling user's
identity (on-behalf-of) — UC grants on platform.watchdog govern access.
"""

import json
import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from mcp.types import TextContent, Tool

from watchdog_mcp.config import WatchdogMcpConfig

logger = logging.getLogger(__name__)

TOOLS = [
    Tool(
        name="get_violations",
        description=(
            "Query open governance violations. Filter by severity, resource type, "
            "policy, or owner. Returns violations from the Watchdog violations table "
            "with full context (resource, policy, first/last detected, status)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["open", "resolved", "exception"],
                    "description": "Violation status filter. Default: open.",
                },
                "severity": {
                    "type": "string",
                    "enum": ["critical", "high", "medium", "low"],
                    "description": "Filter by severity level.",
                },
                "resource_type": {
                    "type": "string",
                    "description": "Filter by resource type (e.g. 'cluster', 'table', 'endpoint').",
                },
                "policy_id": {
                    "type": "string",
                    "description": "Filter by specific policy ID.",
                },
                "owner": {
                    "type": "string",
                    "description": "Filter by resource owner.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results. Default: 50.",
                },
            },
        },
    ),
    Tool(
        name="get_governance_summary",
        description=(
            "Get a high-level summary of the current governance state: "
            "total open violations by severity, recent trends, top offending "
            "resource types, and coverage metrics."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_policies",
        description=(
            "List all governance policies with their status, severity, "
            "description, and last evaluation results."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "active_only": {
                    "type": "boolean",
                    "description": "Only show active policies. Default: true.",
                },
            },
        },
    ),
    Tool(
        name="get_scan_history",
        description=(
            "View recent Watchdog scan results — when scans ran, how many "
            "resources were evaluated, violations found, and resolved."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of recent scans. Default: 10.",
                },
            },
        },
    ),
    Tool(
        name="get_resource_violations",
        description=(
            "Get all violations (open, resolved, exception) for a specific "
            "resource by its resource_id. Shows the full compliance history."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "resource_id": {
                    "type": "string",
                    "description": "The resource identifier to look up.",
                },
            },
            "required": ["resource_id"],
        },
    ),
    Tool(
        name="get_exceptions",
        description=(
            "List approved governance exceptions (waivers). Shows which "
            "violations have been explicitly accepted, by whom, and until when."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "active_only": {
                    "type": "boolean",
                    "description": "Only show non-expired exceptions. Default: true.",
                },
            },
        },
    ),
]


async def handle(
    name: str,
    arguments: dict[str, Any],
    w: WorkspaceClient,
    config: WatchdogMcpConfig,
) -> list[TextContent]:
    if name == "get_violations":
        return await _get_violations(w, config, arguments)
    elif name == "get_governance_summary":
        return await _get_governance_summary(w, config, arguments)
    elif name == "get_policies":
        return await _get_policies(w, config, arguments)
    elif name == "get_scan_history":
        return await _get_scan_history(w, config, arguments)
    elif name == "get_resource_violations":
        return await _get_resource_violations(w, config, arguments)
    elif name == "get_exceptions":
        return await _get_exceptions(w, config, arguments)
    raise ValueError(f"Unknown governance tool: {name}")


def _execute_sql(
    w: WorkspaceClient, config: WatchdogMcpConfig, query: str
) -> dict[str, Any]:
    """Execute SQL and return structured result."""
    response = w.statement_execution.execute_statement(
        warehouse_id=config.warehouse_id,
        statement=query,
        catalog=config.catalog,
        schema=config.schema,
        wait_timeout="30s",
    )
    if response.status and response.status.state and response.status.state.value == "FAILED":
        error_msg = response.status.error.message if response.status.error else "Unknown"
        return {"error": error_msg, "rows": [], "columns": []}

    columns = [c.name for c in response.manifest.schema.columns] if response.manifest else []
    rows = response.result.data_array if response.result else []
    return {"columns": columns, "rows": [dict(zip(columns, r)) for r in rows]}


async def _get_violations(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    status = args.get("status", "open")
    limit = args.get("limit", 50)
    qs = config.qualified_schema

    where_clauses = [f"status = '{status}'"]
    if args.get("severity"):
        where_clauses.append(f"severity = '{args['severity']}'")
    if args.get("resource_type"):
        where_clauses.append(f"resource_type = '{args['resource_type']}'")
    if args.get("policy_id"):
        where_clauses.append(f"policy_id = '{args['policy_id']}'")
    if args.get("owner"):
        where_clauses.append(f"owner = '{args['owner']}'")

    where = " AND ".join(where_clauses)
    query = f"""
        SELECT resource_id, resource_type, policy_id, severity, status,
               owner, first_detected, last_detected, message
        FROM {qs}.violations
        WHERE {where}
        ORDER BY
            CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                          WHEN 'medium' THEN 3 ELSE 4 END,
            last_detected DESC
        LIMIT {limit}
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_governance_summary(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema

    summary_query = f"""
        SELECT
            COUNT(*) FILTER (WHERE status = 'open') as open_violations,
            COUNT(*) FILTER (WHERE status = 'resolved') as resolved_violations,
            COUNT(*) FILTER (WHERE status = 'exception') as exceptions,
            COUNT(*) FILTER (WHERE status = 'open' AND severity = 'critical') as critical_open,
            COUNT(*) FILTER (WHERE status = 'open' AND severity = 'high') as high_open,
            COUNT(*) FILTER (WHERE status = 'open' AND severity = 'medium') as medium_open,
            COUNT(*) FILTER (WHERE status = 'open' AND severity = 'low') as low_open
        FROM {qs}.violations
    """

    by_type_query = f"""
        SELECT resource_type, COUNT(*) as count
        FROM {qs}.violations
        WHERE status = 'open'
        GROUP BY resource_type
        ORDER BY count DESC
    """

    by_policy_query = f"""
        SELECT policy_id, severity, COUNT(*) as count
        FROM {qs}.violations
        WHERE status = 'open'
        GROUP BY policy_id, severity
        ORDER BY count DESC
        LIMIT 10
    """

    summary = _execute_sql(w, config, summary_query)
    by_type = _execute_sql(w, config, by_type_query)
    by_policy = _execute_sql(w, config, by_policy_query)

    result = {
        "summary": summary["rows"][0] if summary["rows"] else {},
        "by_resource_type": by_type["rows"],
        "top_policies": by_policy["rows"],
    }
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_policies(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    active_only = args.get("active_only", True)
    where = "WHERE active = true" if active_only else ""

    query = f"""
        SELECT policy_id, name, description, severity, resource_type,
               active, last_updated
        FROM {qs}.policies
        {where}
        ORDER BY severity, policy_id
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_scan_history(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    limit = args.get("limit", 10)

    query = f"""
        SELECT scan_id, scan_timestamp, resources_scanned,
               violations_found, violations_resolved, duration_seconds
        FROM {qs}.scan_results
        ORDER BY scan_timestamp DESC
        LIMIT {limit}
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_resource_violations(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    resource_id = args["resource_id"]

    query = f"""
        SELECT policy_id, severity, status, first_detected, last_detected,
               resolved_at, message
        FROM {qs}.violations
        WHERE resource_id = '{resource_id}'
        ORDER BY first_detected DESC
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_exceptions(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    active_only = args.get("active_only", True)
    where = "WHERE expires_at > current_timestamp() OR expires_at IS NULL" if active_only else ""

    query = f"""
        SELECT resource_id, policy_id, approved_by, approved_at,
               expires_at, reason
        FROM {qs}.exceptions
        {where}
        ORDER BY approved_at DESC
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
