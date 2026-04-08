"""Watchdog governance tools — violations, policies, scan operations.

Primary tools for the Watchdog MCP server. Expose governance state
tracked in platform.watchdog Delta tables. Runs as the calling user's
identity (on-behalf-of) — UC grants on platform.watchdog govern access.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from mcp.types import TextContent, Tool

from watchdog_mcp.config import WatchdogMcpConfig

logger = logging.getLogger(__name__)

# Shared property for optional metastore filtering
_METASTORE_PROP = {
    "metastore": {
        "type": "string",
        "description": "Filter to a specific metastore ID. Omit for all metastores.",
    },
}

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
                **_METASTORE_PROP,
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
        inputSchema={"type": "object", "properties": {**_METASTORE_PROP}},
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
                **_METASTORE_PROP,
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
                **_METASTORE_PROP,
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
                **_METASTORE_PROP,
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
                **_METASTORE_PROP,
            },
        },
    ),
    Tool(
        name="explain_violation",
        description=(
            "Explain a governance violation in plain language. Provides context on "
            "what the violation means, why the policy exists, the resource's current "
            "state, and step-by-step remediation guidance. Accepts either a "
            "violation_id or a resource_id + policy_id pair."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "violation_id": {
                    "type": "string",
                    "description": "The violation UUID to explain.",
                },
                "resource_id": {
                    "type": "string",
                    "description": "Resource ID (alternative to violation_id).",
                },
                "policy_id": {
                    "type": "string",
                    "description": "Policy ID (used with resource_id).",
                },
                **_METASTORE_PROP,
            },
        },
    ),
    Tool(
        name="what_if_policy",
        description=(
            "Simulate a proposed governance policy against the current resource "
            "inventory. Shows which resources would be in violation if the policy "
            "were activated, without actually creating violations. Useful for "
            "impact analysis before adding new policies."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "applies_to": {
                    "type": "string",
                    "description": "Ontology class the policy targets (e.g. 'DataAsset', 'PiiAsset', '*'). Default: '*'.",
                },
                "rule_type": {
                    "type": "string",
                    "enum": ["tag_exists", "tag_equals", "tag_in", "metadata_equals", "metadata_not_empty"],
                    "description": "Type of rule to simulate.",
                },
                "rule_key": {
                    "type": "string",
                    "description": "Tag key or metadata field to check.",
                },
                "rule_value": {
                    "type": "string",
                    "description": "Expected value (for tag_equals, tag_in, metadata_equals). Comma-separated for tag_in.",
                },
                "severity": {
                    "type": "string",
                    "enum": ["critical", "high", "medium", "low"],
                    "description": "Severity of the proposed policy. Default: medium.",
                },
                **_METASTORE_PROP,
            },
            "required": ["rule_type", "rule_key"],
        },
    ),
    Tool(
        name="list_metastores",
        description="List all metastores Watchdog has scanned with latest scan timestamp and resource count.",
        inputSchema={"type": "object", "properties": {}},
    ),
]


def _resolve_metastore(args: dict, config: WatchdogMcpConfig) -> str:
    """Get metastore filter: explicit arg > config default > empty (no filter)."""
    return args.get("metastore") or config.default_metastore_id or ""


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
    elif name == "explain_violation":
        return await _explain_violation(w, config, arguments)
    elif name == "what_if_policy":
        return await _what_if_policy(w, config, arguments)
    elif name == "list_metastores":
        return await _list_metastores(w, config, arguments)
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
    metastore = _resolve_metastore(args, config)

    where_clauses = [f"status = '{status}'"]
    if metastore:
        where_clauses.append(f"metastore_id = '{metastore}'")
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
    metastore = _resolve_metastore(args, config)
    ms_where = f"WHERE metastore_id = '{metastore}'" if metastore else ""
    ms_and = f"AND metastore_id = '{metastore}'" if metastore else ""

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
        {ms_where}
    """

    by_type_query = f"""
        SELECT resource_type, COUNT(*) as count
        FROM {qs}.violations
        WHERE status = 'open'
        {ms_and}
        GROUP BY resource_type
        ORDER BY count DESC
    """

    by_policy_query = f"""
        SELECT policy_id, severity, COUNT(*) as count
        FROM {qs}.violations
        WHERE status = 'open'
        {ms_and}
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
    metastore = _resolve_metastore(args, config)
    active_only = args.get("active_only", True)
    conditions = []
    if active_only:
        conditions.append("active = true")
    if metastore:
        conditions.append(f"metastore_id = '{metastore}'")
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

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
    metastore = _resolve_metastore(args, config)
    limit = args.get("limit", 10)
    ms_where = f"WHERE metastore_id = '{metastore}'" if metastore else ""

    query = f"""
        SELECT scan_id, scan_timestamp, resources_scanned,
               violations_found, violations_resolved, duration_seconds
        FROM {qs}.scan_results
        {ms_where}
        ORDER BY scan_timestamp DESC
        LIMIT {limit}
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_resource_violations(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    metastore = _resolve_metastore(args, config)
    resource_id = args["resource_id"]
    ms_and = f"AND metastore_id = '{metastore}'" if metastore else ""

    query = f"""
        SELECT policy_id, severity, status, first_detected, last_detected,
               resolved_at, message
        FROM {qs}.violations
        WHERE resource_id = '{resource_id}'
        {ms_and}
        ORDER BY first_detected DESC
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_exceptions(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    metastore = _resolve_metastore(args, config)
    active_only = args.get("active_only", True)
    conditions = []
    if active_only:
        conditions.append("(expires_at > current_timestamp() OR expires_at IS NULL)")
    if metastore:
        conditions.append(f"metastore_id = '{metastore}'")
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = f"""
        SELECT resource_id, policy_id, approved_by, approved_at,
               expires_at, reason
        FROM {qs}.exceptions
        {where}
        ORDER BY approved_at DESC
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


def _build_remediation_steps(
    violation: dict[str, Any],
    policy: dict[str, Any],
    tags: dict[str, Any] | None,
) -> list[str]:
    """Generate specific actionable remediation steps based on the policy type."""
    steps: list[str] = []
    resource_type = violation.get("resource_type", "resource")
    resource_name = violation.get("resource_name", violation.get("resource_id", "unknown"))
    rule_json = policy.get("rule_json")
    policy_id = policy.get("policy_id", "")

    rule = None
    if rule_json:
        try:
            rule = json.loads(rule_json) if isinstance(rule_json, str) else rule_json
        except (json.JSONDecodeError, TypeError):
            rule = None

    if rule:
        rule_type = rule.get("type", "")
        key = rule.get("key", "")
        value = rule.get("value", "")

        if rule_type == "tag_exists":
            steps.append(f"Add tag '{key}' to {resource_type} '{resource_name}'.")
            steps.append(
                f"Example SQL: ALTER {resource_type.upper()} `{resource_name}` "
                f"SET TAGS ('{key}' = '<appropriate_value>');"
            )
        elif rule_type == "tag_equals":
            current = tags.get(key, "<not set>") if tags else "<not set>"
            steps.append(
                f"Set tag '{key}' to '{value}' on {resource_type} '{resource_name}' "
                f"(current value: '{current}')."
            )
            steps.append(
                f"Example SQL: ALTER {resource_type.upper()} `{resource_name}` "
                f"SET TAGS ('{key}' = '{value}');"
            )
        elif rule_type == "tag_in":
            allowed = value if isinstance(value, str) else ", ".join(value)
            steps.append(
                f"Set tag '{key}' to one of [{allowed}] on {resource_type} '{resource_name}'."
            )
        elif rule_type in ("metadata_equals", "metadata_not_empty"):
            steps.append(f"Update metadata field '{key}' on {resource_type} '{resource_name}'.")
        else:
            steps.append(f"Review and remediate the rule condition: {json.dumps(rule, default=str)}")

    # Access governance policies (POL-A*)
    if policy_id.startswith("POL-A"):
        steps.append("Review access grants for the resource and remove or adjust excessive permissions.")
        steps.append(
            f"Example SQL: SHOW GRANTS ON {resource_type.upper()} `{resource_name}`;"
        )

    # Fallback: use remediation text from violation or policy
    if not steps:
        remediation_text = violation.get("remediation") or policy.get("remediation", "")
        if remediation_text:
            steps.append(remediation_text)
        else:
            steps.append("Contact your platform admin for remediation guidance.")

    return steps


def _build_failure_condition(rule_type: str, rule_key: str, rule_value: str | None) -> str:
    """Build a SQL WHERE clause fragment for resources that FAIL the proposed rule."""
    if rule_type == "tag_exists":
        return f"tags['{rule_key}'] IS NULL"
    elif rule_type == "tag_equals":
        return f"COALESCE(tags['{rule_key}'], '') != '{rule_value}'"
    elif rule_type == "tag_in":
        values = ", ".join(f"'{v.strip()}'" for v in (rule_value or "").split(","))
        return f"COALESCE(tags['{rule_key}'], '') NOT IN ({values})"
    elif rule_type == "metadata_equals":
        return f"COALESCE(metadata['{rule_key}'], '') != '{rule_value}'"
    elif rule_type == "metadata_not_empty":
        return f"COALESCE(metadata['{rule_key}'], '') = ''"
    raise ValueError(f"Unsupported rule_type: {rule_type}")


async def _explain_violation(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    metastore = _resolve_metastore(args, config)
    ms_and = f"AND metastore_id = '{metastore}'" if metastore else ""
    violation_id = args.get("violation_id")
    resource_id = args.get("resource_id")
    policy_id = args.get("policy_id")

    if not violation_id and not (resource_id and policy_id):
        return [TextContent(
            type="text",
            text=json.dumps({"error": "Provide either violation_id or both resource_id and policy_id."}),
        )]

    # Step 1: Look up the violation
    if violation_id:
        violation_query = f"""
            SELECT v.violation_id, v.resource_id, v.resource_type, v.resource_name,
                   v.policy_id, v.severity, v.domain, v.detail, v.remediation,
                   v.owner, v.resource_classes, v.first_detected, v.last_detected, v.status
            FROM {qs}.violations v
            WHERE v.violation_id = '{violation_id}'
            {ms_and}
        """
    else:
        violation_query = f"""
            SELECT v.violation_id, v.resource_id, v.resource_type, v.resource_name,
                   v.policy_id, v.severity, v.domain, v.detail, v.remediation,
                   v.owner, v.resource_classes, v.first_detected, v.last_detected, v.status
            FROM {qs}.violations v
            WHERE v.resource_id = '{resource_id}' AND v.policy_id = '{policy_id}'
              AND v.status = 'open'
            {ms_and}
        """

    violation_result = _execute_sql(w, config, violation_query)
    if violation_result.get("error"):
        return [TextContent(type="text", text=json.dumps(violation_result, indent=2, default=str))]
    if not violation_result["rows"]:
        return [TextContent(type="text", text=json.dumps({"error": "Violation not found."}))]

    violation = violation_result["rows"][0]
    resource_id = violation["resource_id"]
    policy_id = violation["policy_id"]

    # Step 2: Look up the policy definition
    policy_query = f"""
        SELECT policy_id, policy_name, applies_to, domain, severity,
               description, remediation, rule_json
        FROM {qs}.policies
        WHERE policy_id = '{policy_id}'
    """
    policy_result = _execute_sql(w, config, policy_query)
    policy = policy_result["rows"][0] if policy_result["rows"] else {}

    # Step 3: Look up resource classifications
    classes_query = f"""
        SELECT class_name, class_ancestors, root_class
        FROM {qs}.resource_classifications
        WHERE resource_id = '{resource_id}'
          AND scan_id = (SELECT MAX(scan_id) FROM {qs}.resource_classifications)
    """
    classes_result = _execute_sql(w, config, classes_query)
    classes = classes_result["rows"]

    # Step 4: Look up resource tags and metadata
    inventory_query = f"""
        SELECT tags, metadata
        FROM {qs}.resource_inventory
        WHERE resource_id = '{resource_id}'
          AND scan_id = (SELECT MAX(scan_id) FROM {qs}.resource_inventory)
    """
    inventory_result = _execute_sql(w, config, inventory_query)
    resource_info = inventory_result["rows"][0] if inventory_result["rows"] else {}
    tags = resource_info.get("tags")
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except (json.JSONDecodeError, TypeError):
            pass

    # Compute days_open
    first_detected = violation.get("first_detected")
    days_open = None
    if first_detected:
        try:
            if isinstance(first_detected, str):
                dt = datetime.fromisoformat(first_detected.replace("Z", "+00:00"))
            else:
                dt = first_detected
            days_open = (datetime.now(timezone.utc) - dt).days
        except (ValueError, TypeError):
            pass

    # Step 5: Compose structured explanation
    resource_name = violation.get("resource_name", resource_id)
    resource_type = violation.get("resource_type", "unknown")
    severity = violation.get("severity", "unknown")
    domain = violation.get("domain", "unknown")
    policy_name = policy.get("policy_name", policy_id)
    policy_description = policy.get("description", "")

    explanation = {
        "violation": {
            "id": violation["violation_id"],
            "status": violation["status"],
            "severity": severity,
            "first_detected": first_detected,
            "days_open": days_open,
        },
        "what_happened": (
            f"Resource '{resource_name}' ({resource_type}) violated policy "
            f"'{policy_name}'. {violation.get('detail', '')}"
        ),
        "why_it_matters": (
            f"This is a {severity} violation in the {domain} domain. "
            f"{policy_description}"
        ),
        "resource_context": {
            "owner": violation.get("owner"),
            "ontology_classes": classes,
            "current_tags": tags,
        },
        "policy_context": {
            "policy_id": policy_id,
            "policy_name": policy_name,
            "applies_to": policy.get("applies_to"),
            "rule": policy.get("rule_json"),
        },
        "remediation": {
            "summary": violation.get("remediation") or policy.get("remediation"),
            "steps": _build_remediation_steps(violation, policy, tags),
        },
    }
    return [TextContent(type="text", text=json.dumps(explanation, indent=2, default=str))]


async def _what_if_policy(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    metastore = _resolve_metastore(args, config)
    ms_and = f"AND ri.metastore_id = '{metastore}'" if metastore else ""
    applies_to = args.get("applies_to", "*")
    rule_type = args["rule_type"]
    rule_key = args["rule_key"]
    rule_value = args.get("rule_value")
    severity = args.get("severity", "medium")

    try:
        failure_condition = _build_failure_condition(rule_type, rule_key, rule_value)
    except ValueError as exc:
        return [TextContent(type="text", text=json.dumps({"error": str(exc)}))]

    # Build queries based on applies_to scope
    if applies_to == "*":
        failing_query = f"""
            SELECT ri.resource_id, ri.resource_type, ri.resource_name, ri.owner,
                   ri.tags['{rule_key}'] as current_value
            FROM {qs}.resource_inventory ri
            WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {qs}.resource_inventory)
              AND {failure_condition}
              {ms_and}
        """
        total_query = f"""
            SELECT COUNT(DISTINCT ri.resource_id) as total
            FROM {qs}.resource_inventory ri
            WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {qs}.resource_inventory)
            {ms_and}
        """
    else:
        failing_query = f"""
            SELECT ri.resource_id, ri.resource_type, ri.resource_name, ri.owner,
                   ri.tags['{rule_key}'] as current_value
            FROM {qs}.resource_inventory ri
            JOIN {qs}.resource_classifications rc
              ON ri.resource_id = rc.resource_id AND ri.scan_id = rc.scan_id
            WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {qs}.resource_inventory)
              AND rc.class_name = '{applies_to}'
              AND {failure_condition}
              {ms_and}
        """
        total_query = f"""
            SELECT COUNT(DISTINCT ri.resource_id) as total
            FROM {qs}.resource_inventory ri
            JOIN {qs}.resource_classifications rc
              ON ri.resource_id = rc.resource_id AND ri.scan_id = rc.scan_id
            WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {qs}.resource_inventory)
              AND rc.class_name = '{applies_to}'
              {ms_and}
        """

    failing_result = _execute_sql(w, config, failing_query)
    if failing_result.get("error"):
        return [TextContent(type="text", text=json.dumps(failing_result, indent=2, default=str))]

    total_result = _execute_sql(w, config, total_query)
    total_count = int(total_result["rows"][0]["total"]) if total_result["rows"] else 0

    failing_rows = failing_result["rows"]
    impact_pct = round(len(failing_rows) / max(total_count, 1) * 100, 1)

    simulation = {
        "proposed_policy": {
            "applies_to": applies_to,
            "rule_type": rule_type,
            "rule_key": rule_key,
            "rule_value": rule_value,
            "severity": severity,
        },
        "impact": {
            "resources_in_scope": total_count,
            "would_violate": len(failing_rows),
            "impact_pct": impact_pct,
        },
        "would_violate": failing_rows[:50],
        "summary": (
            f"This policy would create {len(failing_rows)} new {severity} violations "
            f"across {len(failing_rows)} resources ({impact_pct}% of {applies_to} resources)."
        ),
    }
    return [TextContent(type="text", text=json.dumps(simulation, indent=2, default=str))]


async def _list_metastores(
    w: WorkspaceClient, config: WatchdogMcpConfig, args: dict[str, Any]
) -> list[TextContent]:
    qs = config.qualified_schema
    query = f"""
        SELECT metastore_id, MAX(discovered_at) as last_scanned,
               COUNT(DISTINCT resource_id) as resource_count
        FROM {qs}.resource_inventory
        WHERE metastore_id IS NOT NULL AND metastore_id != ''
        GROUP BY metastore_id
        ORDER BY last_scanned DESC
    """
    result = _execute_sql(w, config, query)
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
