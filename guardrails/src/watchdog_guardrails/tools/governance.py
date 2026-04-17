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
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from mcp.types import TextContent, Tool

from watchdog_guardrails.config import GuardrailsConfig
from watchdog_guardrails.watchdog_client import (
    ResourceGovernanceState,
    _esc,
    get_resource_governance,
)

logger = logging.getLogger(__name__)

# ── Runtime agent session state (per server instance) ──────────────────────
_agent_sessions: dict[str, dict] = {}

# ── Risk matrices ───────────────────────────────────────────────────────────

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

_SENSITIVE_TAGS = {"pii", "phi"}

_MAX_CLASSIFICATION: dict[str, str] = {
    "query": "restricted",
    "chat_context": "confidential",
    "embed": "internal",
    "train": "internal",
}

# ── Tool definitions ────────────────────────────────────────────────────────

TOOLS: list[Tool] = [
    Tool(
        name="get_table_lineage",
        description=(
            "Get upstream and downstream lineage for a table from Unity Catalog. "
            "Shows what feeds into this table and what depends on it."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name: catalog.schema.table"},
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="get_table_permissions",
        description=(
            "List who has access to a table and at what level (SELECT, MODIFY, ALL PRIVILEGES)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name: catalog.schema.table"},
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="describe_table",
        description=(
            "Get detailed metadata for a table: columns, types, comments, tags, and row count."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name: catalog.schema.table"},
                "include_column_tags": {"type": "boolean", "description": "Include column-level tags. Default: true."},
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="search_tables_by_tag",
        description="Find tables matching governance tags (e.g. classification=confidential, pii=true).",
        inputSchema={
            "type": "object",
            "properties": {
                "tag_name": {"type": "string", "description": "Tag key to search for."},
                "tag_value": {"type": "string", "description": "Tag value to match. Omit to find all tables with this tag."},
                "catalog": {"type": "string", "description": "Limit search to a specific catalog."},
            },
            "required": ["tag_name"],
        },
    ),
    Tool(
        name="validate_ai_query",
        description=(
            "Pre-flight governance check before an AI operation. Validates tables against "
            "classification tags and Watchdog violations. Returns proceed/warning/blocked verdict."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tables": {"type": "array", "items": {"type": "string"}, "description": "Fully qualified table names to validate."},
                "operation": {"type": "string", "enum": ["query", "embed", "chat_context", "train"]},
                "purpose": {"type": "string", "description": "Brief description of why this data is needed."},
            },
            "required": ["tables", "operation"],
        },
    ),
    Tool(
        name="suggest_safe_tables",
        description="Find tables safe for a given AI operation in a catalog or schema.",
        inputSchema={
            "type": "object",
            "properties": {
                "operation": {"type": "string", "enum": ["query", "embed", "chat_context", "train"]},
                "schema_name": {"type": "string", "description": "Schema to search (catalog.schema)."},
                "keyword": {"type": "string", "description": "Optional keyword to filter table names."},
                "limit": {"type": "integer", "description": "Max results. Default: 20."},
            },
            "required": ["operation"],
        },
    ),
    Tool(
        name="preview_data",
        description="Peek at sample rows from a table (max 50 rows, respects UC grants).",
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name: catalog.schema.table"},
                "columns": {"type": "array", "items": {"type": "string"}, "description": "Specific columns to preview."},
                "limit": {"type": "integer", "description": "Number of rows. Default: 10, max: 50."},
                "where": {"type": "string", "description": "Optional WHERE clause (read-only only)."},
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="safe_columns",
        description=(
            "For a partially restricted table, find which columns are safe for your operation. "
            "Returns columns grouped as safe, warning, or blocked."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name: catalog.schema.table"},
                "operation": {"type": "string", "enum": ["query", "embed", "chat_context", "train"]},
            },
            "required": ["table_name", "operation"],
        },
    ),
    Tool(
        name="estimate_cost",
        description="Estimate DBU cost of an AI operation on a table before running it.",
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name: catalog.schema.table"},
                "operation": {"type": "string", "enum": ["embed", "chat_context", "train", "query"]},
                "columns": {"type": "array", "items": {"type": "string"}},
                "row_limit": {"type": "integer"},
            },
            "required": ["table_name", "operation"],
        },
    ),
    Tool(
        name="check_before_access",
        description=(
            "Runtime governance check — call BEFORE an agent accesses a table. "
            "Returns allow/warn/deny based on Watchdog violations."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "agent_id": {"type": "string"},
                "table": {"type": "string", "description": "Fully qualified table name."},
                "operation": {"type": "string", "enum": ["SELECT", "INSERT", "UPDATE", "DELETE"], "description": "Default: SELECT."},
                "columns": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["agent_id", "table"],
        },
    ),
    Tool(
        name="log_agent_action",
        description="Log an agent action for governance audit trail.",
        inputSchema={
            "type": "object",
            "properties": {
                "agent_id": {"type": "string"},
                "action": {"type": "string", "enum": ["data_access", "data_export", "external_api_call", "model_invocation", "tool_call"]},
                "target": {"type": "string"},
                "details": {"type": "object"},
                "classification": {"type": "string"},
            },
            "required": ["agent_id", "action", "target"],
        },
    ),
    Tool(
        name="get_agent_compliance",
        description="Get the current compliance status of an agent session.",
        inputSchema={
            "type": "object",
            "properties": {"agent_id": {"type": "string"}},
            "required": ["agent_id"],
        },
    ),
    Tool(
        name="report_agent_execution",
        description="Generate a post-execution compliance report for an agent.",
        inputSchema={
            "type": "object",
            "properties": {
                "agent_id": {"type": "string"},
                "execution_summary": {"type": "string"},
            },
            "required": ["agent_id"],
        },
    ),
]

# ── Router ──────────────────────────────────────────────────────────────────

async def handle(
    name: str, arguments: dict[str, Any], w: WorkspaceClient, config: GuardrailsConfig
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
    raise ValueError(f"Unknown tool: {name}")

# ── Helpers ─────────────────────────────────────────────────────────────────

def _parse_table_name(table_name: str) -> tuple[str, str, str]:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Table name must be fully qualified (catalog.schema.table), got: {table_name}"
        )
    return parts[0], parts[1], parts[2]


# ── Data discovery tools ────────────────────────────────────────────────────

async def _get_table_lineage(w, config, args):
    table_name = args["table_name"]
    try:
        lineage = w.api_client.do(
            "GET", "/api/2.0/lineage-tracking/table-lineage",
            query={"table_name": table_name},
        )
        upstream = [
            {"table": i.get("tableInfo", {}).get("name"),
             "catalog": i.get("tableInfo", {}).get("catalog_name"),
             "schema": i.get("tableInfo", {}).get("schema_name")}
            for i in lineage.get("upstreams", [])
        ]
        downstream = [
            {"table": i.get("tableInfo", {}).get("name"),
             "catalog": i.get("tableInfo", {}).get("catalog_name"),
             "schema": i.get("tableInfo", {}).get("schema_name")}
            for i in lineage.get("downstreams", [])
        ]
        result = {"table": table_name, "upstream_count": len(upstream),
                  "downstream_count": len(downstream), "upstream": upstream, "downstream": downstream}
    except Exception as exc:
        result = {"table": table_name, "error": f"Lineage not available: {exc}"}
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _get_table_permissions(w, config, args):
    table_name = args["table_name"]
    try:
        perms = w.grants.get(securable_type="TABLE", full_name=table_name)
        grants = [
            {"principal": a.principal,
             "privileges": [str(p.privilege) for p in (a.privileges or [])]}
            for a in (perms.privilege_assignments or [])
        ]
        result = {"table": table_name, "grant_count": len(grants), "grants": grants}
    except Exception as exc:
        result = {"table": table_name, "error": f"Cannot read permissions: {exc}"}
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _describe_table(w, config, args):
    table_name = args["table_name"]
    try:
        info = w.tables.get(full_name=table_name)
        columns = [
            {"name": c.name, "type": str(c.type_name) if c.type_name else None,
             "comment": c.comment, "nullable": c.nullable, "position": c.position}
            for c in (info.columns or [])
        ]
        props = info.properties or {}
        result: dict[str, Any] = {
            "table": table_name, "owner": info.owner,
            "table_type": str(info.table_type) if info.table_type else None,
            "comment": info.comment, "properties": dict(props),
            "storage_location": info.storage_location,
            "column_count": len(columns), "columns": columns,
        }
        if "spark.sql.statistics.numRows" in props:
            result["row_count"] = int(props["spark.sql.statistics.numRows"])
    except Exception as exc:
        result = {"table": table_name, "error": str(exc)}
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _search_tables_by_tag(w, config, args):
    tag_name = args["tag_name"]
    tag_value = args.get("tag_value")
    catalog = args.get("catalog")

    where_parts = [f"tag_name = '{_esc(tag_name)}'"]
    if tag_value:
        where_parts.append(f"tag_value = '{_esc(tag_value)}'")
    where = " AND ".join(where_parts)

    if catalog:
        if not re.match(r'^[\w-]+$', catalog):
            return [TextContent(type="text", text=json.dumps({"error": f"Invalid catalog name: {catalog}"}, indent=2))]
        query = f"SELECT catalog_name, schema_name, table_name, tag_name, tag_value FROM `{catalog}`.information_schema.table_tags WHERE {where} ORDER BY schema_name, table_name"
    else:
        query = f"SELECT catalog_name, schema_name, table_name, tag_name, tag_value FROM system.information_schema.table_tags WHERE {where} ORDER BY catalog_name, schema_name, table_name LIMIT 100"

    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id, statement=query, wait_timeout="30s"
        )
        cols = [c.name for c in resp.manifest.schema.columns] if resp.manifest else []
        rows = resp.result.data_array if resp.result else []
        result = {"tag": tag_name, "tag_value": tag_value, "match_count": len(rows),
                  "tables": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        result = {"tag": tag_name, "error": str(exc)}
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# ── validate_ai_query — tag-based, no class hierarchy ──────────────────────

async def _validate_ai_query(w, config, args):
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
        gov = get_resource_governance(w, config, table_name)
        finding["watchdog_available"] = gov.watchdog_available
        finding["ontology_classes"] = gov.classes

        try:
            _parse_table_name(table_name)
            info = w.tables.get(full_name=table_name)
            finding["owner"] = info.owner
            props = info.properties or {}

            # Classification from UC table properties/tags
            tag_classification: str | None = None
            sensitive_flags: list[str] = []
            for key, val in props.items():
                key_lower = key.lower()
                if key_lower in ("classification", "tag_classification"):
                    tag_classification = val.lower()
                if any(tag in key_lower for tag in _SENSITIVE_TAGS):
                    if val.lower() in ("true", "yes", "1"):
                        sensitive_flags.append(key)

            classification = tag_classification or "unclassified"
            finding["classification"] = classification
            finding["classification_source"] = "tags"
            finding["sensitive_flags"] = sensitive_flags

            # Classification × operation risk
            class_risk = _CLASSIFICATION_RISK.get(classification, 1)
            combined_risk = op_risk + class_risk

            if combined_risk >= 7:
                msg = (
                    f"{table_name}: {operation} on {classification} data is blocked "
                    f"(risk score {combined_risk})."
                )
                finding["issues"].append({"severity": "blocker", "message": msg})
                blockers.append(msg)
            elif combined_risk >= 5:
                msg = (
                    f"{table_name}: {operation} on {classification} data — "
                    f"high risk (score {combined_risk}). Confirm necessity."
                )
                finding["issues"].append({"severity": "warning", "message": msg})
                warnings.append(msg)

            # Sensitive tag + high-risk operation
            if sensitive_flags and operation == "train":
                msg = (
                    f"{table_name}: training on {', '.join(sensitive_flags)} data is blocked. "
                    f"Use a governance exception to proceed."
                )
                finding["issues"].append({"severity": "blocker", "message": msg})
                blockers.append(msg)
            elif sensitive_flags and op_risk >= 3:
                msg = f"{table_name}: contains {', '.join(sensitive_flags)} — review before {operation}."
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
                finding["issues"].append({"severity": "warning", "message": f"Metadata unavailable: {err}"})
                warnings.append(f"{table_name}: could not read metadata — {err}")

        # Watchdog violations
        for v in gov.open_violations:
            sev = v.get("severity", "medium")
            policy = v.get("policy_name", v.get("policy_id", "unknown"))
            excepted = any(
                e.get("policy_id") == v.get("policy_id") for e in gov.active_exceptions
            )
            if excepted:
                finding["issues"].append({"severity": "info", "message": f"{table_name}: {sev} violation ({policy}) — exception approved."})
            elif sev in ("critical", "high"):
                msg = f"{table_name}: open {sev} violation ({policy}). Resolve before using in AI workflows."
                finding["issues"].append({"severity": "blocker", "message": msg})
                blockers.append(msg)
            else:
                msg = f"{table_name}: open {sev} violation ({policy})."
                finding["issues"].append({"severity": "warning", "message": msg})
                warnings.append(msg)

        findings.append(finding)

    verdict = "blocked" if blockers else ("warning" if warnings else "proceed")
    result: dict[str, Any] = {
        "verdict": verdict, "operation": operation, "purpose": purpose,
        "tables_checked": len(tables), "blockers": blockers,
        "warnings": warnings, "findings": findings,
    }

    if verdict == "blocked":
        max_class = _MAX_CLASSIFICATION.get(operation, "internal")
        result["guidance"] = (
            f"Blocked. For '{operation}', max classification is '{max_class}'. "
            f"Options: (1) use suggest_safe_tables to find compatible data, "
            f"(2) use a lower-risk operation, "
            f"(3) request a governance exception via watchdog-mcp."
        )
    elif verdict == "warning":
        result["guidance"] = "Passed with warnings — review before proceeding. Access is audit-logged."
    else:
        result["guidance"] = "All tables cleared. Proceed."

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _find_safe_tables_in_schema(w, config, schema_fqn, operation, limit=5, keyword=None):
    op_risk = _OPERATION_RISK.get(operation, 2)
    cat, sch = schema_fqn.split(".", 1)
    safe = []
    for tbl in w.tables.list(catalog_name=cat, schema_name=sch):
        if keyword and keyword.lower() not in (tbl.name or "").lower():
            if keyword.lower() not in (tbl.comment or "").lower():
                continue
        props = tbl.properties or {}
        classification = None
        sensitive_flags = []
        for key, val in props.items():
            if key.lower() in ("classification", "tag_classification"):
                classification = val.lower()
            if any(tag in key.lower() for tag in _SENSITIVE_TAGS):
                if val.lower() in ("true", "yes", "1"):
                    sensitive_flags.append(key)
        class_risk = _CLASSIFICATION_RISK.get(classification or "internal", 1)
        if op_risk + class_risk >= 7:
            continue
        if sensitive_flags and operation == "train":
            continue
        safe.append({
            "table": f"{cat}.{sch}.{tbl.name}",
            "classification": classification or "unclassified",
            "sensitive_flags": sensitive_flags,
            "risk_score": op_risk + class_risk,
            "comment": tbl.comment,
        })
        if len(safe) >= limit:
            break
    safe.sort(key=lambda x: x["risk_score"])
    return safe


async def _suggest_safe_tables(w, config, args):
    operation = args["operation"]
    schema_name = args.get("schema_name")
    keyword = args.get("keyword")
    limit = args.get("limit", 20)
    max_class = _MAX_CLASSIFICATION.get(operation, "internal")

    if schema_name:
        schemas = [schema_name]
    else:
        try:
            schemas = [f"{config.catalog}.{s.name}" for s in w.schemas.list(catalog_name=config.catalog) if s.name != "information_schema"]
        except Exception as exc:
            return [TextContent(type="text", text=json.dumps({"error": str(exc)}, indent=2))]

    all_safe: list[dict[str, Any]] = []
    for schema_fqn in schemas:
        try:
            all_safe.extend(await _find_safe_tables_in_schema(w, config, schema_fqn, operation, limit - len(all_safe), keyword))
        except Exception:
            continue
        if len(all_safe) >= limit:
            break

    result: dict[str, Any] = {
        "operation": operation, "max_classification": max_class,
        "keyword": keyword, "tables_found": len(all_safe), "tables": all_safe[:limit],
    }
    result["guidance"] = (
        f"Found {len(all_safe)} table(s) safe for '{operation}'. Sorted by risk score."
        if all_safe else
        f"No tables found safe for '{operation}'. Max classification is '{max_class}'."
    )
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _preview_data(w, config, args):
    table_name = args["table_name"]
    columns = args.get("columns")
    limit = min(args.get("limit", 10), 50)
    where = args.get("where")
    _parse_table_name(table_name)

    if where:
        dangerous = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "MERGE", "TRUNCATE", "GRANT", "REVOKE"}
        if set(re.findall(r'\b[A-Z_]+\b', where.upper())) & dangerous or ';' in where:
            return [TextContent(type="text", text=json.dumps({"error": "WHERE clause contains forbidden keywords."}, indent=2))]

    col_clause = ", ".join(f"`{_esc(c)}`" for c in columns) if columns else "*"
    where_clause = f"WHERE {where}" if where else ""
    query = f"SELECT {col_clause} FROM `{_esc(table_name)}` {where_clause} LIMIT {limit}"

    try:
        resp = w.statement_execution.execute_statement(warehouse_id=config.warehouse_id, statement=query, wait_timeout="30s")
        col_names = [c.name for c in resp.manifest.schema.columns] if resp.manifest else []
        rows = resp.result.data_array if resp.result else []
        result = {"table": table_name, "columns": col_names, "row_count": len(rows), "rows": [dict(zip(col_names, r)) for r in rows]}
    except Exception as exc:
        result = {"table": table_name, "error": str(exc)}
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _safe_columns(w, config, args):
    table_name = args["table_name"]
    operation = args["operation"]
    op_risk = _OPERATION_RISK.get(operation, 2)

    try:
        info = w.tables.get(full_name=table_name)
    except Exception as exc:
        return [TextContent(type="text", text=json.dumps({"table": table_name, "error": str(exc)}, indent=2))]

    pii_patterns = {
        "ssn", "social_security", "tax_id", "passport", "drivers_license",
        "date_of_birth", "dob", "home_address", "personal_email",
        "phone_number", "mobile", "salary", "compensation",
    }
    phi_patterns = {
        "diagnosis", "treatment", "medical", "patient_id", "health_record",
        "prescription", "dosage",
    }

    safe, warning, blocked = [], [], []
    for col in info.columns or []:
        name_lower = (col.name or "").lower()
        comment_lower = (col.comment or "").lower()
        matches = (
            [f"pii:{p}" for p in pii_patterns if p in name_lower or p in comment_lower] +
            [f"phi:{p}" for p in phi_patterns if p in name_lower or p in comment_lower]
        )
        entry = {"name": col.name, "type": str(col.type_name) if col.type_name else None, "sensitive_matches": matches}
        if matches and operation == "train":
            entry["reason"] = "Sensitive column blocked for model training"
            blocked.append(entry)
        elif matches and op_risk >= 3:
            entry["reason"] = "Sensitive — use with caution"
            warning.append(entry)
        elif matches:
            entry["reason"] = "Contains sensitive data but allowed for read queries"
            warning.append(entry)
        else:
            safe.append(entry)

    safe_names = [c["name"] for c in safe]
    result: dict[str, Any] = {
        "table": table_name, "operation": operation,
        "total_columns": len(safe) + len(warning) + len(blocked),
        "safe_columns": safe, "warning_columns": warning, "blocked_columns": blocked,
        "safe_column_list": safe_names,
    }
    if blocked:
        result["guidance"] = f"{len(blocked)} column(s) blocked for '{operation}'. Use safe_column_list to build your SELECT."
    elif warning:
        result["guidance"] = f"All columns usable but {len(warning)} have sensitive data. Review before use."
    else:
        result["guidance"] = "All columns safe."
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


_TOKENS_PER_VALUE: dict[str, float] = {
    "STRING": 15.0, "LONG": 2.0, "INT": 2.0, "DOUBLE": 2.0,
    "FLOAT": 2.0, "BOOLEAN": 1.0, "DATE": 3.0, "TIMESTAMP": 4.0, "BINARY": 20.0,
}
_DBU_PER_MILLION_TOKENS: dict[str, float] = {
    "embed": 0.5, "chat_context": 5.0, "train": 15.0, "query": 0.0,
}


async def _estimate_cost(w, config, args):
    table_name = args["table_name"]
    operation = args["operation"]
    selected_columns = args.get("columns")
    row_limit = args.get("row_limit")

    try:
        info = w.tables.get(full_name=table_name)
    except Exception as exc:
        return [TextContent(type="text", text=json.dumps({"table": table_name, "error": str(exc)}, indent=2))]

    props = info.properties or {}
    row_count = int(props.get("spark.sql.statistics.numRows", 0))
    if row_count == 0:
        try:
            resp = w.statement_execution.execute_statement(warehouse_id=config.warehouse_id, statement=f"SELECT COUNT(*) FROM {table_name}", wait_timeout="15s")
            if resp.result and resp.result.data_array:
                row_count = int(resp.result.data_array[0][0])
        except Exception:
            pass

    if row_limit:
        row_count = min(row_count, row_limit)

    columns = info.columns or []
    if selected_columns:
        columns = [c for c in columns if c.name in selected_columns]

    tokens_per_row = sum(_TOKENS_PER_VALUE.get(str(c.type_name).upper() if c.type_name else "STRING", 10.0) for c in columns)
    total_tokens = row_count * tokens_per_row
    estimated_dbus = (total_tokens / 1_000_000) * _DBU_PER_MILLION_TOKENS.get(operation, 1.0)

    result: dict[str, Any] = {
        "table": table_name, "operation": operation, "row_count": row_count,
        "column_count": len(columns), "estimated_tokens_per_row": round(tokens_per_row, 1),
        "estimated_total_tokens": int(total_tokens), "estimated_dbus": round(estimated_dbus, 2),
    }
    if operation == "query":
        result["guidance"] = "SQL queries are billed by warehouse compute, not tokens."
    elif estimated_dbus < 1:
        result["guidance"] = f"Low cost (~{estimated_dbus:.2f} DBUs). Proceed."
    elif estimated_dbus < 10:
        result["guidance"] = f"Moderate cost (~{estimated_dbus:.1f} DBUs)."
    else:
        result["guidance"] = f"High cost (~{estimated_dbus:.0f} DBUs). Consider sampling."
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# ── Agent audit tools ───────────────────────────────────────────────────────

def _init_agent_session(agent_id: str) -> dict:
    if agent_id not in _agent_sessions:
        _agent_sessions[agent_id] = {
            "agent_id": agent_id,
            "checks_passed": 0, "checks_denied": 0, "checks_warned": 0,
            "tables_accessed": [], "actions_logged": 0, "risk_level": "low",
            "session_start": datetime.now(timezone.utc).isoformat(),
        }
    return _agent_sessions[agent_id]


def _calculate_risk_level(session: dict) -> str:
    denied = session.get("checks_denied", 0)
    warned = session.get("checks_warned", 0)
    if denied > 0:
        return "high"
    if warned > 0:
        return "medium"
    return "low"


async def _check_before_access(w, config, args):
    agent_id = args["agent_id"]
    table = args["table"]
    operation = args.get("operation", "SELECT")

    session = _init_agent_session(agent_id)
    gov = get_resource_governance(w, config, table)

    reasons: list[str] = []
    alternatives: list[str] = []
    decision = "allow"

    if gov.has_critical_violations:
        n = sum(1 for v in gov.open_violations if v.get("severity") == "critical")
        reasons.append(f"Table has {n} critical violation(s).")
        decision = "deny"
        alternatives.append("Resolve critical violations before access.")

    if gov.has_high_violations and decision != "deny":
        n = sum(1 for v in gov.open_violations if v.get("severity") == "high")
        reasons.append(f"Table has {n} open high-severity violation(s).")
        decision = "warn"

    if decision == "allow":
        session["checks_passed"] += 1
    elif decision == "deny":
        session["checks_denied"] += 1
    elif decision == "warn":
        session["checks_warned"] += 1

    if table not in session["tables_accessed"]:
        session["tables_accessed"].append(table)
    session["risk_level"] = _calculate_risk_level(session)

    result = {
        "decision": decision, "table": table, "agent_id": agent_id,
        "operation": operation, "ontology_classes": gov.classes,
        "open_violations": len(gov.open_violations),
        "reasons": reasons, "alternatives": alternatives,
    }
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _log_agent_action(w, config, args):
    agent_id = args["agent_id"]
    session = _init_agent_session(agent_id)
    session["actions_logged"] += 1

    event_id = str(uuid.uuid4())
    event = {
        "event_id": event_id, "event_type": "agent_action",
        "agent_id": agent_id, "action": args["action"],
        "target": args["target"], "details": args.get("details", {}),
        "classification": args.get("classification"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(json.dumps(event, default=str))
    return [TextContent(type="text", text=json.dumps(
        {"status": "logged", "event_id": event_id, "agent_id": agent_id,
         "action": args["action"], "target": args["target"]}, indent=2
    ))]


async def _get_agent_compliance(w, config, args):
    session = _init_agent_session(args["agent_id"])
    result = {
        "agent_id": args["agent_id"],
        "checks_passed": session["checks_passed"],
        "checks_denied": session["checks_denied"],
        "checks_warned": session["checks_warned"],
        "tables_accessed": session["tables_accessed"],
        "actions_logged": session["actions_logged"],
        "risk_level": session["risk_level"],
        "session_start": session["session_start"],
    }
    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _report_agent_execution(w, config, args):
    agent_id = args["agent_id"]
    session = _agent_sessions.get(agent_id)

    if session is None:
        return [TextContent(type="text", text=json.dumps({
            "agent_id": agent_id, "execution_summary": args.get("execution_summary", ""),
            "compliance_status": "compliant", "risk_level": "low",
            "governance_checks": {"total": 0, "passed": 0, "denied": 0, "warned": 0},
            "data_access": {"tables_accessed": []}, "actions_logged": 0,
            "recommendations": ["No governance activity recorded."],
        }, indent=2))]

    passed, denied, warned = session["checks_passed"], session["checks_denied"], session["checks_warned"]
    risk_level = session["risk_level"]
    compliance_status = "non_compliant" if denied > 0 else ("needs_review" if warned > 0 else "compliant")

    recommendations = []
    if denied > 0:
        recommendations.append(f"{denied} access check(s) denied. Resolve violations before re-running.")
    if not recommendations:
        recommendations.append("Agent execution was fully compliant.")

    report = {
        "agent_id": agent_id, "execution_summary": args.get("execution_summary", ""),
        "compliance_status": compliance_status, "risk_level": risk_level,
        "governance_checks": {"total": passed + denied + warned, "passed": passed, "denied": denied, "warned": warned},
        "data_access": {"tables_accessed": session["tables_accessed"]},
        "actions_logged": session["actions_logged"],
        "recommendations": recommendations,
    }
    del _agent_sessions[agent_id]
    return [TextContent(type="text", text=json.dumps(report, indent=2, default=str))]
