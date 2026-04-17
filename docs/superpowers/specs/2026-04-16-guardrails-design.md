# AI Guardrails MCP Server

**Status:** Design approved
**Date:** 2026-04-16

---

## Problem

Watchdog detects and reports governance violations after the fact. There is no preventive layer: an AI assistant using Databricks SQL can run DROP TABLE, embed a restricted table's contents into a vector store, or quietly read PII columns without leaving any audit trail. Teams running AI workloads need defense-in-depth — governance enforcement at the moment of access, not hours later in a scan report.

## Goals

- Block SQL mutations (DROP, DELETE, ALTER, INSERT, UPDATE, MERGE, GRANT, REVOKE) through the AI service
- Surface active Watchdog violations as advisory context before an AI operation proceeds
- Audit every tool invocation with structured, PII-safe log events
- Give AI assistants tools to discover safe data, validate queries pre-flight, and log their own actions
- Deploy as a standalone Databricks App alongside the existing governance MCP

## Non-Goals

- Class-hierarchy-based blocking (no ITAR/EAR/export-control class inference — use UC column tags directly)
- Replacing UC grants (guardrails are defense-in-depth, not access control)
- Chat completion or embedding execution (the server guards data access tools, it does not run LLM calls)

---

## Architecture

Two MCP servers, different clients, different purposes:

| Server | Package | Who connects | What it does |
|--------|---------|-------------|--------------|
| `mcp/` | `watchdog_mcp` | Governance teams, security reviewers | Query Watchdog: violations, policies, scan history, classifications |
| `guardrails/` | `watchdog_guardrails` | AI assistant users, AI DevKit consumers | Safe data access: block mutations, audit calls, surface violations as context |

Both are Databricks Apps. Both use on-behalf-of auth — each MCP request runs as the calling user's identity. UC grants govern what data the user can see; guardrails govern what operations are allowed through the AI service.

```
databricks-watchdog/
├── engine/          ← scanner + policies (existing)
├── mcp/             ← governance query MCP (existing)
├── guardrails/      ← NEW: safe data access MCP
└── ontos-adapter/   ← governance UI (existing)
```

---

## Package Structure

```
guardrails/
├── src/watchdog_guardrails/
│   ├── __init__.py
│   ├── guardrails.py       ← SQL/chat/embedding safety checks
│   ├── audit.py            ← PII-safe structured audit logging
│   ├── watchdog_client.py  ← Live Watchdog data (violations, exceptions)
│   ├── config.py           ← Env-var configuration
│   ├── server.py           ← MCP SSE server, on-behalf-of auth
│   └── tools/
│       ├── __init__.py
│       └── governance.py   ← 13 MCP tools
├── requirements.txt
├── setup.py
├── app.yaml
└── databricks.yml
```

---

## Components

### 1. `guardrails.py` — Safety Checks

Pure functions, no external dependencies. Three check types:

**SQL guardrail** — blocks any mutation statement regardless of UC permissions:
```python
_BLOCKED_SQL_PATTERNS = [
    (r"\b(DROP)\s+(TABLE|SCHEMA|CATALOG|DATABASE|VIEW|FUNCTION)", "DROP operations"),
    (r"\b(DELETE)\s+FROM\b", "DELETE statements"),
    (r"\b(TRUNCATE)\s+TABLE\b", "TRUNCATE statements"),
    (r"\b(ALTER)\s+(TABLE|SCHEMA|CATALOG)", "ALTER operations"),
    (r"\b(CREATE)\s+(TABLE|SCHEMA|CATALOG|DATABASE|VIEW)", "CREATE operations"),
    (r"\b(INSERT)\s+INTO\b", "INSERT statements"),
    (r"\b(UPDATE)\s+\w+\s+SET\b", "UPDATE statements"),
    (r"\b(MERGE)\s+INTO\b", "MERGE statements"),
    (r"\b(GRANT)\b", "GRANT statements"),
    (r"\b(REVOKE)\b", "REVOKE statements"),
]
MAX_SQL_LENGTH = 10_000
```

Returns `GuardrailResult(allowed: bool, reason: str)`. Not allowed → tool returns error immediately, no audit log of the blocked content.

**Chat guardrail** — validates `max_tokens` ≤ `MAX_CHAT_TOKENS` (8,192).

**Embedding guardrail** — validates text count ≤ `MAX_EMBEDDING_TEXTS` (150).

### 2. `audit.py` — Structured Audit Logging

Emits JSON events to a dedicated `ai_devkit.audit` logger on every tool invocation. PII-safe: SQL queries are summarized (first 200 chars + structural metadata), message content is never logged verbatim.

```python
@dataclass
class AuditEvent:
    event_type: str        # "tool_invocation"
    timestamp: str         # ISO UTC
    user: str              # from x-forwarded-email header
    tool: str              # MCP tool name
    arguments_summary: dict  # redacted args
    duration_ms: int
    success: bool
    error: str | None
    catalog_accessed: str | None
    schema_accessed: str | None
```

Per-tool redaction:
- `sql_query`: logs first 200 chars, length, has_join, has_where — never full query
- `chat_completion`: logs role sequence + message count — never content
- `generate_embeddings`: logs text count — never text content
- `vector_search_query`: logs index_name, num_results — never query_text
- Discovery tools: logs all args (nothing sensitive)

### 3. `watchdog_client.py` — Live Watchdog Data

Queries the Watchdog Delta tables for live governance state. Generic — no class-hierarchy inference. UC grants on the watchdog schema govern access; degrades gracefully if unavailable.

`ResourceGovernanceState` — what the guardrails tools know about a resource:

```python
@dataclass
class ResourceGovernanceState:
    resource_id: str
    classes: list[str]               # ontology class names (informational)
    open_violations: list[dict]      # severity, policy_id, domain
    active_exceptions: list[dict]    # exception_id, policy_id, justification

    @property
    def has_critical_violations(self) -> bool: ...
    @property
    def has_high_violations(self) -> bool: ...
    def has_exception(self, policy_id: str | None = None) -> bool: ...
    def watchdog_available(self) -> bool: ...
```

**Stripping from the Mirion version:** `is_pii`, `is_confidential`, `is_export_controlled`, `is_restricted`, `inferred_classification`, `has_overprivileged_grants`, `has_direct_user_grants`, `get_grants_for_resource`, `get_service_principal_governance`. These depended on Mirion-specific class names. Classification is read directly from UC column tags instead.

Three queries per resource lookup (sequential, each with 10s timeout):
1. `resource_classifications` → class names (informational only)
2. `violations` → open violations ordered by severity
3. `exceptions` → active exceptions that haven't expired

### 4. `config.py` — Configuration

All values from environment variables. Renamed `AiDevkitConfig` → `GuardrailsConfig`.

```python
@dataclass
class GuardrailsConfig:
    host: str                # DATABRICKS_HOST
    catalog: str             # DATABRICKS_CATALOG
    watchdog_schema: str     # WATCHDOG_SCHEMA (default: "platform.watchdog")
    warehouse_id: str        # DATABRICKS_WAREHOUSE_ID
    server_name: str         # "watchdog-guardrails"
    server_version: str      # "1.0.0"
```

Removed from Mirion: `vector_search_endpoint`, `default_fmai_model`, `default_embedding_model`, `default_model_endpoint`, `extra_sensitive_patterns` — these were AI DevKit execution config, not guardrails config.

### 5. `server.py` — MCP SSE Server

FastAPI + MCP SSE transport. On-behalf-of auth: extracts user token from `Authorization: Bearer` or `x-forwarded-access-token` header, creates a `WorkspaceClient` per session with that token. User identity from `x-forwarded-email`.

Endpoints:
- `GET /health` — status, version, tool count
- `GET /mcp/sse` — MCP SSE connection
- `POST /mcp/messages/` — MCP message handler

Every tool call goes through `audit.log_tool_call()` on both success and failure.

### 6. `tools/governance.py` — 13 MCP Tools

Ported from Mirion, adapted for tag-based (not class-hierarchy) governance.

| Tool | What it does |
|------|-------------|
| `get_table_lineage` | Upstream/downstream lineage from UC system tables |
| `get_table_permissions` | Who has access and at what level |
| `describe_table` | Columns, types, comments, tags, row count |
| `search_tables_by_tag` | Find tables by tag key/value |
| `validate_ai_query` | Pre-flight governance check — returns proceed/warning/blocked |
| `suggest_safe_tables` | Find safe alternatives when a table is blocked |
| `preview_data` | Sample rows (respects UC grants, max 50 rows) |
| `safe_columns` | Which columns are safe for an operation |
| `estimate_cost` | DBU cost estimate before running a bulk operation |
| `check_before_access` | Runtime governance check for agents |
| `log_agent_action` | Audit trail entry for agent actions |
| `get_agent_compliance` | Agent compliance status for current session |
| `report_agent_execution` | Post-execution compliance report |

**Adaptation from Mirion — `validate_ai_query` and `safe_columns`:**

Mirion used ontology class names to determine blocking. Core version uses UC column tags directly:

| Tag | Value | Effect |
|-----|-------|--------|
| `pii` | `true` | Blocked for `embed`, `train`; warning for `query`, `chat_context` |
| `classification` | `restricted` | Blocked for all operations |
| `classification` | `confidential` | Blocked for `embed`, `train`; warning for `query` |
| `classification` | `internal` | Allowed with note |
| `classification` | `public` | Always allowed |

Active Watchdog violations influence verdict:
- Critical violations → blocked (unless active exception exists)
- High violations → warning
- Medium/low violations → note only

**Adaptation — `log_agent_action` and `check_before_access`:**

Write to a new `agent_audit_log` table in the watchdog schema:

```sql
CREATE TABLE {schema}.agent_audit_log (
    log_id       STRING NOT NULL,
    agent_id     STRING NOT NULL,
    action       STRING NOT NULL,   -- data_access, data_export, external_api_call, ...
    target       STRING NOT NULL,
    details      MAP<STRING, STRING>,
    classification STRING,
    user         STRING,
    session_id   STRING,
    logged_at    TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

---

## Testing

All tests in `tests/unit/`. No live Databricks connection required.

### `test_guardrails.py`
- `check_sql_query`: pass for SELECT, fail for each blocked pattern, fail for empty query, fail for query over length limit
- `check_chat_completion`: pass at limit, fail over limit, fail for empty messages
- `check_embeddings`: pass at limit, fail over limit, fail for empty list

### `test_guardrails_audit.py`
- `log_tool_call`: emits JSON event to audit logger
- `_summarize_arguments`: SQL tool — redacts query body, keeps structural metadata; chat tool — logs role sequence not content; unknown tool — passes through
- `log_session_start` / `log_session_end`: emit correct event_type

### `test_guardrails_watchdog_client.py`
- `get_resource_governance`: mock SDK, returns populated `ResourceGovernanceState`
- Graceful degradation: SDK raises exception → `watchdog_available=False`, no crash
- `has_critical_violations`: True when severity=critical in open_violations
- `has_exception(policy_id)`: True when policy_id matches active exception

---

## Files Changed

| Action | File | What |
|--------|------|------|
| Create | `guardrails/src/watchdog_guardrails/__init__.py` | Package init |
| Create | `guardrails/src/watchdog_guardrails/guardrails.py` | SQL/chat/embedding safety checks |
| Create | `guardrails/src/watchdog_guardrails/audit.py` | Structured audit logging |
| Create | `guardrails/src/watchdog_guardrails/watchdog_client.py` | Stripped Watchdog client |
| Create | `guardrails/src/watchdog_guardrails/config.py` | GuardrailsConfig |
| Create | `guardrails/src/watchdog_guardrails/server.py` | MCP SSE server |
| Create | `guardrails/src/watchdog_guardrails/tools/__init__.py` | Tools package |
| Create | `guardrails/src/watchdog_guardrails/tools/governance.py` | 13 MCP tools |
| Create | `guardrails/requirements.txt` | mcp, fastapi, uvicorn, databricks-sdk |
| Create | `guardrails/setup.py` | Package metadata |
| Create | `guardrails/app.yaml` | Databricks App config |
| Create | `guardrails/databricks.yml` | Bundle config |
| Create | `tests/unit/test_guardrails.py` | Safety check tests |
| Create | `tests/unit/test_guardrails_audit.py` | Audit logging tests |
| Create | `tests/unit/test_guardrails_watchdog_client.py` | Watchdog client tests |
| Create | `guardrails/src/watchdog_guardrails/tables.py` | `ensure_agent_audit_log_table()` — called on server startup |
