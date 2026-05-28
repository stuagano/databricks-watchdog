# Watchdog Guardrails — Policy Enforcement at Query Time

**Date:** 2026-03-24
**Status:** Ready for review
**Branch:** `p-ai-devkit`

## Problem

Unity Catalog handles "can this user access this data?" but not "should this data be used for this operation?" A developer with SELECT on a table containing PII can query it, embed it, or feed it to a model — UC doesn't distinguish between these operations even though they carry very different risk.

Watchdog scans the platform and reports violations. But reporting after the fact isn't enough for regulated industries with export controls, PII obligations, or compliance requirements. The gap between "UC allows it" and "policy says it's safe for this operation" needs to be filled at query time — before the data moves.

## What Watchdog Guardrails Does

A standalone MCP server that enforces Watchdog governance policies when developers interact with governed data. Works for both AI-assisted workflows (Claude Code, Cursor) and direct developer access via any MCP client.

```
┌──────────────────────────────────────────────────────────────┐
│  Developer / AI Assistant                                     │
│  (Claude Code, Cursor, MCP client, etc.)                      │
└───────────────┬──────────────────────────────────────────────┘
                │ MCP (SSE)
                ▼
┌──────────────────────────────────────────────────────────────┐
│  Watchdog Guardrails                                          │
│  • Pre-flight validation (classification × operation risk)    │
│  • Column-level safety                                        │
│  • Cost estimation                                            │
│  • Audit logging                                              │
└───────────────┬──────────────────────────────────────────────┘
                │ reads policies + classification
                ▼
┌──────────────────────────────────────────────────────────────┐
│  platform.watchdog schema                                     │
│  (violations, resources, classifications)                     │
└──────────────────────────────────────────────────────────────┘
```

**Relationship to other components:**
- **Watchdog** scans and reports — finds what's wrong across the platform
- **Watchdog Guardrails** enforces at query time — prevents developers from using data in ways that violate policy
- **Databricks AI Dev Kit** provides 50+ MCP tools for SQL, compute, jobs, etc. — Guardrails is complementary, not an extension of it

## Tools (9)

### Governance Tools (5)

| Tool | What it does |
|------|-------------|
| `validate_query` | Pre-flight check: classification × operation risk matrix. Returns proceed/warning/blocked. Suggests alternatives when blocked. |
| `suggest_safe_tables` | Find tables safe for a given operation within governance limits. |
| `safe_columns` | Column-level governance for partially restricted tables. Returns safe/warning/blocked columns with a ready-to-use SELECT list. |
| `preview_data` | Governance-checked sample rows (max 50, respects classification). |
| `estimate_cost` | DBU cost estimation before expensive operations. Row count × column types → token estimate → DBU cost. |

### Governance-Enhanced Metadata Tools (4)

| Tool | What it adds |
|------|-------------|
| `describe_table` | Column-level tags (PII, classification) alongside standard metadata. |
| `get_table_lineage` | UC lineage for impact analysis before modifying governed data. |
| `get_table_permissions` | Grants by group and principal — the access boundary. |
| `search_tables_by_tag` | Search by governance tags (e.g., `classification=confidential`, `pii=true`). |

### Classification × Operation Risk Matrix

The same table might be safe to query but unsafe to embed:

| Operation | Risk | Max Classification | Rationale |
|-----------|------|-------------------|-----------|
| `query` | 1 | restricted | Read-only, stays in Databricks |
| `chat_context` | 2 | confidential | Fed to LLM as context |
| `embed` | 3 | internal | Persisted as vector representations |
| `train` | 4 | internal | Baked into model weights permanently |

Combined risk (operation + classification) ≥ 7 = blocked, ≥ 5 = warning.

### Column-Level Safety

When a table is blocked at table level, `safe_columns` identifies which columns are the problem:

```
> safe_columns("catalog.hr.employees", operation="embed")

safe_columns: [department, title, hire_date, office_location]
warning_columns: [salary → pii:compensation]
blocked_columns: [ssn → pii:ssn, home_address → pii:home_address]

guidance: "3 column(s) blocked for 'embed'. Use safe_column_list:
  SELECT department, title, hire_date, office_location FROM catalog.hr.employees"
```

## Additional Components

### Defense-in-Depth Rules (`guardrails.py`)

Even if a user has DDL privileges, mutations shouldn't go through a tool designed for analytics:

- **Read-only SQL** — DROP, DELETE, INSERT, UPDATE, ALTER, MERGE, GRANT, REVOKE all blocked
- **Token limits** — max 8,192 tokens per chat completion
- **Batch limits** — max 150 texts per embedding request
- **Query length** — max 10,000 characters

### Audit Logging (`audit.py`)

Structured JSON audit events for every tool invocation. Captures user identity, tool name, tables accessed, classification tags, verdict, and cost. Events flow to the Databricks App's log output → Azure diagnostics → Log Analytics.

### Configuration (`config.py`)

All thresholds and patterns are configurable via environment variables:

| Setting | Env Var | Default |
|---------|---------|---------|
| Default catalog | `DATABRICKS_CATALOG` | (required) |
| Watchdog schema | `WATCHDOG_SCHEMA` | `platform.watchdog` |
| SQL warehouse | `DATABRICKS_WAREHOUSE_ID` | (required) |
| Extra sensitive patterns | `config.extra_sensitive_patterns` | `{}` |

## Deployment

Deployed as a Databricks App via DAB (`bundles/watchdog-guardrails/`). Runs as a standalone MCP server — not loaded into or dependent on the AI Dev Kit.

```
bundles/watchdog-guardrails/
├── databricks.yml              — DAB bundle (nonprod/prod targets)
├── app.yaml                    — Databricks App manifest
├── resources/guardrails_app.yml
├── setup.py
└── src/watchdog_guardrails/
    ├── __init__.py
    ├── server.py               — FastAPI + MCP SSE server
    ├── config.py               — env-var configuration
    ├── guardrails.py           — defense-in-depth rules
    ├── audit.py                — structured compliance logging
    ├── watchdog_client.py      — reads from platform.watchdog
    └── tools/
        ├── __init__.py
        └── governance.py       — 9 tools
```

**Requires:** Watchdog deployed first (provides `platform.watchdog` schema with classification data).

## TODOs

- [ ] Validate risk matrix thresholds with compliance team
- [ ] Configure `extra_sensitive_patterns` for industry-specific terms (export control, PHI)
- [ ] Set up Azure diagnostics pipeline for audit log ingestion
- [ ] Test with Claude Code and Cursor MCP clients
- [ ] Fill in workspace URLs and warehouse IDs in `databricks.yml`
