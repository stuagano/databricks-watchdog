# p-watchdog-mcp — Governance Query MCP Server

**Date:** 2026-04-14 (updated 2026-05-28)
**Status:** ✅ Superseded — implemented in the standalone watchdog repo
**Branch:** `proposals/stuart-handoff/p-watchdog-mcp`
**Dependencies:** `p-watchdog` deployed (provides `platform.watchdog` schema with violations, policies, scan_results, exceptions tables)

> **Superseded — tool reference at `~/Documents/Projects/databricks-watchdog/docs/guide/reference/mcp-tools.md` (standalone repo `CustomerDataPlatform/watchdog`); server implementation lives in a separate MCP server bundle, not in `databricks-watchdog/engine/`.**
>
> Tool set expanded from 8 → 13 and most names changed:
>
> | Proposed | Shipped |
> | --- | --- |
> | `get_compliance_posture` | `get_governance_summary` |
> | `query_violations` | `get_violations` |
> | `get_resource_compliance` | `get_resource_violations` |
> | `list_policies` + `get_policy` | `get_policies` (merged) |
> | `list_exceptions` | `get_exceptions` |
> | `explain_violation`, `get_scan_history` | unchanged |
> | — | `what_if_policy`, `list_metastores`, `suggest_policies`, `policy_impact_analysis`, `explore_governance`, `suggest_classification` (6 new) |
>
> A companion Guardrails MCP server (also 13 tools, see `docs/guide/reference/guardrails-tools.md`) covers the AI-build-time enforcement surface the original proposal called out as complementary. Keep this file as historical record. Do not implement an MCP server inside customer-infra — it would fork against the standalone repo.

## Problem

Watchdog writes governance data to Delta tables. That data is useful to three audiences who currently can't access it without writing SQL:

- **AI assistants** (Claude Code, Cursor) — can't incorporate compliance state into their reasoning unless they can query it
- **Operators** — need to ask natural-language questions ("what's our compliance posture today?", "which resources are failing cost policies?") without opening a SQL editor
- **Agents** — automated pipelines that need to check whether a dataset is clean before using it

The Watchdog scanner already ships with some MCP tooling embedded in the bundle. This proposal extracts it into a standalone Databricks App so it can be versioned, deployed, and updated independently — and accessed by any MCP client without deploying the full scanner.

**Relationship to p-ai-devkit (Watchdog Guardrails):**
Guardrails enforces at query time — it blocks or warns when a developer tries to use governed data in a risky way. This MCP server is read-only governance *querying* — it surfaces what Watchdog found, explains policies, and reports compliance state. They're complementary: Guardrails says "you can't do that", this says "here's why and what the alternatives are."

## Tools (8)

| Tool | What it does |
|------|-------------|
| `get_compliance_posture` | Overall pass/fail rates by domain and severity for a workspace or catalog. The top-line health check. |
| `query_violations` | List open violations, filterable by domain, severity, owner, resource type, or policy ID. Returns violation_id, resource, policy, first_detected, owner. |
| `get_resource_compliance` | Full compliance status for a specific resource — all policies it's evaluated against, which pass/fail, any active exceptions. |
| `explain_violation` | Plain-language explanation of a violation: what the policy requires, what the resource is missing, and suggested remediation steps. |
| `list_policies` | All active policies with metadata — policy_id, name, domain, severity, applies_to, description. |
| `get_policy` | Full policy definition including rule YAML, description, and recent violation count. |
| `list_exceptions` | Active exceptions, filterable by resource or policy. Shows approver, type, expiry, justification. |
| `get_scan_history` | Recent scan summaries — timestamp, resources scanned, violations found/resolved. Useful for confirming a scan has run recently. |

## Architecture

Identical pattern to `bundles/watchdog-guardrails/` — FastAPI + MCP SSE server deployed as a Databricks App:

```
┌────────────────────────────────────────────┐
│  MCP Client                                │
│  (Claude Code, Cursor, agent framework)    │
└──────────────────┬─────────────────────────┘
                   │ MCP (SSE)
                   ▼
┌────────────────────────────────────────────┐
│  watchdog-mcp (Databricks App)             │
│  • read-only SQL queries to platform.watchdog │
│  • plain-language explanation layer        │
│  • audit log: every tool call recorded     │
└──────────────────┬─────────────────────────┘
                   │ SELECT only
                   ▼
┌────────────────────────────────────────────┐
│  platform.watchdog schema                  │
│  violations, policies, scan_results,       │
│  exceptions, resource_classifications      │
└────────────────────────────────────────────┘
```

The app authenticates to Unity Catalog using a Databricks App service principal with `SELECT` on `platform.watchdog`. No write access — this server cannot modify violations, exceptions, or policies.

## File structure

```
bundles/watchdog-mcp/
├── databricks.yml                    — DAB bundle (alpha/beta/live targets)
├── app.yaml                          — Databricks App manifest
├── resources/
│   └── watchdog_mcp_app.yml          — App resource definition
├── setup.py
└── src/watchdog_mcp/
    ├── __init__.py
    ├── server.py                     — FastAPI + MCP SSE entry point
    ├── config.py                     — env-var config (warehouse ID, catalog, schema)
    ├── audit.py                      — structured audit log for every tool call
    ├── watchdog_client.py            — SQL query layer (read-only)
    └── tools/
        ├── __init__.py
        └── governance.py             — 8 tool implementations
```

## Configuration

| Setting | Env var | Default |
|---------|---------|---------|
| Watchdog catalog | `WATCHDOG_CATALOG` | `platform` |
| Watchdog schema | `WATCHDOG_SCHEMA` | `watchdog` |
| SQL warehouse | `DATABRICKS_WAREHOUSE_ID` | (required) |

## Tool design notes

**`explain_violation`** is the highest-value tool. It takes a `violation_id` and returns a structured explanation:

```
violation: POL-S001 on catalog.hr.employees
policy: pii-requires-steward-and-retention
what's wrong: Table is tagged pii=true but has no data_steward tag and no retention_class tag.
what's needed: Add tags data_steward=<owner-email> and retention_class=<one of: 30d, 90d, 1y, 7y>
how to fix: ALTER TABLE catalog.hr.employees SET TAGS ('data_steward' = 'jsmith@customer.com', 'retention_class' = '7y')
exception path: If this table is exempt, request an exception via the approve_exception notebook.
```

This is what makes the MCP server useful to non-SQL users — Watchdog finds the violation, this explains it in actionable terms.

**`get_compliance_posture`** is the health check. Returns a dict of domain → {total, passing, failing, rate}. AI assistants can call this at the start of a session to know whether governance is clean before operating on data.

## Deployment

```bash
cd bundles/watchdog-mcp
databricks bundle deploy --target alpha
```

MCP endpoint is the app URL + `/sse`. Configure in Claude Code:
```json
{
  "mcpServers": {
    "watchdog": {
      "url": "https://<app-url>/sse",
      "transport": "sse"
    }
  }
}
```

## Activation sequence

1. Deploy `p-watchdog` — base tables must exist.
2. `cd bundles/watchdog-mcp && databricks bundle deploy --target alpha`
3. Confirm app is running: `databricks apps get watchdog-mcp --profile alpha`
4. Test `get_compliance_posture` via curl or MCP inspector.
5. Configure Claude Code with the MCP endpoint.
6. Validate: ask Claude "what governance violations are open today?" — it should call `query_violations` and return results from the live `platform.watchdog` schema.

## TODOs

- [ ] Extract MCP tooling from the watchdog bundle into this standalone bundle — confirm with V4C whether they want to maintain one combined bundle or two separate deployables
- [ ] Decide audit log destination: write to `platform.watchdog.audit_log` (same as Guardrails) or a separate `watchdog_mcp_audit_log` table
- [ ] Add `explain_violation` natural-language templates — these require knowledge of each policy's intent; draft templates alongside `p-watchdog-policies`
- [ ] Rate limiting: the App can be queried in a tight loop by agents — add per-token rate limiting to prevent warehouse saturation
