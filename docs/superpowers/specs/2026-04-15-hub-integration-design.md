# Hub Integration: Schema-First Contract Hardening

**Status:** Design approved
**Date:** 2026-04-15
**Cycle:** 1 of 3 (Hub Integration → Drift Detection → Remediation Agents)

---

## Problem

Watchdog produces 14 compliance views. Six of them are designed for consumption by the Governance Hub (and by extension, Ontos and Guardrails). But:

1. **No explicit contract** — consumers don't know what columns to expect, what types they are, or what they mean. The only reference is the view SQL itself.
2. **Broken dependencies** — at least two views (`v_tag_policy_coverage`, potentially others) reference Delta tables that may not exist at query time (`policies`, `exceptions`).
3. **No automated validation** — nothing checks that the views conform to a stable schema. A change to `views.py` could silently break a Hub panel.
4. **No deployment verification** — an SA deploys Watchdog and has no way to confirm the Hub-facing views are alive and correctly shaped.

## Goals

- Define an explicit, versioned schema contract for all Hub-facing views
- Fix any broken view dependencies (missing tables, bad joins)
- CI-level integration tests that validate views against the contract
- A live smoke test notebook SAs run post-deployment

## Non-Goals

- New views beyond the 6 already implemented
- Changes to the crawler, rule engine, or ontology
- UI work in the Hub itself
- Performance optimization of view queries

---

## Design

### 1. Hub Contract File

**File:** `engine/hub_contract.yml`

A YAML file defining the schema each Hub-facing view must conform to. This is the single source of truth — tests validate against it, the smoke notebook validates against it, and SAs reference it during deployment.

**Views covered:**

| View | Hub Panel | Grain |
|---|---|---|
| `v_domain_compliance` | Governance Dashboard overlay | 1 row per domain |
| `v_class_compliance` | Drill-down by ontology class | 1 row per class |
| `v_resource_compliance` | Drill-down by resource | 1 row per (resource, class) |
| `v_tag_policy_coverage` | Tag compliance panel | 1 row per (resource, policy) |
| `v_data_classification_summary` | Classification coverage panel | 1 row per catalog |
| `v_dq_monitoring_coverage` | DQ monitoring panel | 1 row per table |

**Contract structure per view:**

```yaml
views:
  v_domain_compliance:
    description: "Aggregated compliance posture per governance domain"
    grain: "1 row per domain"
    hub_panel: "Governance Dashboard overlay"
    columns:
      - name: domain
        type: STRING
        nullable: false
        description: "Governance domain (e.g., SecurityGovernance, CostGovernance)"
        example: "SecurityGovernance"
      - name: resources_affected
        type: BIGINT
        nullable: false
        description: "Count of distinct resources with violations in this domain"
        example: 2352
      # ... (all columns defined)
```

### 2. Schema Audit & Fixes

Four issues identified in the existing views:

**Issue 1: `v_tag_policy_coverage` depends on a `policies` Delta table that doesn't exist.**

The engine loads policies from YAML into `PolicyDefinition` dataclasses but never persists them to Delta. The view does `CROSS JOIN {catalog}.{schema}.policies` — fails at query time.

**Fix:** Add a `write_policies_table()` function to `policy_engine.py` that writes active policies to a `policies` Delta table during each scan. Schema:

```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.policies (
    policy_id STRING NOT NULL,
    policy_name STRING NOT NULL,
    applies_to STRING,
    domain STRING,
    severity STRING,
    description STRING,
    remediation STRING,
    active BOOLEAN,
    updated_at TIMESTAMP
)
```

Call this in `evaluate_all()` before writing scan results.

**Issue 2: `v_tag_policy_coverage` references an `exceptions` table.**

**Fix:** Verify whether `exceptions` is a standalone table or tracked inline on `violations`. If standalone, ensure `violations.py` creates it. If inline, rewrite the view's LEFT JOIN to use `violations.status = 'exception'` instead.

**Issue 3: `v_data_classification_summary` uses `ri.domain` as `catalog_name`.**

The `resource_inventory` schema has a `domain` column, but for tables the catalog name comes from parsing `resource_name`. If `domain` isn't consistently populated, the view returns nulls.

**Fix:** Audit the crawler to verify `domain` population. If inconsistent, derive catalog from `SPLIT(resource_name, '.')[0]` as a fallback in the view SQL.

**Issue 4: `v_dq_monitoring_coverage` assumes DQ-enrichment tags.**

Tags `dqm_enabled`, `lhm_enabled`, `dqm_anomalies`, `dqm_metrics_checked` must be populated by the crawler's DQ system table enrichment.

**Fix:** Verify the crawler produces these tags. If the DQ crawler is a stub or optional, the view should degrade gracefully (show `none` for monitoring_status) rather than error.

### 3. Integration Tests

**File:** `tests/unit/test_hub_contract.py`

Uses existing PySpark test fixtures from `conftest.py`.

**Test structure:**

1. **Contract loader** — reads `hub_contract.yml`, parses expected schema per view
2. **Synthetic data fixture** — creates minimal Delta tables (`resource_inventory`, `violations`, `resource_classifications`, `scan_results`, `policies`, `exceptions`) with enough variety to exercise every view:
   - Mix of open/resolved/exception violations
   - Multiple domains and ontology classes
   - Tables with and without DQ tags
   - Resources with and without classifications
3. **Per-view schema test** — for each of the 6 views: create the view, query it, assert column names and types match the contract exactly
4. **Per-view content tests** — assert synthetic data produces expected aggregations:
   - `v_domain_compliance` groups by domain correctly
   - `v_class_compliance` calculates `compliance_pct` correctly
   - `v_tag_policy_coverage` shows `not_evaluated` for resources with no scan results
   - `v_data_classification_summary` computes `classification_pct` and `stewardship_pct`
   - `v_dq_monitoring_coverage` maps monitoring_status enum correctly
5. **Edge case tests** — empty inventory, zero violations, resource with no classifications. Views should return empty results or zeros, never error.

### 4. Live Smoke Test Notebook

**File:** `engine/notebooks/hub_smoke_test.py`

Databricks notebook (`.py` with `# COMMAND ----------` separators) for SA post-deployment verification.

**What it does:**

1. Reads the contract from `hub_contract.yml` (bundled in the deployment)
2. For each of the 6 views, runs `SELECT * FROM {catalog}.{schema}.{view_name} LIMIT 10`
3. Validates:
   - View exists and is queryable (no broken dependencies)
   - Column names and types match the contract
   - At least 1 row returned (warns if empty — flags "did you run a scan first?")
4. Outputs a summary table:

```
View                            Status    Rows    Schema Match    Notes
v_domain_compliance             PASS      5       ✓               
v_class_compliance              PASS      12      ✓               
v_resource_compliance           PASS      847     ✓               
v_tag_policy_coverage           WARN      0       ✓               No scan results yet
v_data_classification_summary   PASS      3       ✓               
v_dq_monitoring_coverage        PASS      134     ✓               
```

**What it doesn't do:** Assert business logic — that's the integration tests' job. It confirms views are alive and shaped correctly on a real workspace.

---

## Files Changed & Created

| Action | File | What |
|---|---|---|
| Create | `engine/hub_contract.yml` | Schema contract for 6 Hub-facing views |
| Create | `tests/unit/test_hub_contract.py` | Integration tests validating views against contract |
| Create | `engine/notebooks/hub_smoke_test.py` | Live workspace smoke test notebook |
| Edit | `engine/src/watchdog/views.py` | Fix broken dependencies in existing views |
| Edit | `engine/src/watchdog/policy_engine.py` | Write `policies` Delta table during scan |
| Edit | `engine/src/watchdog/violations.py` | Ensure `exceptions` table exists if needed |

## Out of Scope

- Crawler changes
- Rule engine changes
- MCP tools, ontos-adapter, guardrails
- New views beyond the existing 6
- Hub UI work
