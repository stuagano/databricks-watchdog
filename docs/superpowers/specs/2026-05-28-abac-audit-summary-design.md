# UC ABAC Audit Summary & Reveal Anomaly Detection

**Status:** Proposed
**Date:** 2026-05-28
**Source:** Imported from an external industry proposal (`p-uc-abac` b-05).
**Builds on:** `2026-04-20-uc-abac-compile-target-design.md` (compile target), `2026-05-28-abac-policy-templates-design.md` (template library).

---

## Problem

The ABAC compile target writes artifacts. The platform enforces them at query time. `system.access.audit` records every read. None of the three speaks to the operator question: **"is the enforcement actually doing its job, or has a user found a side door?"**

Two specific signals get lost in raw audit:

1. **Mask reveals.** When a principal hits a masked column and the platform decides — based on the user's group memberships and the column's audience tag — to return raw data instead of the mask, that's a *reveal*. Reveals are not violations; they're expected for authorized users. But a sudden rate change for one user against one column is one of the cleanest signals of audience-tag drift or group-membership creep.

2. **Row-filter activations.** Same pattern for row filters. Every row that survives the filter for a given user is a permitted view; an unexpected jump in survivors per scan suggests a filter UDF reference broke or an entitlement table widened.

Raw `system.access.audit` is too verbose for either question — operators can't spot the trend without an aggregation layer. There is no aggregator today.

## Goal

A small daily crawler/aggregator that turns audit verbosity into operator-actionable signal. Three components:

1. **`abac_audit_daily` Delta table** — per-day rollup keyed on `(scan_date, principal, table_fqn, column_or_filter)`.
2. **`mask_reveal_anomaly` rule primitive** — flags rollup rows where the reveal rate sits more than N standard deviations above a configurable rolling baseline.
3. **Policy POL-ABAC-AUDIT-001** — "Mask reveal anomalies must be acknowledged within N days." A new operational policy that puts the signal into the existing violation lifecycle.

## Non-Goals

- **Real-time alerting.** Daily aggregation only. Real-time would need a streaming pipeline; out of scope here.
- **Per-query forensics.** The aggregator drops query IDs; operators investigating a specific anomaly query `system.access.audit` directly.
- **Audit log retention.** The platform owns retention of `system.access.audit`. The aggregator reads what's there.
- **Cross-workspace correlation in v1.** Aggregation is per-workspace; cross-workspace rollups can be a follow-up.
- **Detecting unauthorized access.** Reveals are by definition *authorized* — the platform decided to allow them. This spec detects *trends*, not unauthorized reads.

---

## Design

### Input

`system.access.audit` (Databricks-native, populated by UC). Relevant columns:

| Column | Purpose |
|---|---|
| `event_date`, `event_time` | day grain |
| `user_identity.email` | principal |
| `request_params.full_name_arg` | target table fully-qualified |
| `action_name` | filter on `getTable`, `getMetadata`, `getStatement` reads |
| `response.result` | success/fail gate |
| `request_params.column_mask_applied` | platform-emitted flag (where available) |
| `request_params.row_filter_applied` | platform-emitted flag (where available) |

When mask/filter-applied flags are absent (older platform versions), the aggregator falls back to lineage cross-reference: a table has an active mask compile artifact → assume mask was evaluated.

### Output

`{watchdog_catalog}.watchdog.abac_audit_daily` Delta table:

| Column | Type | Notes |
|---|---|---|
| `scan_date` | date | partition column |
| `principal` | string | user or service principal |
| `table_fqn` | string | 3-part name |
| `column_or_filter` | string | column name for masks, `<row_filter>` literal for row filters |
| `enforcement_kind` | string | `column_mask`, `row_filter` |
| `reveal_count` | bigint | reads where the principal saw raw data (mask returned raw) |
| `redact_count` | bigint | reads where the principal saw the masked/filtered output |
| `baseline_30d_avg` | double | rolling 30-day average of `reveal_count` for this `(principal, table_fqn, column_or_filter)` |
| `anomaly_z_score` | double | `(reveal_count - baseline_30d_avg) / stddev_30d`; null if <14 days of history |
| `anomaly_flag` | boolean | true iff `anomaly_z_score >= z_threshold` (default 2.5) |

Partition on `scan_date`. MERGE on `(scan_date, principal, table_fqn, column_or_filter)` so re-runs are idempotent.

### Crawler module

New file `engine/src/watchdog/crawler_abac_audit.py`. Reads the previous day's slice of `system.access.audit` via SQL warehouse, computes the rollup, merges into `abac_audit_daily`. Exposed via:

- A new entrypoint mode: `watchdog crawl --include abac_audit`
- Backfill flag: `--backfill-days N` for first-time deploys

Skipped by default; opt-in per deployment. Customers with no ABAC artifacts deployed get nothing useful from this crawler.

### Rule primitive

In `engine/ontologies/rule_primitives.yml`:

```yaml
mask_reveal_anomaly:
  description: >
    Returns true if abac_audit_daily.anomaly_flag is set for any (principal, table, column)
    tuple in the lookback window.
  params:
    lookback_days: 7
    z_threshold: 2.5     # used by crawler; primitive consumes the boolean
  evaluates: >
    exists abac_audit_daily row where
      scan_date >= current_date - lookback_days
      AND anomaly_flag = true
      AND table_fqn = resource.fqn
```

### Policy

```yaml
- id: POL-ABAC-AUDIT-001
  name: "Mask reveal anomalies must be acknowledged"
  applies_to: MaskedTable    # new ontology class — table with active mask compile artifact
  domain: Security
  severity: high
  description: >
    abac_audit_daily flagged at least one (user, column) tuple as anomalous in the
    last {lookback_days} days. Review the audit roll-up, decide whether the spike
    reflects expected business activity or audience-tag drift, and either approve
    an exception or remediate the audience tag / group membership.
  remediation: >
    1. Query {watchdog_catalog}.watchdog.abac_audit_daily filtered to this table.
    2. Review the principal, the column, and the z-score.
    3. If expected: file an exception (auto-resolves the violation).
    4. If unexpected: remediate the upstream cause (audience tag, group membership,
       UDF reference). Violation resolves on next scan.
  rule:
    ref: mask_reveal_anomaly
```

Severity `high`, not `critical` — anomaly is a signal that *might* be drift; routine spikes (quarter-end, audit prep) are real.

### Ontology

`MaskedTable` — a new derived class in `engine/ontologies/resource_classes.yml`:

```yaml
MaskedTable:
  parent: DataAsset
  classifier:
    has_active_compile_artifact: { target: uc_abac }
```

Tables with no active mask artifact don't classify as `MaskedTable` and aren't evaluated by POL-ABAC-AUDIT-001 — no noise on un-enforced tables.

## Dependencies

- `system.access.audit` available and populated for the workspace.
- `{watchdog_catalog}.watchdog.violations` and the standard scan pipeline (existing).
- Compile-artifact manifest readable so `MaskedTable` classification works (existing — drift detection already consumes it).

## Risks

| Risk | Mitigation |
|---|---|
| Audit log lag from platform (>24h) | Crawler reads `scan_date - 2` window; tolerates 48h lag. |
| First N days have no baseline | `anomaly_z_score` is null for tuples with <14 days of history; `anomaly_flag` is false. |
| Quarter-end activity floods the violation queue | `z_threshold` is configurable; can be raised for known seasonality, or POL severity downgraded per deployment. |
| Platform-emitted `column_mask_applied` flag absent | Fallback to lineage cross-reference (table has artifact → assume mask evaluated). Document the fallback's limitation. |
| Audit retention shorter than 30-day baseline window | Baseline computed from rows present; metric remains valid with shorter history but with reduced confidence. Surface the actual window count in the table for transparency. |

## Order of Operations

1. Add `crawler_abac_audit.py` + entrypoint mode + table DDL.
2. Add `MaskedTable` ontology class.
3. Add `mask_reveal_anomaly` rule primitive.
4. Add POL-ABAC-AUDIT-001 to the ABAC template library (`library/abac-templates/policies/abac_audit.yml`).
5. Add MCP tool surface? Out of scope here — but the existing `get_violations` already covers it once violations land.
6. Unit tests: fixture audit rows → expected rollup → expected anomaly flags.
7. Docs: append a section to the ABAC templates how-to guide.

## Estimated Effort

| Phase | Effort |
|---|---|
| Crawler + table DDL + backfill | 2 days |
| Ontology + primitive + policy | 1 day |
| Unit tests + fixture audit corpus | 1 day |
| Docs | 0.5 days |
| **Total** | **~4.5 days** |
