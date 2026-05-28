# p-watchdog-dashboards — SQL Dashboard Views for Compliance Monitoring

**Date:** 2026-04-14 (updated 2026-05-28)
**Status:** ✅ Superseded — implemented in the standalone watchdog repo
**Branch:** `proposals/stuart-handoff/p-watchdog-dashboards`
**Dependencies:** `p-watchdog` deployed (provides `platform.watchdog` schema with violations, scan_results, exceptions tables)

> **Superseded — see `~/Documents/Projects/databricks-watchdog/engine/dashboards/` and `engine/src/watchdog/views.py` (standalone repo `CustomerDataPlatform/watchdog`).**
>
> Implementation landed after extraction. The Python view generator pattern was kept; the view set diverged from this proposal:
>
> | Proposed | Shipped |
> | --- | --- |
> | v2: `v_compliance_posture`, `v_violation_summary`, `v_violation_trends`, `v_exception_status`, `v_owner_compliance`, `v_domain_health` (6) | v2: `v2_compliance_summary`, `v2_violations_detail`, `v2_violations_by_owner`, `v2_exceptions_active`, `v2_exceptions_history` (5; trends + domain-health rolled into summary) |
> | v3: `v_dq_coverage`, `v_dq_violations`, `v_dq_freshness` (3) | v3: `v3_dq_coverage`, `v3_dq_anomalies`, `v3_dq_summary` (3; freshness folded into anomalies) |
> | — | v5: agent governance views (`v5_agent_executions`, `v5_agent_inventory`, `v5_agent_overview`, `v5_agent_risk`, `v5_ai_gateway`) — beyond original scope |
>
> Keep this file as historical record of the view design. Do not duplicate the SQL into `customer-infra/bundles/watchdog/` — that would fork content against the "no longer syncing into customer-infra" decision.

## Problem

The v1 dashboard shipped with `p-watchdog` is a single admin view — it shows everything to everyone who can access it. In practice, different audiences need different lenses: a platform admin needs overall posture and trend data, a data steward needs to see violations in their domain, and an executive needs a summary without the per-resource detail. Without role-scoped views, the raw `violations` and `scan_results` tables are either shared too broadly or not shared at all.

The second problem is maintainability: ad-hoc SQL dashboard queries become impossible to test or version. When the underlying schema changes, all the queries break. A Python view-generation layer keeps dashboard SQL consistent with the data model.

## What this adds

Two generations of SQL views:

**v2 (compliance posture)** — the primary operational layer. Role-scoped views that aggregate violations, track trends, and surface exception status. These drive the day-to-day compliance workflow.

**v3 (data quality)** — overlays DQM/LHM/DQX coverage metrics on top of v2 compliance data. Connects data quality policy violations to the underlying monitor health.

### v2 views

| View | Audience | What it shows |
|------|---------|--------------|
| `v_compliance_posture` | Admin, data stewards | Overall pass/fail rates by domain, severity, workspace — the top-line compliance score |
| `v_violation_summary` | Admin, data stewards | Open violations grouped by policy, resource type, and owner — the actionable queue |
| `v_violation_trends` | Admin | Daily open/resolved/new counts per domain over the last 30 days — detects drift |
| `v_exception_status` | Admin, stewards | Active exceptions with expiry dates — flags ones expiring in <7 days |
| `v_owner_compliance` | Individual owners | Their own resources' violation status — what they're responsible for |
| `v_domain_health` | Domain stewards | Compliance rate per domain, broken down by policy severity |

### v3 views

| View | Audience | What it shows |
|------|---------|--------------|
| `v_dq_coverage` | Admin, data engineers | DQM/LHM/DQX coverage per catalog and schema — which tables have no monitors |
| `v_dq_violations` | Data engineers | Violations specifically from data quality policies (POL-Q*) with monitor health context |
| `v_dq_freshness` | Data engineers | Tables with freshness SLA violations — last scan time vs. expected cadence |

### Python view generator

`src/watchdog/views.py` generates view DDL from a config dict that specifies:
- Which base tables each view joins
- Column selection and aliases
- Row-level filters (for owner-scoped views: `WHERE owner = current_user()`)
- Any aggregations

The generator ensures views stay in sync with the schema. When a column is added to `violations`, updating `views.py` propagates the change to all downstream views. Dashboard SQL files in `dashboards/` are the generated output — they're committed to the repo and redeployed on schema changes, not generated at query time.

## File structure

```
bundles/watchdog-bundle/
├── src/watchdog/
│   └── views.py                    — view DDL generator
└── dashboards/
    ├── v2_compliance_posture.sql   — v_compliance_posture DDL
    ├── v2_violation_summary.sql    — v_violation_summary DDL
    ├── v2_violation_trends.sql     — v_violation_trends DDL
    ├── v2_exception_status.sql     — v_exception_status DDL
    ├── v2_owner_compliance.sql     — v_owner_compliance DDL
    ├── v2_domain_health.sql        — v_domain_health DDL
    ├── v3_dq_coverage.sql          — v_dq_coverage DDL
    ├── v3_dq_violations.sql        — v_dq_violations DDL
    └── v3_dq_freshness.sql         — v_dq_freshness DDL
```

Views live in the `platform.watchdog` schema alongside the base tables. They're created by a `create_views` task in the DAB bundle that runs after `policy_loader` during deployment.

## Access control model

Views enforce access at query time via row-level filtering — no separate grants needed per consumer.

| View | Access | Mechanism |
|------|--------|-----------|
| `v_compliance_posture` | `customer-platform-admins`, `customer-data-stewards` | Granted SELECT on view |
| `v_violation_summary` | Same + domain stewards | Granted SELECT; domain filter on steward |
| `v_owner_compliance` | All authenticated users | `WHERE owner = current_user()` in view definition |
| `v_exception_status` | `customer-platform-admins`, `customer-data-stewards` | Granted SELECT on view |
| `v3_*` | `customer-data-engineers` + admins | Granted SELECT on views |

Owner-scoped views require no per-user grants — the `current_user()` filter means every user automatically sees only their own resources.

## Activation sequence

1. Deploy `p-watchdog` — base tables must exist before views can be created.
2. Run `python bundles/watchdog-bundle/src/watchdog/views.py generate` to produce DDL files in `dashboards/`.
3. Review generated SQL — confirm column names match your schema version.
4. Deploy bundle with `create_views` task enabled — views are created in `platform.watchdog`.
5. Grant view access: `GRANT SELECT ON VIEW platform.watchdog.v_compliance_posture TO customer-data-stewards`.
6. Build Lakeview (AI/BI) dashboards on top of the views — dashboards reference views, not base tables.

## Code-ready defaults

| Decision | Default to code with | When to revisit |
|----------|---------------------|----------------|
| Entra group for data stewards | `customer-data-stewards` | Update grant SQL when the customer confirms exact group name |
| Entra group for domain stewards | `customer-{domain}-stewards` (parametric) | Substitute actual group names when known |
| `v_owner_compliance` filter | Tag-based: `WHERE owner = current_user()` | Switch to group membership if tag drift becomes a problem in practice |
| v3 DQ views | Include in code but set `active = false` in the `create_views` task | Enable once DQM/LHM adoption confirmed |

Grant SQL is a one-line change per group — V4C ships the views and runs grants once group names are confirmed. The view logic itself doesn't depend on group names.

## TODOs (post-coding)

- [ ] Confirm Entra group names for data stewards and domain stewards: run the grant statements with actual names
- [ ] Build Lakeview dashboard tiles for the compliance review meeting (post-views, separate from view SQL)
- [ ] Confirm DQM/LHM monitor adoption in alpha before enabling v3 views
