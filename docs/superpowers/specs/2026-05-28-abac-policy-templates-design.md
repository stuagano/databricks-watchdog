# UC ABAC Policy Templates & Mask/Filter UDF Library

**Status:** Proposed
**Date:** 2026-05-28
**Source:** Imported from an external industry proposal (`p-uc-abac`).
**Builds on:** `2026-04-20-uc-abac-compile-target-design.md` (compile mechanism).

---

## Problem

The UC ABAC compile target lets a watchdog policy emit a column-mask artifact. What's missing is the policy *content* customers should write to address the three enforcement patterns that come up over and over in regulated-industry deployments:

1. **PHI / PII column redaction** — masking sensitive columns on tables consumers can otherwise see (HIPAA, GDPR contexts).
2. **Export-control column redaction** — same shape as PHI/PII but the audience condition is citizenship/role rather than data category (ITAR, EAR).
3. **Per-user row scoping** — row filters that restrict to rows the current user is entitled to (territory, department, clearance).

Today the compile-target spec covers *how* to emit an artifact, the permissions-compiler reference shows *toy examples*, but there's no opinionated library of policy templates a customer can drop in. They reinvent the YAML each time, reinvent the SQL mask/filter UDFs each time, and the structural choices drift.

## Goal

A small, opinionated **template library** at `library/abac-templates/` with two parts:

1. **Three policy YAML templates** matching the three enforcement patterns, parameterized by tag and audience.
2. **A SQL UDF reference** (`library/abac-templates/sql/governance_functions.sql`) with the standard mask and filter functions the templates reference. Same convention as the permissions-compiler reference — copy-and-adapt, not a runtime dependency.

## Non-Goals

- **The compile target.** That ships in `2026-04-20-uc-abac-compile-target-design.md`.
- **The deployer / apply pipeline.** UC `SET COLUMN MASK` / `SET ROW FILTER` execution is the deployer's job.
- **Audience tag taxonomy enforcement.** The source proposal calls for `audience.*` / `policy_class.*` governed tag conventions enforced via CI. That can be a follow-up — the templates accept audience tag keys as parameters but don't mandate naming.
- **Row-filter compile target.** The current ABAC compile target covers column masks only. Row-filter target is a separate spec (called out as a non-goal in the compile-target design).
- **Mask function lifecycle (create/update/verify UDFs).** Same non-goal as the compile-target spec — the UDFs are a user-authored dependency.

---

## The Three Templates

### Template 1 — Column mask by tag

Use case: "every column tagged X must be masked except for principals in audience Y."

```yaml
# library/abac-templates/templates/column_mask_by_tag.yml
policy_id: POL-ABAC-MASK-{tag_value}
name: "Mask columns tagged {tag_key}={tag_value}"
applies_to: PIIColumn          # ontology class; override per deployment
domain: Security
severity: critical
description: "Columns where {tag_key}={tag_value} must be masked at query time."
rule:
  ref: column_has_tag
  param:
    key: "{tag_key}"
    value: "{tag_value}"
compile_to:
  - target: uc_abac
    mask_function: "{catalog}.governance.{mask_fn}"   # e.g. mask_full, mask_hash
    apply_when: "{scope_note}"                        # optional human-readable note
```

Covers PHI/PII and export-control redaction. Choice of `mask_fn` selects redaction style (full, partial, hash).

### Template 2 — Row filter by user attribute

Use case: "consumers see only rows where a column matches an attribute of the current user."

```yaml
# library/abac-templates/templates/row_filter_by_user_attribute.yml
policy_id: POL-ABAC-ROW-{attribute}
name: "Row-scope by user {attribute}"
applies_to: ScopedTable        # ontology class identifying tables that carry the scope column
domain: Security
severity: high
description: >
  Rows are visible only when {scope_column} matches the current user's {attribute}
  as resolved via {entitlement_table}.
rule:
  ref: table_has_scope_column
  param:
    column: "{scope_column}"
compile_to:
  - target: uc_row_filter      # see Non-Goals — separate compile target, future
    filter_function: "{catalog}.governance.filter_user_{attribute}"
    apply_when: "{scope_note}"
```

Covers territory/department/region scoping. Depends on a future `uc_row_filter` compile target.

### Template 3 — Row filter by cohort tag

Use case: "consumers see only rows whose `policy_class` matches an entitlement the user holds."

```yaml
# library/abac-templates/templates/row_filter_by_cohort_tag.yml
policy_id: POL-ABAC-COHORT-{cohort}
name: "Cohort row filter — {cohort}"
applies_to: CohortTable
domain: Security
severity: critical
description: >
  Only principals with the {audience_tag} entitlement see rows in the {cohort} cohort.
rule:
  ref: table_has_cohort_column
  param:
    cohort: "{cohort}"
compile_to:
  - target: uc_row_filter
    filter_function: "{catalog}.governance.filter_cohort_{cohort}"
    apply_when: "audience tag: {audience_tag}"
```

Covers regulatory cohort filtering (e.g. study cohorts, regulatory regimes).

## SQL UDF Reference

`library/abac-templates/sql/governance_functions.sql` — opinionated reference UDFs that the templates assume exist. Live in a `governance` schema in the customer's watchdog catalog. **Customers copy and adapt** — not deployed by the engine.

| Function | Signature | Returns |
|---|---|---|
| `mask_full(value)` | `(value ANY) → ANY` | typed `'***'` sentinel |
| `mask_partial(value)` | `(value STRING) → STRING` | left-N-then-redacted (`LEFT(value,3) \|\| '***'`) |
| `mask_hash(value)` | `(value STRING) → STRING` | `sha2(value, 256)` — joinable redaction |
| `filter_user_attribute(scope, entitlement_table, attribute)` | `(scope ANY) → BOOLEAN` | `true` iff `scope IN (SELECT … FROM {entitlement_table} WHERE user = current_user())` |
| `filter_cohort(cohort, audience_tag)` | `(cohort STRING) → BOOLEAN` | `true` iff caller carries `audience_tag` via UC group membership |

The reference file is a single `.sql` with `CREATE OR REPLACE FUNCTION` statements. Customers run it once per environment as part of their watchdog catalog bootstrap.

## Drift Watchdog Policy

A new policy at `library/abac-templates/policies/abac_coverage.yml` asserts that for every table classified into `PIIColumn` / `ScopedTable` / `CohortTable`, an active compile artifact exists. This catches "policy templated but never deployed" drift. Plugs into the existing drift detection pipeline.

## Dependencies

- `2026-04-20-uc-abac-compile-target-design.md` (compile target) — landed.
- `uc_row_filter` compile target — does not yet exist. Templates 2 and 3 require it. Track in a follow-up spec.
- Ontology classes `PIIColumn` (exists), `ScopedTable`, `CohortTable` — last two are new and added by the template library's `library/abac-templates/ontologies/resource_classes.yml`.

## Risks

| Risk | Mitigation |
|---|---|
| Customers think the SQL UDFs are a runtime dependency | README explicitly frames them as reference / copy-and-adapt, parallel to `template/permissions-compiler/`. |
| Template parameterization confuses authors | Each template ships with a worked example `library/abac-templates/examples/` showing `{tag_key}` filled in. |
| ABAC stays Public Preview / Beta in target environments | Templates compile to artifacts whether or not ABAC is enforced — they remain useful as documentation of intent, and the drift policy still tracks coverage. |
| Row-filter target not yet built | Templates 2/3 ship as "uncompilable until row-filter target lands"; column-mask template is fully usable today. |

## Order of Operations

1. Add `library/abac-templates/` directory with README, three template files, examples, and the SQL reference file.
2. Add `ScopedTable` and `CohortTable` ontology classes under the template library's ontology file.
3. Add `column_has_tag`, `table_has_scope_column`, `table_has_cohort_column` rule primitives (or confirm equivalents already exist).
4. Write the drift policy `abac_coverage.yml`.
5. Documentation: add `docs/guide/how-to/abac-templates.md` explaining the workflow (apply template → fill parameters → deploy mask UDF → enable via compile artifact).
