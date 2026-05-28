# Dynamic-Views Compile Target (ABAC Fallback)

**Status:** Proposed
**Date:** 2026-05-28
**Source:** Imported from an external industry proposal (`p-uc-abac` Dynamic-Views fallback / b-FB).
**Builds on:** `2026-04-20-uc-abac-compile-target-design.md` (which calls dynamic views "the workable fallback if ABAC GA slips").

---

## Problem

The existing `uc_abac` compile target emits column-mask artifacts that depend on UC ABAC being available in the deployment region. UC ABAC remains Public Preview / Beta in many regions; customers in those regions can author policies in YAML but cannot enforce them.

The conventional workaround — dynamic views built on `is_account_group_member()` — is well-understood and widely deployed, but each customer reinvents the DDL. Without a compile target, the policy YAML and the view DDL drift: someone updates the YAML, no view rebuild happens, enforcement falls behind intent.

## Goal

A new compile target `uc_dynamic_view` that emits SQL DDL (`CREATE OR REPLACE VIEW`) from the same policy YAML the `uc_abac` target consumes. Customers compile once to whichever target their region supports; when ABAC GAs, they re-compile to `uc_abac` and drop the dynamic views.

## Non-Goals

- **Auto-deploying the DDL.** The compiler emits artifacts; the deployer (separate component) executes them. Same separation as the existing target.
- **Maintaining the underlying view objects.** The artifact contains DDL; running it creates/refreshes the view. Lifecycle (drop, rename, view-of-a-view) is the deployer's concern.
- **Column-mask equivalence.** Dynamic views can mask whole rows or substitute column expressions per-row, but cannot truly *redact a column for some users and not others* without `CASE WHEN ... THEN raw ELSE mask END` per column — which works but bloats DDL. The target supports this pattern with an explicit `columns:` block; complex multi-column policies should stay on `uc_abac` once it's GA.
- **Row-filter compile target.** Dynamic views can implement row-filter semantics via `WHERE` clauses. A separate row-filter target may also emit dynamic-view DDL — that's covered by the row-filter target's own spec.
- **Cross-database view chains.** v1 emits one view per base table; chained views are not generated.

---

## Config Shape

The same `compile_to:` block accepts a new target:

```yaml
compile_to:
  - target: uc_dynamic_view
    base_table: main.silver.dosage_records      # required, 3-part name
    view_name: main.governed.dosage_records_v   # required, target view name
    mask_columns:                                # optional; column-mask semantics
      - column: patient_id
        mask_function: governance.mask_hash      # UDF for the redacted value
        unmask_when_group: phi_audience          # optional, account-group name
    row_filter:                                  # optional; row-filter semantics
      predicate: "region IN (SELECT region FROM governance.user_regions WHERE user = current_user())"
      bypass_when_group: sales_global            # optional
```

Either `mask_columns` or `row_filter` (or both) must be present. The compiler validates:

- `base_table` and `view_name` are 3-part identifiers
- `view_name` is not in the same schema as `base_table` unless it differs by name
- Every `mask_function` is a 3-part UDF identifier (not verified against the workspace — same convention as `uc_abac`)
- At most one `row_filter` block per artifact
- Account-group references in `unmask_when_group` / `bypass_when_group` are not validated against the workspace (same convention)

## Artifact Shape

Stored at `uc_dynamic_view/{policy_id}.sql`. Content is a single SQL statement:

```sql
-- Watchdog dynamic-view artifact
-- policy_id: POL-PHI-001
-- generated: 2026-05-28T14:00:00Z
-- source-hash: sha256:7d...e2
--
-- Replaces UC ABAC enforcement for environments where ABAC is not yet GA.
-- When ABAC GAs in this region, recompile this policy to target uc_abac and
-- drop this view.

CREATE OR REPLACE VIEW main.governed.dosage_records_v AS
SELECT
  -- masked columns
  CASE
    WHEN is_account_group_member('phi_audience') THEN patient_id
    ELSE main.governance.mask_hash(patient_id)
  END AS patient_id,
  -- pass-through columns
  dose_mSv,
  detector_id,
  measurement_ts
FROM main.silver.dosage_records
WHERE
  -- row filter
  is_account_group_member('sales_global')
  OR (region IN (SELECT region FROM main.governance.user_regions WHERE user = current_user()))
;
```

Header comments are part of the artifact. The `source-hash` enables drift detection (same primitive the manifest already uses for other targets).

Pass-through columns are enumerated explicitly — the compiler reads `information_schema.columns` for `base_table` at compile time to expand `*`. This catches schema changes (a new column appears upstream → drift detection flags the artifact as stale).

## Compatibility With `uc_abac`

The two targets accept compatible-but-not-identical YAML. A policy can declare *both*:

```yaml
compile_to:
  - target: uc_abac
    mask_function: governance.redact_phi
  - target: uc_dynamic_view
    base_table: main.silver.dosage_records
    view_name: main.governed.dosage_records_v
    mask_columns:
      - column: patient_id
        mask_function: governance.mask_hash
        unmask_when_group: phi_audience
```

Customers in pre-GA regions deploy `uc_dynamic_view`; customers in GA regions deploy `uc_abac`; customers mid-migration deploy both and consumers query whichever they're routed to.

## Drift Detection

Same model as other targets:

- Artifact content is hashed (sorted SQL tokens, comments stripped before hashing for stability).
- Drift detection reads the manifest and compares the deployed view's DDL (via `SHOW CREATE VIEW`) against the hash.
- Schema drift on `base_table` invalidates the artifact (because pass-through column list embedded in the SQL goes stale).

## Dependencies

- `2026-04-20-uc-abac-compile-target-design.md` — the target registry pattern.
- `engine/src/watchdog/compiler.py` — target registry; one-line wiring to add the new target.
- UDF library convention (from `2026-05-28-abac-policy-templates-design.md`) — same `governance.*` UDFs work for both targets.

## Risks

| Risk | Mitigation |
|---|---|
| Customers leave dynamic views in place after ABAC GAs | Drift detection includes a freshness check; once `uc_abac` artifacts exist for the same policy, the `uc_dynamic_view` artifact emits a deprecation warning in the artifact header. |
| Pass-through column list goes stale when base table evolves | Schema drift detection (mentioned above) flags the artifact; recompile produces fresh DDL. |
| View-on-view chains harm performance | v1 emits one view per base table only. Customers building multi-layer views do so outside the compiler. |
| Complex multi-column masks bloat DDL | Acceptable for v1; complex policies move to `uc_abac` once GA. |
| Account-group naming convention mismatches across regions | Group names are passed through verbatim from YAML; no normalization. Document the convention in the how-to. |

## Order of Operations

1. Add `UCDynamicViewTarget` to `engine/src/watchdog/compiler.py` target registry.
2. Implement compile logic in a new module `engine/src/watchdog/compile_targets/uc_dynamic_view.py`.
3. Extend the compile-manifest schema (additive — new target name only).
4. Extend drift detection to read the deployed view via `SHOW CREATE VIEW` and compare.
5. Unit tests: each option (mask only, filter only, combined) plus a schema-drift case.
6. Docs: extend `docs/guide/how-to/abac-templates.md` with the dual-target deployment pattern.

## Estimated Effort

| Phase | Effort |
|---|---|
| Target implementation + registry wiring | 2 days |
| Drift extension | 1 day |
| Tests | 1 day |
| Docs | 0.5 days |
| **Total** | **~4.5 days** |
