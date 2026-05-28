# p-uc-abac — Row- and Column-Level Enforcement via UC ABAC

**Date:** 2026-05-13
**Status:** Proposal (no code)
**Branch:** (none yet — proposal-only on `develop`)
**Dependencies:** `p-data-classification` deployed (provides the column tags ABAC reads), `p-watchdog-policies` (tag taxonomy + governed tag policies), `p-uc-isolation` (defines which catalogs are isolation boundaries that policies attach to)
**Roadmap:** Phase 3 — Enforcement layer on top of the classification substrate.
**Priority:** Tier 2 — Required for regulated workloads (HIPAA cohort filtering, export-control redaction). Not blocking the catalog's launch; gates the "regulated data live in the same workspace as everyone else" pattern.

> For the surrounding governance stack see `docs/customer-catalog/architecture.md`; this proposal slots in as the **enforcement** rung between Watchdog tagging and the Customer Catalog read-side UI.

---

## Problem

Today's stack tags everything correctly and *displays* the labels in the catalog — but every consumer with `SELECT` on a table still sees every row and every column. Three concrete consequences at the customer:

1. **HPGe R&D data** carries `export_controlled = ITAR` on specific columns. Today the only way to keep US-persons-only access is to fork the dataset and grant narrowly. A second copy of every detector dataset is operationally untenable.
2. **Dosimetry records** carry `pii_category = phi`. Analysts need everything *except* the PHI columns to build aggregates; the current pattern is per-team views, which drift from source.
3. **Sales territory data** is owned per region. A territory analyst needs only their rows, but the underlying table is one ROW-level entity. No per-user row scoping exists.

The fix isn't more tagging — Watchdog already produces the right labels. The fix is **enforcement** that consumes those labels at query time.

## What UC ABAC actually buys

Unity Catalog ABAC (Attribute-Based Access Control) lets you write a policy once that names a tag, and UC applies it everywhere that tag appears. Two enforcement primitives:

| Primitive | Shape | What it does |
| --- | --- | --- |
| **Column mask** | `MASK FUNCTION ... ON COLUMN` (via ABAC policy on a tag) | When the policy matches, the function rewrites the column at query time. Common shapes: full redact (`'***'`), partial reveal (`LEFT(ssn,3) \|\| '-**-****'`), hash (`sha2(col, 256)`), or unmodified pass-through when the requester has an exempting tag/role. |
| **Row filter** | `ROW FILTER ... ON TABLE` (via ABAC policy on a tag) | When the policy matches, the function returns a predicate that's `AND`-ed into every query against the table. Common shapes: `region = user_region()`, `classification <= user_clearance()`, `customer_id IN (SELECT ... FROM entitlements WHERE user = current_user())`. |

The crucial property is that the **policy targets a tag, not a table**. Tag `pii_category = phi` onto a new column and the mask follows automatically. Watchdog's tag-emitter is already the right upstream.

## Options Considered

| Option | Substrate | Where masks live | Drift risk | Notes |
| --- | --- | --- | --- | --- |
| **Per-team views** (today) | Per-team schemas in same catalog | View DDL | High — views fork from source | Operationally how we cope without ABAC. Doesn't scale beyond ~5 cohorts. |
| **Copy-per-cohort tables** | Per-cohort catalogs in `p-uc-isolation` | Copy logic in DLT | High — copies stale; storage 2-Nx | Storage and freshness cost is the killer. |
| **External proxy (Immuta/Privacera)** | Same UC, external policy engine | Proxy intercepts queries | Medium | Adds a network hop, separate license, separate audit surface. |
| **UC ABAC** *(this proposal)* | Native UC | One policy per `(tag, audience)` | Low — UC catalog is source of truth | Currently Preview/Beta; GA status before deploy is gating. |
| **Dynamic views with `current_user()`** | Same UC, view DDL | View DDL with `IS_ACCOUNT_GROUP_MEMBER` | Medium — one view per gating rule, still per-table | The workable fallback if ABAC GA slips. Loses tag-driven inheritance. |

**Why ABAC wins for the customer:** tag-driven inheritance is the only option that keeps Watchdog's outputs as the *enforcement substrate*, not just a labeling layer. Every other option means re-encoding the same classification in a second place that can drift.

## Scope

### b-01: Tag taxonomy for ABAC

ABAC reads from UC governed tags. Two new categories beyond what Watchdog tags today:

- **`audience.*`** — *exempting tag* on principals (groups). E.g., `audience.compliance_officer = true`, `audience.itar_us_person = true`. Policies use these to decide pass-through vs mask.
- **`policy_class.*`** — *grouping tag* on columns/rows. E.g., `policy_class = high` collects multiple `pii_category` and `export_controlled` values under one redaction policy. Lets us write 5 policies, not 50.

These tags get YAML-defined alongside the existing Watchdog policies in `bundles/watchdog/policies/*.yml`. CI validates that every column carrying `pii_category` or `export_controlled` also carries the appropriate `policy_class`.

### b-02: Mask + filter function library

A small SQL function library at `{watchdog_catalog}.governance.*`:

| Function | Returns | Used by |
| --- | --- | --- |
| `mask_full(value)` | `'***'` typed-correct | Column masks for `pii_category = phi`, `ssn`, etc. |
| `mask_partial_id(value)` | `LEFT(value, 3) \|\| '-**-****'` | Display-friendly partial reveal |
| `mask_hash(value)` | `sha2(value, 256)` | Joinable redaction for join-key columns |
| `filter_user_region(region)` | `region IN (SELECT region FROM user_regions WHERE user = current_user())` | Row filter for sales territory |
| `filter_clearance(level)` | `level <= user_clearance()` | Row filter for export-controlled rows |

These live in a single `governance` schema in the watchdog catalog, owned by the watchdog SP. Deployment is `bundles/watchdog/sql/governance_functions.sql`.

### b-03: ABAC policy definitions (YAML)

YAML at `bundles/watchdog/abac/*.yml`, applied via a small DAB job that calls the UC REST API:

```yaml
policy_id: pol-phi-mask
name: PHI column redaction
target:
  on_tag: pii_category
  values: [phi, phi_dob, phi_mrn]
enforcement: column_mask
function: governance.mask_full
exempt:
  group_tags: [audience.compliance_officer]
```

Same pattern for row filters:

```yaml
policy_id: pol-sales-region
name: Sales territory row scope
target:
  on_tag: policy_class
  values: [sales_territory]
enforcement: row_filter
function: governance.filter_user_region
exempt:
  group_tags: [audience.sales_global]
```

Apply pipeline lives at `bundles/watchdog/abac/apply.py`. Reads YAML → diffs against UC current state → posts adds/updates/deletes. Idempotent, dry-run flag.

### b-04: Catalog surfacing (read-only)

The Customer Catalog already has the right place to show enforcement: the table detail and column detail panes. Three additions:

- **Column pane** — when a column has an active mask policy, show a chip "🛡 Masked" with the policy name. Hover reveals the function used. Already-present `pii_category` badge sits alongside.
- **Table pane** — when an active row filter applies, show "🛡 Row filter: <policy_name>" near the schema header. Click → policy detail view (existing `/policies/:id` route).
- **Policy detail** — extends the policy-viewer policy viewer to render mask/filter policies. Includes the `exempt.group_tags` list so consumers know which audience tag would let them through.

All read-only; authoring stays in YAML-in-Git.

### b-05: Audit surface

ABAC enforcement events go to `system.access.audit`. A small Watchdog rule polls daily and writes a summary to `{watchdog_catalog}.watchdog.abac_audit_daily`:

- Count of masked column reads per `(user, table, column)`
- Count of row-filter activations per `(user, table)`
- Anomalies: users whose mask-reveal rate suddenly changes (suggests audience-tag drift)

Surfaced in the catalog under a new **Policies → Audit** tab.

### b-06: Watchdog policy for ABAC drift

A new Watchdog policy (`policies/governance/pol-abac-coverage.yml`):

- For every column tagged `pii_category` in `(phi, phi_dob, phi_mrn, ssn)` → assert an active mask policy matches.
- For every table tagged `policy_class = sales_territory` → assert an active row filter exists.

Violations open like any other Watchdog finding. Same exception workflow.

## Why not external proxies

| | UC ABAC | Immuta / Privacera proxy |
| --- | --- | --- |
| Substrate | UC tags (Watchdog already emits) | Separate policy DSL, must mirror |
| Performance | Native, same query path | Proxy hop adds latency + becomes failure point |
| Audit | `system.access.audit` (same surface as everything else) | Proxy's own audit log |
| Cost | Included with UC | Per-seat license |
| Lock-in | UC native | Vendor-specific |
| Capability today | Preview/Beta | GA |

The only real reason to choose a proxy is "we need this in production now and ABAC isn't GA yet" — the Dynamic Views option (below) is the better stopgap because it stays inside UC.

## Dependencies

| Prereq | Status | Why it matters |
| --- | --- | --- |
| `p-data-classification` deployed | 🟢 Coded | Without `pii_category` / `export_controlled` tags on columns, no mask has anything to match. |
| `p-watchdog-policies` | 🟢 Coded | Tag policies + governed-tag-allowed-values come from this. |
| `p-uc-isolation` | 🟢 Coded | ABAC policies attach at the metastore level but materially matter only when isolation_mode bounds where they apply. |
| UC ABAC GA in the FE Stable / the customer regions | 🔴 Pending | Gating. Preview today; GA timeline tracked separately. |

## Related Work

- [p-data-classification(P2)](./p-data-classification\(P2\).md) — the upstream tagging this consumes.
- [p-watchdog-policies(P2)](./p-watchdog-policies\(P2\).md) — same YAML-in-Git pattern; ABAC policies join the same review surface.
- [p-watchdog-exceptions(P2)](./p-watchdog-exceptions\(P2\).md) — exceptions to mask/filter policies reuse the same workflow.
- [p-customer-catalog(P3)](./p-customer-catalog\(P3\).md) — read-side surfacing (b-04 above) extends policy-viewer / column detail.

## Data Replication Cost

**None for ABAC itself** — that's the point. The whole proposal exists to avoid the copy-per-cohort and view-per-cohort patterns that would otherwise replicate data. Minor cost contributions:

- `abac_audit_daily` table — KB/day, immaterial.
- `user_regions` / `user_clearance` entitlement tables for row filters — one row per (user, scope) pair, ~MB at the customer scale.

## Risks

| Risk | Mitigation |
| --- | --- |
| ABAC stays Preview through 2026 | Land Dynamic Views fallback (b-FB) on the same tag taxonomy; switch policies over when ABAC GAs. The YAML shape can be designed to compile to either. |
| Mask function performance on large fact tables | UC pushes masks to scan; published benchmarks suggest <5% overhead. Validate during b-02 with TPC-H scaled to dosimetry size. |
| Drift between YAML and UC | b-03's apply pipeline is the only writer; b-06 Watchdog policy catches drift. |
| Audience tags grow unbounded ("just add another exempting tag") | YAML schema requires every `audience.*` tag to map to a documented business rationale. CI fails on undocumented additions. |
| Row filters reference offline entitlement table that lags | Materialize `user_regions` from the source-of-truth identity system; Watchdog policy on staleness >24h. |

## Order of Operations

1. Confirm UC ABAC GA timeline for the FE Stable / the customer regions.
2. b-01 tag taxonomy — additive, no enforcement yet.
3. b-02 function library — additive, no enforcement yet.
4. b-03 policy YAML + apply pipeline — dry-run mode first; one canary policy on one canary column for two weeks.
5. b-04 catalog read-side surfacing.
6. b-06 drift policy (after canary stabilizes).
7. b-05 audit summary.
8. Roll forward to all `pii_category` / `export_controlled` columns once canary holds.

## Estimated Effort

| Phase | Effort | Status |
| --- | --- | --- |
| b-01 tag taxonomy | 2 days | Pending |
| b-02 function library | 3 days | Pending |
| b-03 apply pipeline + 5 canary policies | 5 days | Pending |
| b-04 catalog surfacing | 3 days (UI work) | Pending |
| b-05 audit surface | 2 days | Pending |
| b-06 drift watchdog policy | 1 day | Pending |
| Dynamic-Views fallback (b-FB) | 5 days | Conditional on ABAC GA timing |

## Customer/Business Decisions Still Open

- **Initial policy scope** — which Watchdog-tagged columns get masked first. Recommend `pii_category` (PHI subset) as the canary; everything else follows once the pattern's proven.
- **Audience tag governance** — who can grant `audience.compliance_officer` etc.? Same Entra group that owns Watchdog exception approval, or separate?
- **Row-filter entitlement source** — `user_regions`, `user_clearance` etc. need an authoritative source. Pulling from Workday vs. a customer-internal directory vs. Watchdog-managed UC table is a customer decision.
- **Stopgap stance** — if ABAC GA slips, do we ship the Dynamic Views fallback in production or wait? Operational risk on regulated datasets is the input to this call.
