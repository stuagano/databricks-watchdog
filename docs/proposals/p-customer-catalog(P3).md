# p-customer-catalog — customer-branded Data Catalog with Curation, Exceptions, and Ontology

**Date:** 2026-05-12
**Status:** Coded on branch
**Branch:** `stuart/d-customer-catalog-P3`
**Implements:** A customer-tailored React + FastAPI catalog over Lakebase that curates UC assets, surfaces Watchdog governance state, and provides workflows for enrichment, exception requests, and ontology browsing.
**Depends on:** [p-uc-isolation(P2)](./p-uc-isolation(P2).md) — the catalog only works because UC assets are isolated, governable, and tagged at the platform layer.
**Replaces (the customer deployment of):** Ontos, [d-watchdog-discover](./p-watchdog-discover(P3).md)'s portal role. Watchdog stays the engine; Ontos remains a reference implementation per [memory: Ontos not deploying at the customer](../docs/customer-catalog/comparison.md#systems-in-scope).
**Roadmap:** Phase 3 — UI layer on top of the platform substrate and policy engine.
**Priority:** Tier 1 — without it, end users have no customer-branded surface for the governance work.

---

## Why this proposal sits on top of uc-isolation

The three-layer governance stack at the customer is:

```
┌─────────────────────────────────────────────────────────────┐
│  Customer Catalog FE  (this proposal — P3, UI)                │
│  Curated browse · table detail · exceptions · ontology       │
│  React + FastAPI · Lakebase-cached reads · OBO sample data   │
└──────────────────────────┬──────────────────────────────────┘
                           │ reads
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Watchdog  (p-watchdog P1, engine)                          │
│  Crawl → classify → emit governed tags · violations/policies │
│  Writes Delta tables in {watchdog_catalog}.watchdog.*        │
└──────────────────────────┬──────────────────────────────────┘
                           │ enforces against
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  UC Isolation  (p-uc-isolation P2, substrate)               │
│  Per-workspace isolation_mode · sp-publisher · hub_catalog    │
│  The actual UC objects + storage + identity that exist       │
└─────────────────────────────────────────────────────────────┘
```

The catalog is the **read-mostly UI** that makes this stack usable. It can only:
- **Curate** because uc-isolation gives each workspace a governable boundary and Watchdog tags assets with `watchdog_domain` from the ontology
- **Show violations** because Watchdog writes them to `{watchdog_catalog}.watchdog.violations`, on Delta tables created in the substrate uc-isolation defines
- **Approve exceptions** because the catalog can write back to `{watchdog_catalog}.watchdog.exceptions` via UC `MODIFY` grants — grants that only make sense when uc-isolation has scoped the SP's blast radius correctly

If uc-isolation isn't deployed, the catalog has nothing trustworthy to curate. If the catalog isn't deployed, uc-isolation's governance is invisible to end users.

---

## What's built

Four capability gaps, each scoped in its own doc under `docs/customer-catalog/`:

| Gap | What | Detail doc |
| --- | --- | --- |
| 4 | **Curation** — only assets with `watchdog_domain` or `customer_catalog_include` show in the catalog. Centralized admin team promotes/excludes via UC tag writes. | [curation.md](../docs/customer-catalog/curation.md) |
| 1 | **Exception workflow** — request → approve/reject/revoke. Approvals write directly to `watchdog.exceptions` Delta via Statement Execution API. Reaper job retries failed propagation. | [exceptions-workflow.md](../docs/customer-catalog/exceptions-workflow.md) |
| 2 | **Policy viewer** — rich `/policies/:id` with rule tree, violation trend, exceptions list. Read-only; authoring stays in YAML-in-Git. | [policy-viewer.md](../docs/customer-catalog/policy-viewer.md) |
| 3 | **Ontology viewer** — `/ontology` class tree + per-class detail + Discover-domains index. Reads `bundles/watchdog/ontologies/*.yml` at runtime. | [ontology-viewer.md](../docs/customer-catalog/ontology-viewer.md) |
| 5 | **uc-isolation surfaces** — hub/spoke distinction, workspace ownership, publishing lineage on hub assets. Closes the loop between the platform layer and the UI. | [uc-isolation-surfaces.md](../docs/customer-catalog/uc-isolation-surfaces.md) |
| 6 | **Workspace bindings** — "Accessible from" panel on `/tables/:id`, browse facet, OPEN vs ISOLATED detection. Closes uc-isolation-surfaces's V2 deferral. | [workspace-bindings.md](../docs/customer-catalog/workspace-bindings.md) |

Plus the foundational pieces already on develop before this branch:
- Lakebase-cached reads (instant page loads, no warehouse cold start)
- OBO sample data preview (respects UC ACLs)
- Enrichment workflow (owner/description with `x-forwarded-user` audit)
- Postgres FTS search

Full capability matrix vs Native UC / Watchdog / Ontos: [comparison.md](../docs/customer-catalog/comparison.md).

---

## What's NOT in scope

- **No policy authoring UI.** Policies stay in YAML in `bundles/watchdog/policies/*.yml`, reviewed via PR.
- **No ontology authoring UI.** Same pattern — YAML in `bundles/watchdog/ontologies/*.yml`.
- **No classification logic in the FE.** Watchdog owns crawling, tagging, and policy evaluation.
- **No bulk curation operations for V1** — single-asset only per the curator UX brief.

---

## Coupling to uc-isolation

Specific places where this proposal depends on uc-isolation's substrate:

| Catalog feature | uc-isolation precondition |
| --- | --- |
| `/browse` filters by `watchdog_domain` tag | Watchdog `tag_emitter` writes the tag onto UC tables — only meaningful on isolated catalogs scoped to a single business domain |
| `/admin/discover` writes `customer_catalog_include = true` UC tag | Catalog SP needs `MODIFY` on the catalog/schema; uc-isolation's `isolation_mode = ISOLATED` keeps that grant scoped |
| Exception approval writes to `watchdog.exceptions` Delta | The watchdog catalog is in a known location (`{watchdog_catalog}.watchdog`) because uc-isolation's regional-infra deploy created it |
| Sample data OBO preview respects UC ACLs | UC ACLs only meaningful in a properly isolated metastore — the multi-workspace BYO-VNet + workspace bindings model from uc-isolation |
| Curated set excludes the Watchdog catalog from showing as user data | The `WATCHDOG_CATALOG` env split — a uc-isolation deployment decision |

A catalog deployment without uc-isolation would be possible but degenerate: it would show everything in `system.information_schema.tables` with no governance signal, no enforced boundaries, and no audit trail on writes.

---

## Open items / not-yet-built

Beyond the six built gaps + the rule-primitives endpoint refinement, what remains:

- **Lineage tab** — UC native lineage surfaced on `/tables/:id` (separate from uc-isolation-surfaces's `Published from` panel). Frontend placeholder exists in `DatasetDetail.tsx`; backend not wired.
- **Notifications** — Slack/email pings on exception requests, exception expiry.

Shipped since first draft (2026-05-12):

- ✅ **Bulk operations** — multi-select promote/exclude (`97ee6828`)
- ✅ **Saved searches / shared filters** — personal + team-shared presets (`13c86748`)
- ✅ **Resource detail for compute** — warehouses, clusters, jobs (`27b9c14f`)
- ✅ **Auto-detect user's home workspace** — shipped alongside bulk ops (`97ee6828`)

These build on the same architecture; none requires changes to uc-isolation.

---

## Effort

| Task | Status |
| --- | --- |
| Gap 4 curation | ✅ Code complete |
| Gap 1 exception workflow | ✅ Code complete |
| Gap 2 policy viewer | ✅ Code complete |
| Gap 3 ontology viewer | ✅ Code complete |
| Gap 5 uc-isolation surfaces | ✅ Code complete (comes alive at deploy) |
| Gap 6 workspace bindings | ✅ Code complete (needs MANAGE grant + real workspace ids in YAML at deploy) |
| Rule-primitives endpoint (policy-viewer followup closure) | ✅ Code complete |
| Comparison + gap docs + cross-links | ✅ Done |
| Bulk curation ops + auto-detect host workspace | ✅ Code complete (`97ee6828`) |
| Saved searches (personal + team-shared) | ✅ Code complete (`13c86748`) |
| Compute resource detail (warehouses, clusters, jobs) | ✅ Code complete (`27b9c14f`) |
| First-time deploy to FE Stable (schema migrations + UC grants + bundle deploy) | ✅ Deployed (`6faa7396` setup notebook + `a9e218c0` reverse-etl) |
| Curator onboarding (Entra group, training, seed promotions) | Pending — ~2 hr |
| Lineage tab backend wiring | Pending |
| Notifications (Slack/email) | Pending |

## Customer / Business Decisions Still Open

- **Admin group name** — set `CUSTOMER_CATALOG_ADMIN_GROUP` env to actual Entra ID display name
- **Curatable catalog allowlist** — which UC catalogs should curators be able to promote from (`CUSTOMER_CATALOG_CURATABLE_CATALOGS`)
- ~~**Separation-of-duties stance**~~ — **Decided 2026-05-13:** Catalog SP holds direct UC write grants across all gaps. `MODIFY` on `{watchdog}.watchdog.exceptions` (exceptions), `MODIFY` on user catalogs for tag writes (curation), `MANAGE` on catalogs for `SHOW WORKSPACE BINDINGS` (workspace-bindings). Forward-ETL fallback stays unbuilt.
- **Domains list** — defaults match the customer's Discover Domains; confirm or override via `CUSTOMER_CATALOG_DOMAINS`
- **Approver group** — same Entra ID question for the exception approval flow; today defaulted to the admin group
