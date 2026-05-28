# d-watchdog-discover — Watchdog Governance Portal

**Status:** ❌ Dropped (2026-05-28). Scope abandoned — see note below.
**Stage:** P3 — was on FE Stable when proposal was last touched; no longer maintained.

> **Dropped — the consumer-browse `discover_domain` governed-tag concept and `tag_emitter` were not built and are not on the watchdog roadmap.** Greps across `~/Documents/Projects/databricks-watchdog/` (standalone repo `CustomerDataPlatform/watchdog`) for `discover_domain`, `watchdog_domain`, and `tag_emitter` all return zero hits.
>
> **What survived in a different shape:** the `ontos-adapter/` directory in the watchdog repo implements a `GovernanceProvider` protocol that exposes violations/policies/exceptions/asset-browser endpoints to Ontos — i.e. the "Ontos embeds watchdog views" part of this proposal. But that's a server-side governance API, not the consumer-browse "one tag, one job" model this proposal centered on.
>
> **What was dropped:**
> - `watchdog_domain` governed tag written to UC objects via `tag_emitter.py`
> - `discover_domain:` field on ontology classes
> - `ItarAsset has no discover_domain` enforcement-only carve-out
> - The branch `stuart/d-watchdog-discover-P3` referenced in this header doesn't exist in customer-infra (and never did, locally).
>
> Keep this file as historical record of the discover design. Do not implement `tag_emitter` or `discover_domain` in `customer-infra/` or `databricks-watchdog/` without re-opening the design discussion.

---

## Architecture

Two layers, each with a distinct job:

```
┌─────────────────────────────────────────────────────┐
│  Ontos  (one-stop governance portal)                │
│  OWL/TTL ontology authoring + watchdog governance   │
│  ai-devkit-governance embeds watchdog views into it │
└─────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────┐
│  Watchdog engine                                    │
│  Scans catalog → classifies → writes Delta tables   │
│  tag_emitter writes watchdog_domain governed tag    │
└─────────────────────────────────────────────────────┘
```

**One app.** Ontos is the governance portal — the `ai-devkit-governance` bundle embeds watchdog views (violations, policies, exceptions, asset browser, resource detail) directly into Ontos so users never leave the app.

### Ontology vs UC Discover Domains — separation of concerns

| | Watchdog ontology | UC Discover Domain |
|---|---|---|
| Purpose | Enforcement scope (`applies_to:` in policies) | Browse/find by business meaning |
| Shape | Hierarchical (`GoldTable → DataAsset → Thing`) | Flat groupings |
| Audience | Policy authors, the engine | Data consumers |
| Stored in | `resource_classifications` Delta table | Governed tag on UC object |

These are not alternatives — they're complementary. The ontology drives enforcement; the Domain tag drives discovery. `tag_emitter.py` is the bridge: it reads `discover_domain:` from the ontology YAML and writes `watchdog_domain = <label>` as a governed tag on the real UC table.

**One tag, one job.** `watchdog_domain` is the only consumer-facing governed tag written to UC objects. Technical classes (`GoldTable`, `UnownedAsset`, `BronzeTable`) stay internal to `resource_classifications` — they're policy-authoring primitives, not browse labels.

**ItarAsset has no `discover_domain`.** It's enforcement-only. Tables classified as ITAR never appear in the asset browser — they surface only through violations and exceptions, which require explicit access.

### Tag design

```yaml
# ontologies/resource_classes_customer.yml
DosimetryData:
  parent: DataAsset
  discover_domain: "Dosimetry & Safety Data"   # → written as governed tag
  classifier:
    tag_equals: { domain: dosimetry }

ItarAsset:
  parent: ConfidentialAsset
  # no discover_domain — enforcement-only, never browsable
```

`tag_emitter.py` reads `discover_domain` from each leaf class and runs:
```sql
ALTER TABLE <catalog>.<schema>.<table>
  SET TAGS ('watchdog_domain' = 'Dosimetry & Safety Data')
```

The asset browser then queries this natively:
```sql
SELECT catalog_name, schema_name, table_name, tag_value AS domain
FROM system.information_schema.table_tags
WHERE tag_name = 'watchdog_domain'
```

No internal inventory table needed for browsing. The tag IS the catalog record.

---

## What's built (FE Stable)

### Ontos governance views (`ai-devkit-governance`)

Embedded into Ontos via `bundles/ai-devkit-governance/`. Routes and views:

| Route | What it does |
|---|---|
| `/governance/watchdog/assets` | Browse all UC tables by domain, grouped by `watchdog_domain` tag. Queries `system.information_schema.table_tags` natively. Violation counts inline. |
| `/governance/watchdog/assets/<id>` | Per-asset detail: classifications, active violations, active exceptions |
| `/governance/watchdog` | Active violations filterable by severity and domain |
| `/governance/watchdog/resources/:id` | Violation detail with linked policy, resource metadata, grants |
| `/governance/policies` | Policy list with YAML-origin lock; create/edit dialog |
| `/governance/exceptions` | Active/expired toggle; revoke and bulk-revoke-expired |

### Lakeview dashboards (deployed)
- `watchdog_governance_posture.json` — posture overview
- `uc_governance_hub_unified.json` — unified UC governance view
- `watchdog_agent_compliance.json` — agent compliance

### Genie space
Instructions in `bundles/watchdog/genie/watchdog_genie_instructions.md`. Covers violations, policies, exceptions, classifications, domain posture.

### Demo UC tables (FE Stable)
Placeholder tables in `serverless_stable_qh44kx_catalog` tagged with `watchdog_domain`:

| Schema | Tables | Domain |
|---|---|---|
| `dosimetry` | raw_badge_scans, cleaned_readings, dose_calculations, monthly_dose_summary | Dosimetry & Safety Data |
| `hpge` | detector_calibrations, spectral_analysis, detector_performance | Detector & Calibration Data |
| `compliance` | nrc_reporting | Regulatory Compliance Data |
| `supply_chain` | raw_orders, enriched_orders | Sales & Orders Data |

### the customer ontology overlay
`bundles/watchdog/ontologies/resource_classes_customer.yml` — customer-specific classes:
- `DosimetryData`, `DetectorData`, `NuclearRegulatoryData`, `OrdersData` (business domains)
- `ItarAsset` (enforcement-only, no discover_domain)

### tag_emitter
`bundles/watchdog/src/watchdog/tag_emitter.py` — reads `discover_domain` from ontology YAML, writes `watchdog_domain` governed tag to real UC tables after each scan.

---

## UC Discover — supplementary, not primary

UC Discover Domains (Beta, Feb 2026) don't have a public REST API for programmatic domain asset assignment today. Internal Databricks roadmap targets PuPr with API in May 2026, GA ~July 2026.

**Until GA:** Ontos is the one-stop governance portal. UC Discover is a nice supplementary browse surface for data consumers but not required for the governance story.

**After GA:** `tag_emitter.py` writes `watchdog_domain` today. When the Domains API ships, domain membership will be derivable from that same tag — likely a one-line migration. The "the customer Watchdog" domain exists in the FE Stable workspace UI already; per-class business domains (`Dosimetry & Safety Data`, etc.) get added once the API is available.

The Ontos asset browser is the one place governance operators see assets, violations, policies, and exceptions together.

---

## Demo sequence (operator narrative)

1. Open Ontos → **Assets** tab.
   Sees four business domains with real UC tables, violation counts inline.
2. Click a table (e.g. `dosimetry.dose_calculations`) → asset detail.
   Classifications, active violations, and exceptions in one view.
3. Click through to **Violations** — filter by severity, see evidence per violation.
4. Open a violation → linked policy explains the rule; linked grants show who has access.
5. **Policies** tab → toggle a policy active/inactive to show live governance control.
6. **Exceptions** tab → approve or revoke a waiver, with audit trail.
7. (Optional) Open Genie space → ask "what's our top compliance risk this week?" for the AI/BI angle.

---

## Remaining work

| Item | Notes |
|---|---|
| UC grants for non-admin users | `scripts/grant-watchdog-demo-access.sh` — run with `--principal <group>` before demo |
| UC Discover domain population | Defer until Domain API ships (May 2026 PuPr) |
| UC Discover domain population | Defer until Domain API ships (May 2026 PuPr) |

## Completed open items

| Item | How resolved |
|---|---|
| Asset detail page | Migrated to `system.information_schema.tables` + `.table_tags`; no longer queries `resource_inventory` |
| `tag_emitter.py` wiring | Fixed `DOMAIN_TAG_KEY` → `watchdog_domain`; `evaluate()` entrypoint emits tags post-classify; `OntologyEngine` now loads overlay files and exposes `class_to_domain_map()` |
| V4C handoff docs | `bundles/ai-devkit-governance/DEPLOY.md` — covers standalone vs embedded deployment, frontend wiring, grants, env vars |
| Ontos Assets view | `WatchdogAssets.tsx` + `/assets` backend endpoint — queries `system.information_schema.table_tags` natively |
| Ontos resource detail | `get_resource` migrated to `system.information_schema.tables` + `.table_tags` for UC tables; governed tags rendered in metadata card |

---

## Key decisions (locked)

- **Ontos** is the one-stop governance portal. Watchdog views are embedded via `ai-devkit-governance` so users never switch apps.
- **`watchdog_domain`** is the only consumer-facing governed tag. All other class metadata stays in `resource_classifications`.
- **Asset browser** reads `system.information_schema.table_tags` natively. No custom inventory table for browsing.
- **ItarAsset** has no `discover_domain` — enforcement-only, never surfaces in the browse view.
- **UC Discover** is supplementary; Ontos is the primary browse surface until the Domains API ships.
