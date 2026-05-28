# Hub Publishing Governance

**Status:** Proposed
**Date:** 2026-05-28
**Source:** Imported from an external industry proposal (`p-uc-isolation`).
**Builds on:** `engine/policies/medallion_governance.yml` (intra-catalog tiering).

---

## Problem

`medallion_governance.yml` covers intra-catalog tiering — Bronze must have a source system tag, Silver must have a data steward, Gold must have a retention policy, production pipelines must be healthy. It says nothing about **cross-catalog promotion**.

A common multi-workspace pattern: each business unit / region runs a spoke catalog (`alpha_gold`, `beta_gold`, …) and a shared `hub` catalog republishes selected tables for cross-spoke consumption. Today watchdog has no policy concept for "is this hub table actually backed by a clean spoke gold source?" The result is two failure modes:

1. **Hub tables that point at nothing** — a hub view references a spoke gold table that was renamed or dropped; the view silently 404s.
2. **Hub tables that point at violations** — a hub table republishes a spoke gold table that itself has open critical violations (missing steward, missing retention, stale). The hub catalog inherits the noise without inheriting the accountability.

The medallion ontology terminates at `GoldTable`. There's no `HubTable` to attach cross-catalog rules to.

## Goal

Add a small, generic policy pattern for hub-publishing governance:

1. A new ontology class `HubTable` that identifies tables in a customer-designated hub catalog.
2. Three policies in a new file `engine/policies/hub_publishing.yml` covering: source lineage, scan cleanliness of the source, and ownership inheritance.
3. A rule primitive `has_clean_spoke_source` that joins a hub table against the violation table for its lineage upstream.

## Non-Goals

- **The promotion mechanism.** Delta Sharing, CTAS, `CREATE VIEW`, mirror — out of scope. The policies evaluate the *result* of promotion, not how it happens.
- **Workspace, IAM, or network isolation.** The source proposal also covered Terraform isolation. That's platform infra, not policy. Out of scope here.
- **Bidirectional publishing.** Spokes consuming from hub is a different shape (likely a separate policy).
- **Customer-specific catalog naming.** The hub catalog identifier is a deployment-time configuration value, not a hard-coded name.

---

## Ontology Addition

A new derived class in `engine/ontologies/resource_classes.yml`:

```yaml
HubTable:
  parent: GoldTable          # inherits Gold layer governance
  classifier:
    catalog_in: "{hub_catalogs}"   # deployment-time list, e.g. ["hub", "platform_hub"]
```

`{hub_catalogs}` is a configurable parameter — a customer with one hub catalog passes a one-element list; a customer with multiple regional hubs passes them all. Resolved at compile time from a top-level config key.

## Policies (`engine/policies/hub_publishing.yml`)

### POL-HUB-001 — Hub table must declare its spoke source

```yaml
- id: POL-HUB-001
  name: "Hub tables must declare a spoke gold source"
  applies_to: HubTable
  domain: DataQuality
  severity: critical
  description: >
    Every table in a hub catalog must be tagged with `spoke_source` pointing to the
    fully-qualified spoke gold table it republishes. Without this tag, lineage is
    broken and the source-cleanliness check (POL-HUB-002) cannot run.
  remediation: "Add a 'spoke_source' tag with the source table's 3-part name (catalog.schema.table)"
  rule:
    ref: has_tag
    param: { key: spoke_source }
```

### POL-HUB-002 — Hub source must be a clean spoke gold table

```yaml
- id: POL-HUB-002
  name: "Hub table source must be a spoke gold table with no open critical violations"
  applies_to: HubTable
  domain: DataQuality
  severity: critical
  description: >
    The table named in `spoke_source` must (a) exist, (b) classify as GoldTable in
    the watchdog ontology, and (c) have zero open critical violations as of the
    last scan. Republishing a source with open critical violations propagates the
    risk without inheriting the accountability.
  rule:
    ref: has_clean_spoke_source
    param: { severity_threshold: critical }
```

### POL-HUB-003 — Hub table must inherit a steward

```yaml
- id: POL-HUB-003
  name: "Hub tables must have a publishing steward"
  applies_to: HubTable
  domain: DataQuality
  severity: high
  description: >
    Publishing into a hub catalog is an accountable act. Each hub table requires
    a `publishing_steward` tag identifying the person responsible for keeping the
    hub view in sync with the spoke source. May differ from the spoke `data_steward`.
  remediation: "Add a 'publishing_steward' tag with the responsible person's email"
  rule:
    ref: has_tag
    param: { key: publishing_steward }
```

## Rule Primitive

A new primitive `has_clean_spoke_source` in `engine/ontologies/rule_primitives.yml`:

```yaml
has_clean_spoke_source:
  description: >
    Resolves the resource's `spoke_source` tag to a 3-part table identifier, looks
    that table up in resource_inventory, and asserts (a) it exists, (b) classifies
    as GoldTable, and (c) has zero open violations at or above the severity threshold.
  params:
    severity_threshold: critical   # default; override per policy
  evaluates: |
    let src = resource.tags.spoke_source
    let src_row = resource_inventory.find(src)
    src_row.exists
      AND GoldTable in src_row.classes
      AND violations.open_count(src_row.id, severity >= severity_threshold) == 0
```

Implementation note: this primitive crosses resource rows — the rule engine already supports cross-resource joins via the `cross_resource` machinery used by drift detection. Confirm reuse before adding parallel logic.

## Configuration

A new top-level config key in `engine/databricks.yml` (or wherever deployment config lands):

```yaml
hub_publishing:
  hub_catalogs:
    - hub
    # add more per deployment
  spoke_catalog_pattern: "*_gold"   # optional, default matches any catalog ending in _gold
```

The ontology classifier substitutes `{hub_catalogs}` from this config at compile time.

## Dependencies

- `medallion_governance.yml` ontology classes (`GoldTable`) — landed.
- Cross-resource rule evaluation — already used by drift detection; verify the join path.
- A `spoke_source` tag convention — customers must apply this tag when promoting. Document in the tag taxonomy reference (`2026-05-28-data-classification-crawler-design.md`).

## Risks

| Risk | Mitigation |
|---|---|
| `spoke_source` tag not applied retroactively | POL-HUB-001 fires; remediation guides the steward to backfill. |
| Hub catalog naming differs per deployment | `hub_catalogs` is a config list, not hard-coded. |
| Cross-resource join performance on large estates | Primitive limits the join to `HubTable`-classified rows only — bounded by hub catalog size, not total inventory. |
| Customers with no hub pattern see noise | No `HubTable`-classifying resources → no violations. Policy file is opt-in by default. |
| Spoke source dropped or renamed → POL-HUB-002 fires incorrectly as "violations" | This is the intended behavior. Renames must update the `spoke_source` tag, same as any lineage convention. |

## Order of Operations

1. Add `HubTable` ontology class with config-driven classifier.
2. Add `has_clean_spoke_source` rule primitive; verify cross-resource join reuse.
3. Add `engine/policies/hub_publishing.yml` with POL-HUB-001..003.
4. Add config key + default to `databricks.yml` example.
5. Document in `docs/guide/concepts/hub-publishing.md`.
6. Tests: fixture rows for `HubTable` classification, primitive behavior with missing / dirty / clean sources.

## Estimated Effort

| Phase | Effort |
|---|---|
| Ontology + classifier | 0.5 days |
| Rule primitive + cross-resource join | 1.5 days |
| Policy file + tests | 1 day |
| Docs | 0.5 days |
| **Total** | **~3.5 days** |
