# Catalog Isolation Mode Policy

**Status:** Proposed
**Date:** 2026-05-28
**Source:** Imported from an external industry proposal (`p-uc-isolation`).

---

## Problem

UC catalogs have an `isolation_mode` setting: `OPEN` (accessible from any workspace in the metastore) or `ISOLATED` (accessible only from workspaces with an explicit binding). `OPEN` is the default and the safest choice for shared reference data; `ISOLATED` is the right choice for any catalog holding regulated, classified, or otherwise scoped data.

Today nothing enforces the alignment. A catalog tagged `regulatory_domain = HIPAA` can be left as `OPEN`, and consumers in any workspace in the metastore can hit it. The audit catches the *access*; nothing catches the *misconfiguration*.

The fix is a single small policy that asserts catalogs in regulated domains use `isolation_mode = ISOLATED`. The crawler already collects the field (`engine/src/watchdog/crawler.py` references `isolation_mode`); the policy just consumes it.

## Goal

A new policy in `engine/policies/access_governance.yml` (POL-A005) asserting that any catalog tagged with a regulatory domain uses `ISOLATED` binding.

## Non-Goals

- **Changing isolation_mode.** Watchdog never writes to UC. Detection only.
- **Enumerating which workspaces a catalog is bound to.** That's a separate concern (workspace bindings inventory).
- **Defining the regulatory taxonomy.** Uses the existing `regulatory_domain` tag from the data-classification tag taxonomy.
- **Network or storage isolation.** Different control layer; this is the UC-binding layer only.

---

## Design

### Policy

```yaml
- id: POL-A005
  name: "Regulated catalogs must use ISOLATED binding"
  applies_to: UCCatalog
  domain: SecurityGovernance
  severity: critical
  description: >
    Catalogs tagged with a regulatory_domain (HIPAA, ITAR, SOX, GDPR, etc.) must
    use isolation_mode = ISOLATED so access is restricted to explicitly bound
    workspaces. OPEN isolation makes the catalog reachable from every workspace
    in the metastore, defeating workspace-level scoping.
  remediation: >
    1. Identify the workspaces that legitimately need access:
       SHOW CATALOGS IN METASTORE — find the catalog's current bindings.
    2. ALTER CATALOG {catalog_name} SET ISOLATION_MODE = ISOLATED;
    3. Add explicit workspace bindings for each authorized workspace:
       ALTER CATALOG {catalog_name} ADD WORKSPACE BINDING {workspace_id};
    4. Verify with: SHOW WORKSPACE BINDINGS FOR CATALOG {catalog_name};
  rule:
    type: if_then
    condition:
      type: has_tag
      key: regulatory_domain
    then:
      type: metadata_equals
      field: isolation_mode
      value: ISOLATED
```

`if_then` is an existing rule type — see `2026-04-21-meta-violations-design.md` and `engine/src/watchdog/rule_engine.py`. The conditional avoids firing on unregulated catalogs.

### Crawler

Confirmation that `isolation_mode` is already crawled — referenced in `engine/src/watchdog/crawler.py`. No crawler changes required.

If the field is captured but not exposed on the `metadata` map the policy reads, that's a one-line plumbing fix in the crawler's serialization. Verify before merging the policy.

### Ontology

`UCCatalog` already exists as an ontology class. No additions.

## Dependencies

- `regulatory_domain` tag convention from the tag taxonomy (`2026-05-28-data-classification-crawler-design.md`).
- `isolation_mode` field exposed on `UCCatalog` in the resource inventory — verify in `crawler.py` before merge.
- Existing `if_then` rule type and `has_tag` / `metadata_equals` primitives.

## Risks

| Risk | Mitigation |
|---|---|
| Customers without `regulatory_domain` tags see no violations | This is the intended behavior — the policy is opt-in by tagging. |
| Setting `isolation_mode = ISOLATED` without adding workspace bindings breaks access | Remediation steps lay out the binding addition explicitly. Customers approve a short exception while they migrate. |
| Some catalogs are intentionally `OPEN` for regulatory cross-cutting reference (e.g., reference tables shared across compliance regimes) | Exception workflow handles the few legitimate cases. Severity `critical` ensures these get explicit decisions, not silent drift. |
| `isolation_mode` field name differs across platform versions | Crawler abstracts the field name; the policy reads from the watchdog-side `metadata.isolation_mode`. |

## Order of Operations

1. Verify `isolation_mode` is exposed on `UCCatalog` rows in `resource_inventory` — adjust `crawler.py` if needed.
2. Add POL-A005 to `engine/policies/access_governance.yml`.
3. Unit tests: catalog with regulatory_domain + OPEN → violation; with regulatory_domain + ISOLATED → no violation; without regulatory_domain → not evaluated.

## Estimated Effort

| Phase | Effort |
|---|---|
| Crawler field plumbing (if needed) | 0.25 days |
| Policy + tests | 0.5 days |
| **Total** | **~0.75 days** |
