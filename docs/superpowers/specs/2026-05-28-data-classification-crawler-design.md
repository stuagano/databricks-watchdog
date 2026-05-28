# Data Classification Crawler & Tag Taxonomy

**Status:** Proposed
**Date:** 2026-05-28
**Source:** Imported from an external industry proposal (`p-data-classification`). Closes gaps flagged in commit `772adb4` ("POL-DC-001/002 require a classification crawler not yet in core").

---

## Problem

The data classification pack landed (`library/data-classification/policies/classification_enforcement.yml`, `terraform/modules/watchdog/`, permissions compiler reference) but two enabling pieces are missing:

1. **POL-DC-001 and POL-DC-002 cannot evaluate.** Both policies require knowing whether a table has auto-detected PII or PHI columns. That signal lives in the platform's `system.data_classification.results` table. No crawler in `engine/src/watchdog/crawler.py` reads it today, so the policies match no resources.
2. **No tag taxonomy reference.** Customers landing the pack don't have a single document that lists which tags exist, who applies them, and which policies enforce them. Tags drift between docs, policy YAML, and the permissions compiler.

## Goal

Two additions:

1. A new crawler `data_classification_crawler.py` that reads `system.data_classification.results`, joins to `resource_inventory`, and writes derived classification tags onto the watchdog-side row (not the UC object).
2. A reference document `docs/guide/reference/tag-taxonomy.md` enumerating every tag the platform reads and writes, the source-of-truth for each, and which policies depend on it.

## Non-Goals

- **Enabling auto-classification.** The Terraform module already wraps `databricks_data_classification_catalog_config`. Out of scope here.
- **Writing tags onto UC objects.** Watchdog never writes to UC. The crawler enriches the watchdog-side inventory only.
- **Re-implementing the platform classifier.** This consumes the platform's output; it does not classify columns.
- **Tag policies for governed-tag-allowed-values.** That's a separate UC tag-policy concern handled by the platform team.

---

## Crawler Design

### Input

`system.data_classification.results` — produced by Databricks Data Classification when enabled on a catalog. Schema (per platform docs):

| Column | Used? |
|---|---|
| `catalog_name`, `schema_name`, `table_name`, `column_name` | yes — joins to `resource_inventory` |
| `classifier_name` (e.g., `email`, `ssn`, `phi_mrn`) | yes — bucketed into PII / PHI / financial / network |
| `last_classified_at` | yes — freshness check |
| `confidence` | yes — threshold gate (default ≥ 0.85, configurable) |

### Output

Per `resource_inventory` row (table-level), the crawler writes derived tags onto the watchdog-side classification map:

| Derived tag | Source rule |
|---|---|
| `has_pii_columns = true` | any column row classified as `email`, `ssn`, `phone`, `address`, `name`, `government_id` |
| `has_phi_columns = true` | any column row classified as `phi_*` (MRN, diagnosis, treatment) |
| `has_financial_columns = true` | any column row classified as `credit_card`, `bank_account` |
| `classifier_last_scanned_at` | max(`last_classified_at`) across columns in the table |
| `unscanned_for_days` | `current_date - classifier_last_scanned_at`, or `null` if never scanned |

Bucketing rules live in `engine/ontologies/classification_buckets.yml` so the mapping is data-driven, not hard-coded in Python.

### Rule primitive

A new primitive `has_classifier_finding` (one row in `rule_primitives.yml`):

```yaml
has_classifier_finding:
  description: "Resource has at least one column with a classifier finding in the named bucket"
  param: bucket  # one of: pii, phi, financial, network
  evaluates: resource_inventory.classification.{bucket}_columns_count > 0
```

POL-DC-001 and POL-DC-002 then bind to this primitive instead of an inline check.

### Where it runs

New entrypoint mode: `watchdog crawl --include classification`. Skipped by default unless either (a) the catalog has `databricks_data_classification_catalog_config.enabled = true`, detected via SDK, or (b) the user passes `--include classification`. Costs nothing on workspaces that don't enable classification.

### Ontology

Add a derived ontology class `ClassifiedTable` in `engine/ontologies/resource_classes.yml`:

```yaml
ClassifiedTable:
  parent: DataAsset
  classifier:
    has_classifier_finding: { bucket: any }
```

Policies that want to scope to "tables the platform classifier flagged anything on" use `applies_to: ClassifiedTable` instead of an inline filter.

---

## Tag Taxonomy Reference

A new file `docs/guide/reference/tag-taxonomy.md` lists every tag the platform reads and writes, structured as:

| Tag key | Values | Applied by | Read by | Notes |
|---|---|---|---|---|
| `system.classifier.*` | platform-defined (email, ssn, phi_mrn, etc.) | Databricks Data Classification (auto) | data classification crawler | column-level |
| `data_classification` | public, internal, confidential, restricted, pii | data steward (manual) | medallion_governance POL-MED-004 | table-level |
| `data_steward` | email | data steward (manual) | classification_enforcement POL-DC-001 | table-level |
| `regulatory_domain` | HIPAA, ITAR, SOX, GDPR, etc. | data steward (manual) | classification_enforcement POL-DC-002, POL-DC-004 | table-level |
| `export_classification` | NONE, EAR, ITAR | data steward (manual) | classification_enforcement POL-DC-004 | table-level |
| `retention_days` | integer | data steward (manual) | medallion_governance POL-MED-006 | table-level |
| `data_layer` | bronze, silver, gold | pipeline (automated) | medallion ontology classifiers | table-level |
| `source_system` | free text | pipeline / steward | medallion_governance POL-MED-001 | table-level (Bronze) |

The doc lives in `docs/guide/reference/` (alongside `policy-schema.md`, `rule-types.md`, etc.) and is updated under the same removal-and-rename-hygiene rules that `CLAUDE.md` already prescribes.

---

## Dependencies

- `system.data_classification.results` available (auto-classification is currently Public Preview/Beta on Azure; behind a feature flag elsewhere). The crawler degrades gracefully if the table doesn't exist.
- `library/data-classification/policies/classification_enforcement.yml` (POL-DC-001..004) — landed.
- `terraform/modules/watchdog/` — already provisions the SP/grants; no additions needed for the crawler itself.

## Risks

| Risk | Mitigation |
|---|---|
| Classification false positives → noisy violations | POL-DC-001/002 are `medium`/`high`, not `critical`; remediation is "review", not "block". |
| Confidence threshold too loose / too strict | Threshold lives in the bucket config YAML, tunable without code change. |
| `system.data_classification.results` schema evolves | Crawler reads a defined column subset; unknown columns ignored. |
| Customers without Data Classification enabled get empty violations | Crawler skips bucketing if zero rows present; policies match no resources rather than match-and-fail. |

## Order of Operations

1. Add `engine/ontologies/classification_buckets.yml` with default bucket rules.
2. Add `has_classifier_finding` rule primitive.
3. Implement crawler module `engine/src/watchdog/crawler_classification.py`; wire into `entrypoints.py` `crawl()` behind `--include classification`.
4. Add `ClassifiedTable` ontology class.
5. Rebind POL-DC-001/002 from inline filters to the new primitive.
6. Write `docs/guide/reference/tag-taxonomy.md`.
7. Add tests under `engine/tests/unit/test_crawler_classification.py` with fixture rows.

## Estimated Effort

| Phase | Effort |
|---|---|
| Crawler + bucket config + primitive | 1.5 days |
| Ontology class + policy rebind | 0.5 days |
| Tag taxonomy reference doc | 0.5 days |
| Unit tests | 0.5 days |
| **Total** | **~3 days** |
