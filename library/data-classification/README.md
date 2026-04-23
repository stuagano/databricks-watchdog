# Data Classification Policy Pack

Bridges Databricks auto-classification results with Watchdog governance
policies. Ensures that tables with auto-detected sensitive data (PII, PHI,
export-controlled) have the required stewardship, regulatory tagging, and
audit metadata.

## Policies

| ID | Name | Severity |
|----|------|----------|
| POL-DC-001 | Tables with auto-detected PII must have a data steward | critical |
| POL-DC-002 | Tables with auto-detected PHI must have HIPAA regulatory domain | critical |
| POL-DC-003 | All catalogs must have data classification enabled | high |
| POL-DC-004 | Export-controlled tables must have full export metadata | critical |

## Prerequisites

### Crawler enrichment (not yet implemented)

POL-DC-001 and POL-DC-002 depend on a crawler enrichment step that reads
`system.data_classification.results` and sets boolean metadata fields on each
table resource:

- `has_pii_columns=true` when auto-classification detects PII columns
- `has_phi_columns=true` when auto-classification detects PHI columns

This crawler enrichment is **not yet implemented** in the core engine. These
two policies will only fire once a classification crawler is added. Until then,
they remain active but will never match any resources.

### Catalog-level check

POL-DC-003 wraps the catalog enablement check in an `if_then` on
`resource_type=catalog` because there is no `CatalogAsset` ontology class.
Resources that are not catalogs will not be evaluated against this policy.

### Export classification

POL-DC-004 is written for ITAR-controlled data. Organizations in other
regulated industries should adapt the `export_classification` tag value
(e.g., `EAR`, `GDPR-transfer`) and the corresponding `regulatory_domain`.

## Ontology Classes

- **AutoClassifiedAsset** -- Table with auto-classification results from
  Databricks Data Classification

## Rule Primitives

- **has_regulatory_domain** -- Resource must have a `regulatory_domain` tag
- **has_export_classification** -- Resource must have an `export_classification` tag

## Installation

Copy the YAML files into your Watchdog engine directories:

```bash
cp library/data-classification/policies/classification_enforcement.yml \
   engine/policies/classification_enforcement.yml

cp library/data-classification/ontologies/resource_classes.yml \
   engine/ontologies/classification_classes.yml

cp library/data-classification/ontologies/rule_primitives.yml \
   engine/ontologies/classification_primitives.yml
```
