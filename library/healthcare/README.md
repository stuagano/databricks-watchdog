# Healthcare (HIPAA) Policy Pack

Industry policy pack for organizations subject to the Health Insurance Portability
and Accountability Act (HIPAA). Provides ontology classes, rule primitives, and
governance policies for Protected Health Information (PHI) and electronic PHI (ePHI).

## What's Included

| File | Description |
|------|-------------|
| `ontology_classes.yml` | PHI, ePHI, audit trail, and de-identified dataset classes |
| `rule_primitives.yml` | HIPAA-specific reusable rule checks (stewardship, encryption, BAA, etc.) |
| `policies.yml` | 10 governance policies mapped to HIPAA Privacy, Security, and Breach Notification Rules |
| `dashboard_queries.sql` | Lakeview/Genie SQL queries for HIPAA compliance dashboards |

## Installation

Copy the pack files into your engine directories:

```bash
# Ontology classes — append derived_classes to your resource_classes.yml
cat library/healthcare/ontology_classes.yml >> engine/ontologies/resource_classes.yml

# Rule primitives — append to your rule_primitives.yml
cat library/healthcare/rule_primitives.yml >> engine/ontologies/rule_primitives.yml

# Policies — copy into your policies directory
cp library/healthcare/policies.yml engine/policies/hipaa.yml
```

After copying, verify the YAML parses cleanly:

```bash
python -c "import yaml; yaml.safe_load(open('engine/ontologies/resource_classes.yml'))"
python -c "import yaml; yaml.safe_load(open('engine/ontologies/rule_primitives.yml'))"
python -c "import yaml; yaml.safe_load(open('engine/policies/hipaa.yml'))"
```

## Policies Overview

| ID | Name | Severity | Domain |
|----|------|----------|--------|
| POL-HIPAA-001 | PHI assets must have a designated data steward | critical | RegulatoryCompliance |
| POL-HIPAA-002 | PHI assets must have a retention policy | critical | RegulatoryCompliance |
| POL-HIPAA-003 | ePHI must be encrypted at rest | critical | SecurityGovernance |
| POL-HIPAA-004 | PHI access must be logged | critical | SecurityGovernance |
| POL-HIPAA-005 | PHI must not exist in development environments | critical | RegulatoryCompliance |
| POL-HIPAA-006 | PHI shared with third parties must reference a BAA | high | RegulatoryCompliance |
| POL-HIPAA-007 | PHI access must follow minimum necessary standard | high | SecurityGovernance |
| POL-HIPAA-008 | PHI assets must have a breach notification contact | high | SecurityGovernance |
| POL-HIPAA-009 | De-identified datasets must document the method used | medium | RegulatoryCompliance |
| POL-HIPAA-010 | HIPAA audit tables must be append-only | high | RegulatoryCompliance |

## Prerequisites

The base engine ontology must include `ConfidentialAsset` and `DataAsset` classes
(shipped with the default `engine/ontologies/resource_classes.yml`). The `PhiAsset`
class extends `ConfidentialAsset`, so ensure that class is present before installing.

## Tagging Guide

For assets to be classified by this pack, apply the following Unity Catalog tags:

| Tag Key | Values | Purpose |
|---------|--------|---------|
| `data_classification` | `phi`, `ephi`, `hipaa` | Triggers PhiAsset/EphiAsset classification |
| `storage_type` | `electronic` | Required for EphiAsset (with `ephi` classification) |
| `data_steward` / `phi_steward` | email address | Satisfies stewardship policies |
| `retention_policy` / `retention_years` | e.g., `6_years` / `6` | Satisfies retention policies |
| `encryption_at_rest` | `true` | Confirms encryption compliance |
| `access_logging_enabled` | `true` | Confirms audit trail |
| `baa_id` | agreement reference | Links to Business Associate Agreement |
| `breach_notification_contact` | email or team | Breach response contact |
| `de_identification_method` | `safe_harbor`, `expert_determination` | De-identification documentation |
| `purpose` | `hipaa_audit` | Classifies audit trail tables |
