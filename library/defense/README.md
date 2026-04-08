# Defense Industry Policy Pack

Drop-in governance policies for NIST 800-171, CMMC Level 2, and ITAR compliance
in Databricks Unity Catalog environments.

## Installation

Copy the YAML files into your Watchdog engine directories:

```bash
cp library/defense/ontology_classes.yml  engine/ontologies/defense_classes.yml
cp library/defense/rule_primitives.yml   engine/ontologies/defense_primitives.yml
cp library/defense/policies.yml          engine/policies/defense_governance.yml
```

## Policies

| ID | Name | Severity | Framework |
|----|------|----------|-----------|
| POL-NIST-001 | CUI must have proper markings | critical | NIST 800-171 |
| POL-NIST-002 | CUI access must be controlled | critical | NIST 800-171 |
| POL-NIST-003 | CUI must have media protection controls | high | NIST 800-171 |
| POL-NIST-004 | CUI must have incident response plan | high | NIST 800-171 |
| POL-NIST-005 | Defense audit trails must be immutable | high | NIST 800-171 |
| POL-CMMC-001 | CMMC L2 assets must reference system security plan | critical | CMMC |
| POL-CMMC-002 | CMMC L2 assets must use FIPS-validated encryption | critical | CMMC |
| POL-ITAR-001 | ITAR data must have export control classification | critical | ITAR |

## Ontology Classes

- **CuiAsset** — Controlled Unclassified Information (CUI)
- **ItarAsset** — ITAR-controlled technical data
- **ExportControlledAsset** — Export-controlled under EAR/ITAR
- **CmmcLevel2Asset** — Assets requiring CMMC Level 2 controls
- **DefenseAuditTrail** — Audit tables for defense data

## Dashboard Queries

`dashboard_queries.sql` contains SQL queries for monitoring defense compliance
posture, CUI coverage gaps, and CMMC readiness.
