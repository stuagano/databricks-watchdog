# General Governance Policy Pack

Drop-in CIS-style benchmark policies for Databricks Unity Catalog governance.
Covers tagging hygiene, documentation, cost attribution, lifecycle management,
and operational best practices.

## Installation

Copy the YAML files into your Watchdog engine directories:

```bash
cp library/general/ontology_classes.yml  engine/ontologies/general_classes.yml
cp library/general/rule_primitives.yml   engine/ontologies/general_primitives.yml
cp library/general/policies.yml          engine/policies/general_governance.yml
```

## Policies

| ID | Name | Severity |
|----|------|----------|
| POL-GEN-001 | All tables must have data classification | high |
| POL-GEN-002 | All tables must have documentation/comments | medium |
| POL-GEN-003 | All compute must have cost center attribution | high |
| POL-GEN-004 | All resources must have an owner | high |
| POL-GEN-005 | All resources must have environment tag | medium |
| POL-GEN-006 | Clusters must have auto-termination enabled | high |
| POL-GEN-007 | Production tables must have quality monitors | medium |
| POL-GEN-008 | All resources must have lifecycle status | low |
| POL-GEN-009 | Stale assets should be reviewed for deprecation | medium |
| POL-GEN-010 | Undocumented assets in production must be documented | high |

## Ontology Classes

- **UntaggedAsset** — Data assets missing a data_classification tag
- **StaleAsset** — Assets with no queries in 90+ days
- **UndocumentedAsset** — Assets with no comment or description
- **CostUnattributedAsset** — Compute assets missing cost_center tag
- **HighCostCompute** — Compute with cost_center but high monthly cost

## Dashboard Queries

`dashboard_queries.sql` contains SQL queries for governance health scoring,
tagging coverage, stale asset detection, and cost attribution gaps.
