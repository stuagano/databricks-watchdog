# Financial Industry Policy Pack (SOX / PCI-DSS / GLBA)

Drop-in compliance policies for financial services organizations subject to
Sarbanes-Oxley (SOX), Payment Card Industry Data Security Standard (PCI-DSS),
and the Gramm-Leach-Bliley Act (GLBA).

## What's included

| File | Purpose |
|------|---------|
| `ontology_classes.yml` | 6 derived resource classes (FinancialReportingAsset, PciAsset, CardholderDataAsset, GlbaAsset, FinancialAuditTrail, SeparationOfDutiesAsset) |
| `rule_primitives.yml` | 11 reusable rule primitives for SOX, PCI-DSS, and GLBA checks |
| `policies.yml` | 12 policies: SOX (5), PCI-DSS (4), GLBA (3) |
| `dashboard_queries.sql` | SQL queries for financial compliance dashboards |

## Installation

1. Copy the ontology classes into your engine ontologies or merge them:

   ```bash
   cp library/financial/ontology_classes.yml engine/ontologies/financial_classes.yml
   ```

2. Copy the rule primitives:

   ```bash
   cp library/financial/rule_primitives.yml engine/ontologies/financial_primitives.yml
   ```

3. Copy the policies:

   ```bash
   cp library/financial/policies.yml engine/policies/financial.yml
   ```

4. (Optional) Import the dashboard queries into your Databricks SQL warehouse
   for compliance reporting.

## Prerequisites

- Resources must be tagged with the appropriate regulatory domain and data
  classification tags (e.g., `regulatory_domain: SOX`, `data_classification: pci`).
- The base `ConfidentialAsset` and `DataAsset` classes from
  `engine/ontologies/resource_classes.yml` must be loaded.

## Policies

### SOX (Sarbanes-Oxley)

| ID | Name | Severity |
|----|------|----------|
| POL-SOX-001 | Financial reporting assets must have a control owner | critical |
| POL-SOX-002 | Financial data changes must have audit trails | critical |
| POL-SOX-003 | Financial data must have change management controls | high |
| POL-SOX-004 | SOX controls must have a review cycle | high |
| POL-SOX-005 | Separation of duties must be enforced on financial data | critical |

### PCI-DSS (Payment Card Industry)

| ID | Name | Severity |
|----|------|----------|
| POL-PCI-001 | Cardholder data must be encrypted at rest and in transit | critical |
| POL-PCI-002 | PAN must be masked | critical |
| POL-PCI-003 | Cardholder data retention must be limited | high |
| POL-PCI-004 | PCI assets must have a data steward | high |

### GLBA (Gramm-Leach-Bliley Act)

| ID | Name | Severity |
|----|------|----------|
| POL-GLBA-001 | NPI must have a privacy notice reference | high |
| POL-GLBA-002 | NPI sharing must have opt-out mechanism | high |
| POL-GLBA-003 | GLBA audit trail tables must be immutable | medium |
