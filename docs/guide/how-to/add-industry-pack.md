# Deploy Industry Policy Packs

This guide covers deploying pre-built policy packs for regulated industries and creating custom packs for organization-specific needs.

## What Is a Policy Pack?

A policy pack is a self-contained set of governance components for a specific industry or regulatory framework. Each pack includes:

| Component | File | Purpose |
|-----------|------|---------|
| Ontology classes | `ontology_classes.yml` | Industry-specific resource classifications |
| Rule primitives | `rule_primitives.yml` | Reusable checks for industry regulations |
| Policies | `policies.yml` | Governance rules referencing the classes and primitives |
| Dashboard SQL | `dashboard_queries.sql` | SQL queries for industry-specific dashboard pages |
| README | `README.md` | Pack documentation and deployment instructions |

## Available Packs

Packs live in the `library/` directory:

| Pack | Directory | Coverage |
|------|-----------|----------|
| **General** | `library/general/` | Baseline governance: ownership, classification, cost attribution |
| **Healthcare** | `library/healthcare/` | HIPAA, PHI protection, BAA tracking |
| **Financial** | `library/financial/` | SOX, PCI-DSS, GLBA compliance |
| **Defense** | `library/defense/` | ITAR, CUI, CMMC, cleared personnel |

## Deploying a Pack

### Step 1: Review the Pack Contents

Read the pack's README and review the policies to understand what it enforces:

```bash
cat library/healthcare/README.md
cat library/healthcare/policies.yml
```

### Step 2: Copy Ontology Classes

Merge the pack's ontology classes into the engine's class hierarchy:

```bash
# Append industry classes to resource_classes.yml
cat library/healthcare/ontology_classes.yml >> engine/ontologies/resource_classes.yml
```

Alternatively, add them manually to maintain clean organization:

```yaml
# In engine/ontologies/resource_classes.yml, under derived_classes:

  # ── Healthcare (HIPAA) ───────────────────────────────────────────
  HipaaAsset:
    parent: ConfidentialAsset
    description: "Data subject to HIPAA regulations"
    classifier:
      tag_equals:
        regulation: "hipaa"
```

### Step 3: Copy Rule Primitives

Merge industry-specific primitives:

```bash
cat library/healthcare/rule_primitives.yml >> engine/ontologies/rule_primitives.yml
```

### Step 4: Copy Policies

Copy the policy file to the engine's policies directory:

```bash
cp library/healthcare/policies.yml engine/policies/healthcare.yml
```

### Step 5: Deploy Dashboard SQL

Add the industry-specific dashboard queries to the Lakeview dashboard or Genie Space. Each pack includes SQL files that create additional dashboard pages.

### Step 6: Sync and Verify

Run the evaluate entrypoint with `--sync-policies` to push the new policies to Delta:

```bash
python -m watchdog.entrypoints evaluate \
  --catalog platform \
  --schema watchdog \
  --sync-policies
```

Verify the policies loaded:

```sql
SELECT policy_id, policy_name, domain, active
FROM platform.watchdog.policies
WHERE source_file = 'healthcare.yml'
ORDER BY policy_id
```

## Creating a Custom Pack

### Directory Structure

Create a new directory under `library/`:

```
library/my-industry/
  ontology_classes.yml    # New derived classes
  rule_primitives.yml     # Reusable rule building blocks
  policies.yml            # Policy definitions
  dashboard_queries.sql   # Dashboard SQL
  README.md               # Documentation
```

### Ontology Classes Template

```yaml
# my-industry ontology classes
derived_classes:
  RegulatedAsset:
    parent: DataAsset
    description: "Data subject to industry regulation"
    classifier:
      tag_equals:
        regulation: "my-industry"

  HighRiskAsset:
    parent: RegulatedAsset
    description: "High-risk regulated data requiring enhanced controls"
    classifier:
      all_of:
        - tag_equals: { regulation: "my-industry" }
        - tag_equals: { risk_level: "high" }
```

### Rule Primitives Template

```yaml
# my-industry rule primitives
primitives:
  has_compliance_officer:
    type: tag_exists
    description: "Asset has a designated compliance officer"
    keys: [compliance_officer]

  has_audit_trail:
    type: tag_equals
    description: "Asset has audit trail enabled"
    key: audit_trail_enabled
    value: "true"

  industry_compliant:
    type: all_of
    description: "Full industry compliance check"
    rules:
      - type: tag_exists
        keys: [compliance_officer]
      - type: tag_exists
        keys: [retention_days]
      - type: tag_equals
        key: audit_trail_enabled
        value: "true"
```

### Policies Template

```yaml
# my-industry policies
policies:
  - id: POL-IND-001
    name: "Regulated assets must have a compliance officer"
    applies_to: RegulatedAsset
    domain: RegulatoryCompliance
    severity: critical
    description: "Industry regulation requires a designated compliance officer"
    remediation: "Add 'compliance_officer' tag with the officer's email"
    active: true
    rule:
      ref: has_compliance_officer
```

### Testing

After deploying a pack, verify with an ad-hoc scan:

```bash
python -m watchdog.entrypoints adhoc \
  --catalog platform \
  --schema watchdog \
  --secret-scope watchdog
```

Check that resources are classified into the new classes:

```sql
SELECT class_name, COUNT(*) as resource_count
FROM platform.watchdog.resource_classifications
WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_classifications)
  AND class_name IN ('RegulatedAsset', 'HighRiskAsset')
GROUP BY class_name
```
