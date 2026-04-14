# Customize the Ontology

This guide explains how to add resource classes and rule primitives to the Watchdog ontology for organization-specific governance needs.

## When to Add a Class

Add a new ontology class when:

- A set of resources shares governance requirements that differ from the base class (e.g., "HIPAA tables need extra controls beyond what ConfidentialAsset requires").
- Tag-based classification can identify the set (e.g., resources tagged `regulation=hipaa`).
- Existing classes do not capture the distinction (check the inheritance tree first).

Do not add a class when a policy with an `if_then` rule achieves the same result. Classes are for reusable categorization; conditional rules are for one-off checks.

## Ontology File Structure

The ontology lives in two files under `engine/ontologies/`:

```
engine/ontologies/
  resource_classes.yml    # Class hierarchy and classifiers
  rule_primitives.yml     # Reusable rule building blocks
```

### Base Classes

Base classes match on `resource_type` from the crawler. They form the roots of the inheritance tree:

```yaml
base_classes:
  DataAsset:
    description: "Any data object in Unity Catalog"
    matches_resource_types: [table, volume, catalog, schema]

  ComputeAsset:
    description: "Any compute resource"
    matches_resource_types: [job, cluster, warehouse, pipeline]

  IdentityAsset:
    description: "Users, groups, service principals"
    matches_resource_types: [user, group, service_principal]

  GrantAsset:
    description: "Any grant (permission assignment) in Unity Catalog"
    matches_resource_types: [grant]

  AgentAsset:
    description: "AI agent or agent execution"
    matches_resource_types: [agent, agent_execution]
```

### Derived Classes

Derived classes extend a parent and add tag-based classifiers:

```yaml
derived_classes:
  HipaaAsset:
    parent: DataAsset
    description: "Data subject to HIPAA regulations"
    classifier:
      tag_equals:
        regulation: "hipaa"
```

## Step-by-Step: Add a New Class

### 1. Define the Class

Add the class definition to `engine/ontologies/resource_classes.yml` under `derived_classes`:

```yaml
derived_classes:
  # ... existing classes ...

  HipaaAsset:
    parent: ConfidentialAsset
    description: "Data subject to HIPAA regulations (PHI/ePHI)"
    classifier:
      tag_equals:
        regulation: "hipaa"
```

### 2. Choose the Right Parent

The parent determines which policies the new class inherits. Common patterns:

| If the class represents... | Parent should be... |
|---------------------------|---------------------|
| Sensitive data with a regulation | `ConfidentialAsset` or `PiiAsset` |
| A table at a specific layer | `DataAsset` |
| A compute resource with special needs | `ComputeAsset` |
| An agent with specific risk | `AgentAsset` |

### 3. Define Tag-Based Classifiers

Classifiers use the same operators as rule primitives:

```yaml
# Exact match
classifier:
  tag_equals:
    regulation: "hipaa"

# Value in set
classifier:
  tag_in:
    regulation: [hipaa, hitech, phi]

# Tag key exists (any value)
classifier:
  tag_exists: [regulation, phi_indicator]

# Regex match
classifier:
  tag_matches:
    regulation: "^hipaa"

# AND composition
classifier:
  all_of:
    - tag_equals: { regulation: "hipaa" }
    - metadata_equals: { resource_type: "table" }

# OR composition
classifier:
  any_of:
    - tag_equals: { regulation: "hipaa" }
    - tag_equals: { regulation: "hitech" }

# NOT (exclusion)
classifier:
  none_of:
    - tag_equals: { environment: "dev" }

# Metadata check
classifier:
  metadata_equals:
    resource_type: "table"
```

### 4. Test Classification

Run an ad-hoc scan and check the `resource_classifications` table:

```sql
SELECT resource_name, class_name, class_ancestors
FROM platform.watchdog.resource_classifications
WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_classifications)
  AND class_name = 'HipaaAsset'
```

Resources must have the matching tags to be classified. If no resources appear, verify that the tags exist on the target resources.

### 5. Add Policies for the New Class

Create policies that target the new class:

```yaml
- id: POL-HIPAA-001
  name: "HIPAA assets must have a privacy officer designated"
  applies_to: HipaaAsset
  domain: RegulatoryCompliance
  severity: critical
  description: "HIPAA requires a designated privacy officer for all PHI data"
  remediation: "Add 'privacy_officer' tag with the officer's email"
  active: true
  rule:
    type: tag_exists
    keys: [privacy_officer]
```

## Adding Rule Primitives

When multiple policies share the same check, extract it into a named primitive in `engine/ontologies/rule_primitives.yml`:

```yaml
primitives:
  # ... existing primitives ...

  has_privacy_officer:
    type: tag_exists
    description: "HIPAA asset has a designated privacy officer"
    keys: [privacy_officer]

  has_baa_reference:
    type: tag_exists
    description: "Asset references a Business Associate Agreement"
    keys: [baa_id]

  hipaa_compliant:
    type: all_of
    description: "Full HIPAA compliance check"
    rules:
      - type: tag_exists
        keys: [privacy_officer]
      - type: tag_exists
        keys: [baa_id]
      - type: tag_exists
        keys: [retention_days]
      - type: tag_exists
        keys: [data_steward]
```

Policies then reference these primitives:

```yaml
rule:
  ref: hipaa_compliant
```

## Inheritance Implications

When adding a derived class, understand the inheritance chain:

```
DataAsset
  ├── ConfidentialAsset
  │     ├── PiiAsset
  │     │     └── PiiTable
  │     └── HipaaAsset (new)
  ├── GoldTable
  ├── SilverTable
  └── BronzeTable
```

A policy on `ConfidentialAsset` applies to `PiiAsset`, `PiiTable`, and `HipaaAsset`. When adding `HipaaAsset` under `ConfidentialAsset`:

- It inherits all `ConfidentialAsset` policies (e.g., POL-S002 "must have data steward").
- It inherits all `DataAsset` policies (e.g., POL-S003 "must have classification label").
- Policies targeting `HipaaAsset` specifically do not affect `PiiAsset` or other siblings.

## Full Inheritance Tree

```
DataAsset
├── PiiAsset
│   └── PiiTable
├── ConfidentialAsset
├── InternalAsset
├── PublicAsset
├── GoldTable
├── SilverTable
└── BronzeTable

ComputeAsset
├── ProductionJob
│   └── CriticalJob
├── ProductionPipeline
├── DevelopmentCompute
├── InteractiveCluster
├── UnattributedAsset
└── SharedCompute

IdentityAsset

GrantAsset
├── OverprivilegedGrant
└── DirectUserGrant

AgentAsset
├── ManagedModelEndpoint
├── AgentWithPiiAccess
├── AgentWithExternalAccess
├── AgentWithDataExport
├── UngovernedAgent
├── HighRiskExecution
└── ProductionAgent
```
