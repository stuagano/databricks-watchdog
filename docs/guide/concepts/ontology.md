# Ontology

## Why Classification Matters

Unity Catalog supports flat tags -- key-value pairs attached to resources. A tag like `data_classification=pii` states a fact about one resource. But flat tags have no structure. They cannot express that PII is a subset of Confidential, that Confidential is a subset of Internal, or that a policy targeting "all confidential data" should automatically apply to PII tables.

Watchdog's ontology adds an inheritance hierarchy on top of UC tags. Resources are classified into ontology classes based on their tags and metadata. Classes form a tree: a `PiiTable` inherits all policies that apply to `PiiAsset`, which inherits from `ConfidentialAsset`, which inherits from `DataAsset`.

The practical consequence: one policy on `ConfidentialAsset` covers PII, HIPAA, SOX, and every future child class. Adding a new sub-classification requires zero changes to existing policies.

## Base Classes

Five base classes correspond to the fundamental resource types in a Databricks workspace. Every crawled resource maps to exactly one base class by its `resource_type`:

| Base Class | Resource Types | Description |
|---|---|---|
| `DataAsset` | table, volume, catalog, schema | Any data object in Unity Catalog |
| `ComputeAsset` | job, cluster, warehouse, pipeline | Any compute resource |
| `IdentityAsset` | user, group, service_principal | Users, groups, and service principals |
| `GrantAsset` | grant | Any permission assignment in Unity Catalog |
| `AgentAsset` | agent, agent_execution | AI agents and their execution traces |

Base classes use `matches_resource_types` for classification -- no tags required.

## Derived Classes

Derived classes refine base classes using tag-based classifiers. A resource can belong to multiple derived classes simultaneously if its tags satisfy multiple classifiers.

### Data Classes

| Class | Parent | Classifier | Description |
|---|---|---|---|
| `PiiAsset` | DataAsset | `data_classification = pii` | Contains personally identifiable information |
| `ConfidentialAsset` | DataAsset | `data_classification IN (confidential, restricted, pii)` | Confidential business data |
| `InternalAsset` | DataAsset | `data_classification IN (internal, confidential, restricted, pii)` | Internal-only data |
| `PublicAsset` | DataAsset | `data_classification = public` | Approved for external sharing |
| `PiiTable` | PiiAsset | `data_classification = pii AND resource_type = table` | Table containing PII |
| `GoldTable` | DataAsset | `data_layer = gold` | Curated gold-layer table |
| `SilverTable` | DataAsset | `data_layer = silver` | Cleaned silver-layer table |
| `BronzeTable` | DataAsset | `data_layer = bronze` | Raw ingestion bronze-layer table |

### Compute Classes

| Class | Parent | Classifier | Description |
|---|---|---|---|
| `ProductionJob` | ComputeAsset | `environment = prod AND resource_type = job` | Production job |
| `ProductionPipeline` | ComputeAsset | `environment = prod AND resource_type = pipeline` | Production DLT pipeline |
| `DevelopmentCompute` | ComputeAsset | `environment IN (dev, sandbox, test)` | Development/sandbox compute |
| `InteractiveCluster` | ComputeAsset | `resource_type = cluster AND NOT cluster_type = job` | Interactive (all-purpose) cluster |
| `CriticalJob` | ProductionJob | `environment = prod AND criticality = high AND resource_type = job` | SLA-bound production job |
| `UnattributedAsset` | ComputeAsset | `cost_center tag missing` | Missing cost attribution |
| `SharedCompute` | ComputeAsset | `shared = true` | Multi-team compute resource |

### Identity and Grant Classes

| Class | Parent | Classifier | Description |
|---|---|---|---|
| `OverprivilegedGrant` | GrantAsset | `privilege = ALL PRIVILEGES OR MANAGE` | Overly broad permission |
| `DirectUserGrant` | GrantAsset | `grantee does not match ^(group:\|account group:)` | Grant to individual user, not group |

### Agent Classes

| Class | Parent | Classifier | Description |
|---|---|---|---|
| `ManagedModelEndpoint` | AgentAsset | `managed_endpoint = true` | Databricks FMAPI endpoint (not a customer agent) |
| `AgentWithPiiAccess` | AgentAsset | `accessed_pii = true` | Agent that accessed PII data recently |
| `AgentWithExternalAccess` | AgentAsset | `used_external_tool = true` | Agent calling external APIs |
| `AgentWithDataExport` | AgentAsset | `exported_data = true` | Agent exporting data outside the lakehouse |
| `UngovernedAgent` | AgentAsset | `agent_owner missing AND audit_logging_enabled missing` | No governance metadata |
| `HighRiskExecution` | AgentAsset | `resource_type = agent_execution AND accessed_pii = true` | Execution that accessed sensitive data |
| `ProductionAgent` | AgentAsset | `environment = prod` | Production-deployed agent |

## Inheritance

Ontology inheritance means a policy attached to a parent class applies to all descendants. Consider this hierarchy:

```
DataAsset
  |-- ConfidentialAsset
        |-- PiiAsset
              |-- PiiTable
```

A policy `POL-CONF-001: Confidential data must have a data steward` targets `ConfidentialAsset`. During evaluation, the engine checks this policy against:

- Every resource classified as `ConfidentialAsset`
- Every resource classified as `PiiAsset` (child of ConfidentialAsset)
- Every resource classified as `PiiTable` (grandchild of ConfidentialAsset)

Adding a new class `HipaaAsset` under `ConfidentialAsset` immediately inherits `POL-CONF-001` without editing the policy. Removing `HipaaAsset` removes it from evaluation. The policy definition never changes.

This is the primary advantage over flat tags: taxonomy changes propagate through the policy model automatically.

## Classifier Operators

Derived classes use classifier rules composed from these operators:

| Operator | Syntax | Description |
|---|---|---|
| `tag_equals` | `{ key: value }` | Exact tag match |
| `tag_in` | `{ key: [v1, v2] }` | Tag value in set |
| `tag_exists` | `[key1, key2]` | Tag keys present (any value) |
| `tag_matches` | `{ key: "regex" }` | Tag value matches regex |
| `all_of` | `[ ...classifiers ]` | AND -- all must match |
| `any_of` | `[ ...classifiers ]` | OR -- at least one must match |
| `none_of` | `[ ...classifiers ]` | NOT -- none may match |
| `metadata_equals` | `{ key: value }` | Check metadata field |
| `metadata_matches` | `{ key: "regex" }` | Metadata regex match |

These are the same operators used in policy rules (see [Policies](policies.md)), making the classifier and rule syntax consistent.

## Customizing the Ontology

Custom ontology classes are added by editing `engine/ontologies/resource_classes.yml`. Each class specifies a parent, a classifier, and a description:

```yaml
derived_classes:
  HipaaAsset:
    parent: ConfidentialAsset
    description: "Data subject to HIPAA regulations"
    classifier:
      tag_equals:
        regulatory_domain: "hipaa"
```

Industry policy packs (in `library/`) ship pre-built ontology extensions for healthcare, financial services, and defense. See the how-to guide for adding custom classes (coming soon).
