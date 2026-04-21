# Ontology Classes Reference

Watchdog classifies every crawled resource into one or more ontology classes based on its tags and metadata. Classes form an inheritance tree: policies on a parent class apply to all descendants.

## Inheritance Tree

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

RowFilterAsset

ColumnMaskAsset

GroupMemberAsset

AgentAsset
├── ManagedModelEndpoint
├── AgentWithPiiAccess
├── AgentWithExternalAccess
├── AgentWithDataExport
├── UngovernedAgent
├── HighRiskExecution
└── ProductionAgent
```

## Base Classes

Base classes match on the `resource_type` field from the crawler. Every resource is assigned to exactly one base class.

| Class | Resource Types | Description |
|-------|---------------|-------------|
| `DataAsset` | table, volume, catalog, schema | Any data object in Unity Catalog |
| `ComputeAsset` | job, cluster, warehouse, pipeline | Any compute resource |
| `IdentityAsset` | user, group, service_principal | Users, groups, service principals |
| `GrantAsset` | grant | Any grant (permission assignment) in Unity Catalog |
| `RowFilterAsset` | row_filter | A Unity Catalog row filter applied to a table |
| `ColumnMaskAsset` | column_mask | A Unity Catalog column mask applied to a table column |
| `GroupMemberAsset` | group_member | A member of a workspace or account-level group |
| `AgentAsset` | agent, agent_execution | AI agent or agent execution |

`RowFilterAsset`, `ColumnMaskAsset`, and `GroupMemberAsset` currently have no derived classes. Resources matching these base classes are classified solely by their `resource_type`.

## Derived Classes: DataAsset

| Class | Parent | Classifiers | Description |
|-------|--------|-------------|-------------|
| `PiiAsset` | DataAsset | `data_classification` = `pii` | Contains personally identifiable information |
| `ConfidentialAsset` | DataAsset | `data_classification` in (confidential, restricted, pii) | Contains confidential business data |
| `InternalAsset` | DataAsset | `data_classification` in (internal, confidential, restricted, pii) | Internal-only data |
| `PublicAsset` | DataAsset | `data_classification` = `public` | Data approved for external sharing |
| `PiiTable` | PiiAsset | `data_classification` = `pii` AND `resource_type` = `table` | Table containing PII data |
| `GoldTable` | DataAsset | `data_layer` = `gold` | Curated gold-layer table for consumption |
| `SilverTable` | DataAsset | `data_layer` = `silver` | Cleaned/conformed silver-layer table |
| `BronzeTable` | DataAsset | `data_layer` = `bronze` | Raw ingestion bronze-layer table |

## Derived Classes: ComputeAsset

| Class | Parent | Classifiers | Description |
|-------|--------|-------------|-------------|
| `ProductionJob` | ComputeAsset | `environment` = `prod` AND `resource_type` = `job` | Job running in production |
| `CriticalJob` | ProductionJob | `environment` = `prod` AND `criticality` = `high` AND `resource_type` = `job` | Business-critical production job (SLA-bound) |
| `ProductionPipeline` | ComputeAsset | `environment` = `prod` AND `resource_type` = `pipeline` | DLT pipeline in production |
| `DevelopmentCompute` | ComputeAsset | `environment` in (dev, sandbox, test) | Compute in dev/sandbox environment |
| `InteractiveCluster` | ComputeAsset | `resource_type` = `cluster` AND NOT `cluster_type` = `job` | Interactive (all-purpose) cluster |
| `UnattributedAsset` | ComputeAsset | NOT `cost_center` tag exists | Compute missing cost attribution tags |
| `SharedCompute` | ComputeAsset | `shared` = `true` | Shared compute resource (multi-team) |

## Derived Classes: GrantAsset

| Class | Parent | Classifiers | Description |
|-------|--------|-------------|-------------|
| `OverprivilegedGrant` | GrantAsset | `privilege` = `ALL PRIVILEGES` OR `privilege` = `MANAGE` | Grant with overly broad privileges |
| `DirectUserGrant` | GrantAsset | `grantee` does NOT match `^(group:\|account group:)` | Grant assigned directly to a user |

## Derived Classes: AgentAsset

| Class | Parent | Classifiers | Description |
|-------|--------|-------------|-------------|
| `ManagedModelEndpoint` | AgentAsset | `managed_endpoint` = `true` | Databricks FMAPI endpoint |
| `AgentWithPiiAccess` | AgentAsset | `accessed_pii` = `true` | Agent that accessed PII data |
| `AgentWithExternalAccess` | AgentAsset | `used_external_tool` = `true` | Agent calling external APIs |
| `AgentWithDataExport` | AgentAsset | `exported_data` = `true` | Agent exporting data outside lakehouse |
| `UngovernedAgent` | AgentAsset | NOT `agent_owner` AND NOT `audit_logging_enabled` | Agent with no governance metadata |
| `HighRiskExecution` | AgentAsset | `resource_type` = `agent_execution` AND `accessed_pii` = `true` | Execution that accessed sensitive data |
| `ProductionAgent` | AgentAsset | `environment` = `prod` | Agent deployed in production |

## Classification Behavior

- A resource can belong to multiple classes simultaneously. A table tagged `data_classification=pii` and `data_layer=gold` is classified as both `PiiAsset` (and `PiiTable`) and `GoldTable`.
- Class membership is determined by tag values at scan time. Adding or removing tags changes classification on the next scan.
- Base class assignment is automatic based on `resource_type`. Derived class assignment requires matching tag conditions.
- Resources with no matching derived class classifiers remain in their base class only.
- The `resource_classifications` table records all class assignments per scan, including the `class_ancestors` column showing the full inheritance chain.

## Classifier Operators

Classifiers in `resource_classes.yml` support these operators:

| Operator | Syntax | Behavior |
|----------|--------|----------|
| `tag_equals` | `{key: value}` | Exact tag value match |
| `tag_in` | `{key: [v1, v2]}` | Tag value in allowed set |
| `tag_exists` | `[key1, key2]` | Tag keys present (any value) |
| `tag_matches` | `{key: "regex"}` | Tag value matches regex |
| `metadata_equals` | `{key: value}` | Metadata field exact match |
| `metadata_matches` | `{key: "regex"}` | Metadata field regex match |
| `all_of` | `[...classifiers]` | AND: all must match |
| `any_of` | `[...classifiers]` | OR: at least one must match |
| `none_of` | `[...classifiers]` | NOT: none may match |
