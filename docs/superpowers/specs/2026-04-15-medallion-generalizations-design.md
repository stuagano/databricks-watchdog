# Medallion Generalizations: IRI Parameterization, Pipeline Freshness, Medallion Policies

> Design spec for extracting three patterns into Watchdog core for any medallion architecture deployment.
>
> Date: 2026-04-15

## Context

Three gaps exist in Watchdog's support for Bronze/Silver/Gold medallion architectures and Ontos integration:

1. The Ontos adapter has a hardcoded workspace IRI that should be parameterized per deployment.
2. Pipeline freshness is not crawled — operators can't write policies like "pipelines must succeed within X hours."
3. Medallion governance policies exist only for Gold tables. Bronze and Silver layers have no coverage.

## 1. Ontos IRI Parameterization

### Problem

`ontos-adapter/src/watchdog_governance/ontos_sync.py` line 44:

```python
ONTOLOGY_BASE_IRI = "https://<workspace>.databricks.com/ontology/watchdog/class/"
```

Every deployment of the Ontos adapter must edit source code to change this.

### Design

Add `ontology_base_iri` as a parameter to `sync_classifications_to_ontos()` with a three-level fallback:

1. Explicit `ontology_base_iri=` parameter (highest priority)
2. `WATCHDOG_ONTOLOGY_BASE_IRI` environment variable
3. Default: `https://{workspace_host}/ontology/watchdog/class/` where `workspace_host` comes from the WorkspaceClient config

Remove the module-level `ONTOLOGY_BASE_IRI` constant.

### Changes

| File | Change |
|------|--------|
| `ontos-adapter/src/watchdog_governance/ontos_sync.py` | Add `ontology_base_iri` param, implement fallback chain, remove hardcoded constant |

### IRI Construction

The IRI for a class named `GoldTable` becomes:

```
{ontology_base_iri}GoldTable
```

The trailing slash convention is enforced: if the resolved base IRI doesn't end with `/`, one is appended.

## 2. Pipeline Freshness Crawler

### Problem

The existing `_crawl_pipelines()` gets pipeline metadata (state, creator) from the SDK but not run history. Customers cannot write policies like "production pipelines must have succeeded within 24 hours" because freshness data isn't in the inventory.

### Design

Add `_crawl_pipeline_freshness()` as an enrichment crawler (same pattern as `_crawl_dqm_status()`). It reads `system.lakeflow.pipeline_event_log`, computes per-pipeline freshness metrics, and enriches existing pipeline rows in `resource_inventory` with metadata tags.

### Data Source

```sql
SELECT
    pipeline_id,
    event_type,
    timestamp,
    message
FROM system.lakeflow.pipeline_event_log
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAY
  AND event_type IN ('create_update', 'update_progress')
```

The `update_progress` events with message containing `COMPLETED` or `FAILED` indicate pipeline run outcomes. The `create_update` events mark run starts.

Note: The `system.lakeflow` schema is available on workspaces with system tables enabled. The event schema may vary across workspace versions — the crawler should select only the columns it needs and handle missing columns gracefully.

### Enrichment Tags

Per-pipeline tags written to existing `resource_inventory` rows (UPDATE, not INSERT):

| Tag | Type | Description |
|-----|------|-------------|
| `last_success_at` | ISO timestamp | Most recent successful completion |
| `last_failure_at` | ISO timestamp | Most recent failure (empty if none in 7d) |
| `failure_count_7d` | integer string | Number of failures in last 7 days |
| `freshness_hours` | integer string | Hours since last successful completion |
| `pipeline_health` | `healthy` / `degraded` / `failing` | Derived: healthy = succeeded recently with no failures; degraded = succeeded but has recent failures; failing = no success in 7d or last run failed |

### Health Derivation

```
if no runs in 7 days:         pipeline_health = "failing"
elif last run failed:          pipeline_health = "failing"
elif failure_count_7d > 0:     pipeline_health = "degraded"
else:                          pipeline_health = "healthy"
```

### Graceful Fallback

If `system.lakeflow.pipeline_event_log` is not available (not all workspaces have it enabled), print a warning and return an empty list — same pattern as `_crawl_dqm_status()`.

### Registration

Register `_crawl_pipeline_freshness` in `crawl_all()` via `_safe_crawl()`, after `_crawl_pipelines` (it enriches those rows).

### Changes

| File | Change |
|------|--------|
| `engine/src/watchdog/crawler.py` | Add `_crawl_pipeline_freshness()` method, register in `crawl_all()` |

### No New Tables

This is tag enrichment on existing pipeline inventory rows, not a new table. Same approach as DQM/LHM enrichment.

## 3. Medallion Governance Policies

### Problem

The ontology already defines `BronzeTable`, `SilverTable`, and `GoldTable` classes. Gold has 7 policies (POL-Q002 through POL-Q009). Bronze and Silver have zero dedicated policies. This leaves the majority of the medallion pipeline ungoverned.

### Design

Add `engine/policies/medallion_governance.yml` with policies covering all three layers. Add supporting rule primitives to `engine/ontologies/rule_primitives.yml`.

### New Rule Primitives

Add to `engine/ontologies/rule_primitives.yml`:

```yaml
has_source_system:
  type: tag_exists
  description: "Ingestion table identifies its source system (ERP, API, file drop, etc.)"
  keys: [source_system]

has_ingestion_owner:
  type: tag_exists
  description: "Ingestion table has a designated owner responsible for the data feed"
  keys: [ingestion_owner]

has_source_documentation:
  type: tag_exists
  description: "Table has a link to transformation or reconciliation documentation"
  keys: [source_documentation]

silver_has_classification:
  type: if_then
  description: "Silver tables must have data classification applied"
  condition:
    type: tag_equals
    key: data_layer
    value: "silver"
  then:
    type: tag_exists
    keys: [data_classification]
```

### New Policies

File: `engine/policies/medallion_governance.yml`

```yaml
policies:

  # ── Bronze Layer ─────────────────────────────────────────────────────

  - id: POL-MED-001
    name: "Bronze tables must identify their source system"
    applies_to: BronzeTable
    domain: DataQuality
    severity: high
    description: >
      Raw ingestion tables must be tagged with the source system they ingest
      from (e.g., SAP, Oracle, Salesforce, API). Without source tracking,
      lineage is broken and impact analysis is impossible.
    remediation: "Add a 'source_system' tag identifying the upstream source"
    active: true
    rule:
      ref: has_source_system

  - id: POL-MED-002
    name: "Bronze tables must have an ingestion owner"
    applies_to: BronzeTable
    domain: DataQuality
    severity: medium
    description: >
      Each ingestion feed needs a designated owner who is accountable for
      data freshness and schema changes from the source system.
    remediation: "Add an 'ingestion_owner' tag with the responsible person's email"
    active: true
    rule:
      ref: has_ingestion_owner

  # ── Silver Layer ─────────────────────────────────────────────────────

  - id: POL-MED-003
    name: "Silver tables must have a data steward"
    applies_to: SilverTable
    domain: DataQuality
    severity: high
    description: >
      The Silver layer is where raw data becomes business-usable. A named
      steward is accountable for transformation logic, data quality, and
      reconciliation correctness.
    remediation: "Add a 'data_steward' tag with the responsible person's email"
    active: true
    rule:
      ref: has_data_steward

  - id: POL-MED-004
    name: "Silver tables must have data classification"
    applies_to: SilverTable
    domain: DataQuality
    severity: high
    description: >
      Classification should be applied at the Silver layer where data is
      cleaned and conformed. Bronze is too raw (classifications may not
      yet be determinable). Gold inherits from Silver.
    remediation: "Add a 'data_classification' tag (public, internal, confidential, restricted, pii)"
    active: true
    rule:
      ref: has_data_classification

  - id: POL-MED-005
    name: "Silver tables must document their source transformations"
    applies_to: SilverTable
    domain: DataQuality
    severity: medium
    description: >
      Silver tables transform and reconcile data from one or more Bronze
      sources. The transformation logic must be documented so downstream
      consumers can understand data provenance.
    remediation: >
      Add a 'source_documentation' tag with a link to the transformation
      documentation (e.g., Confluence page, README, or notebook URL)
    active: true
    rule:
      ref: has_source_documentation

  # ── Gold Layer ───────────────────────────────────────────────────────

  - id: POL-MED-006
    name: "Gold tables must have a retention policy"
    applies_to: GoldTable
    domain: DataQuality
    severity: medium
    description: >
      Curated consumption tables must specify how long data is retained.
      Without retention policies, storage grows unbounded and compliance
      obligations (GDPR, HIPAA, SOX) cannot be verified.
    remediation: "Add a 'retention_days' tag with the maximum retention period in days"
    active: true
    rule:
      ref: has_retention_policy

  # ── Cross-Layer (Pipeline Freshness) ─────────────────────────────────

  - id: POL-MED-007
    name: "Production pipelines must be healthy"
    applies_to: ProductionPipeline
    domain: OperationalGovernance
    severity: critical
    description: >
      Production DLT pipelines must have completed successfully within the
      last 7 days. Failing pipelines mean stale data is reaching downstream
      consumers. Requires the pipeline freshness crawler to be active.
    remediation: >
      1. Check pipeline logs for failure cause
      2. Fix the failing pipeline and trigger a manual run
      3. Verify pipeline_health tag updates to 'healthy' on next Watchdog scan
    active: true
    rule:
      metadata_equals:
        pipeline_health: "healthy"

  - id: POL-MED-008
    name: "Production pipelines must have succeeded within 48 hours"
    applies_to: ProductionPipeline
    domain: OperationalGovernance
    severity: high
    description: >
      Production pipelines that haven't succeeded in 48 hours are delivering
      stale data. This is a tighter check than pipeline_health — a pipeline
      can be 'degraded' (recent failures but also recent successes) and still
      violate this policy if its last success is too old.
    remediation: >
      1. Investigate why the pipeline hasn't completed successfully
      2. Check for upstream dependencies, cluster issues, or schema changes
      3. The 48-hour threshold assumes daily pipelines — adjust for your cadence
    active: true
    rule:
      type: metadata_lte
      field: freshness_hours
      threshold: "48"
```

Note: POL-MED-008 uses `metadata_lte`, a new rule type. See section 3.1.

### 3.1 Rule Engine: `metadata_lte` Rule Type

POL-MED-008 needs "freshness_hours must be <= 48." The existing `metadata_gte` checks `field >= threshold`. We need the inverse.

Add `metadata_lte` to the rule engine dispatch table:

```python
"metadata_lte": self._eval_metadata_lte,
```

Schema:
```yaml
type: metadata_lte
field: freshness_hours    # metadata key to check
threshold: "48"           # maximum allowed value (numeric comparison)
```

Implementation follows `_eval_metadata_gte` exactly but reverses the comparison.

Update POL-MED-008 to use:
```yaml
rule:
  metadata_lte:
    field: freshness_hours
    threshold: "48"
```

Add `metadata_lte` to `rule_primitives.yml` header comment listing all rule types.

### New Ontology Classes

None. `BronzeTable`, `SilverTable`, `GoldTable`, and `ProductionPipeline` already exist in `engine/ontologies/resource_classes.yml`.

### Changes

| File | Change |
|------|--------|
| `engine/ontologies/rule_primitives.yml` | Add 4 primitives: `has_source_system`, `has_ingestion_owner`, `has_source_documentation`, `silver_has_classification`. Update header comment. |
| `engine/policies/medallion_governance.yml` | New file: 8 policies (POL-MED-001 through POL-MED-008) |
| `engine/src/watchdog/rule_engine.py` | Add `metadata_lte` rule type + `_eval_metadata_lte()` method |

### Policy Summary

| ID | Layer | Severity | Rule |
|----|-------|----------|------|
| POL-MED-001 | Bronze | high | Must have `source_system` tag |
| POL-MED-002 | Bronze | medium | Must have `ingestion_owner` tag |
| POL-MED-003 | Silver | high | Must have `data_steward` tag |
| POL-MED-004 | Silver | high | Must have `data_classification` tag |
| POL-MED-005 | Silver | medium | Must have `source_documentation` tag |
| POL-MED-006 | Gold | medium | Must have `retention_days` tag |
| POL-MED-007 | Pipeline | critical | Pipeline health must be `healthy` |
| POL-MED-008 | Pipeline | high | Freshness must be <= 48 hours |

## Testing

### Unit Tests

| Test | File |
|------|------|
| `metadata_lte` rule type: pass, fail, missing field, non-numeric | `tests/unit/test_rule_engine.py` |
| Pipeline freshness tag derivation (healthy, degraded, failing) | `tests/unit/test_crawler.py` (new) |
| Medallion policies parse and reference valid primitives | `tests/unit/test_policy_packs.py` (existing pattern) |
| New primitives are valid rule definitions | `tests/unit/test_rule_engine.py` |

### Integration Tests

| Test | Scope |
|------|-------|
| Ontos sync with parameterized IRI | `tests/integration/` (if Ontos test environment available) |
| Pipeline freshness enrichment writes expected tags | `tests/integration/test_policy_lifecycle.py` |

## File Change Summary

| File | Type | Description |
|------|------|-------------|
| `ontos-adapter/src/watchdog_governance/ontos_sync.py` | modify | Parameterize IRI, remove hardcoded URL |
| `engine/src/watchdog/crawler.py` | modify | Add `_crawl_pipeline_freshness()`, register in `crawl_all()` |
| `engine/src/watchdog/rule_engine.py` | modify | Add `metadata_lte` rule type |
| `engine/ontologies/rule_primitives.yml` | modify | Add 4 medallion primitives |
| `engine/policies/medallion_governance.yml` | create | 8 medallion governance policies |
| `tests/unit/test_rule_engine.py` | modify | Tests for `metadata_lte` |
| `tests/unit/test_crawler.py` | create | Tests for pipeline freshness tag derivation |
