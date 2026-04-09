# Databricks Watchdog

**Compliance posture evaluator for Unity Catalog.**

The platform enforces governance at query time (ABAC, tag policies, column masks). Watchdog answers the question nobody else answers: *"across all my policies, how compliant is my estate right now, who owns the gaps, and is it getting better or worse?"*

## What You Get in 30 Minutes

Deploy Watchdog to a workspace and run one scan. Here's what comes back:

### Cross-Domain Compliance Posture

One view across security, data quality, cost, and operations:

| Domain | Resources | Critical | High | Medium |
|---|---|---|---|---|
| SecurityGovernance | 2,352 | 8 | 4,480 | 10 |
| CostGovernance | 2,374 | 0 | 40 | 2,431 |
| DataQuality | 2,134 | 0 | 3 | 3,492 |
| OperationalGovernance | 22 | 0 | 0 | 22 |

*The Governance Hub shows tag policies, ABAC rules, and DQ monitors separately. Nobody aggregates across all of them.*

### Owner Accountability

Every violation is attributed to a resource owner with remediation steps:

| Owner | Total | Critical | High | Policies Violated | Domains |
|---|---|---|---|---|---|
| stuart.gano@ | 358 | 1 | 96 | 20 | 4 |
| System user | 896 | 0 | 296 | 7 | 3 |
| eric.popowich@ | 870 | 0 | 268 | 7 | 3 |

*The platform has no concept of "violations per owner" or accountability tracking.*

### Ontology Classification with Inheritance

Resources are classified into a hierarchy. One policy on `ConfidentialAsset` automatically covers every `PiiAsset`, `HipaaAsset`, and `SoxAsset`:

```
PiiTable → PiiAsset → ConfidentialAsset → DataAsset
```

*UC has flat tags. Change a policy at the ConfidentialAsset level? With tags, you'd edit every child policy individually.*

### Actionable Remediation Lists

Watchdog caught 211 direct-user grants (POL-A002):

```
3f550a6e-...  has USE_SCHEMA  on schema  explain_my_bill
account users has SELECT      on table   property_listing_sample
```

An admin takes this list and migrates every grant to group-based access. The platform shows grants in `information_schema` but doesn't flag which ones violate your policies.

### Composable Rules the Platform Can't Express

| Rule | What It Checks | Platform Equivalent |
|---|---|---|
| IF `data_classification = pii` THEN must have BOTH `data_steward` AND `retention_days` | Cross-tag conditional | **None** |
| Grant grantee must match regex `^(group:\|account group:)` | Grant metadata evaluation | **None** |
| IF `environment = prod` THEN must have `on_call_team` AND `alert_channel` | Conditional tag requirement | **None** |

Tag Policies enforce "this tag must use these values." Watchdog rules express arbitrary logic across tags and metadata.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Watchdog Engine (Daily Scan Job)                                │
│  Crawl 14 resource types → Classify via ontology → Evaluate     │
│  37+ policies → Track violations with lifecycle → Notify owners  │
└───────────┬───────────────────┬──────────────────┬──────────────┘
            │                   │                  │
    ┌───────▼───────┐  ┌───────▼────────┐  ┌──────▼──────────────┐
    │ Lakeview       │  │ Watchdog MCP   │  │ Guardrails MCP      │
    │ Dashboard      │  │ 9 AI tools     │  │ 9 build-time tools  │
    │ 5 pages        │  │ for assistants │  │ for AI agents       │
    ├────────────────┤  ├────────────────┤  │ "Is this table      │
    │ Genie Space    │  │ Ontos Adapter  │  │  safe to use?"      │
    │ 19 tables      │  │ Business       │  └─────────────────────┘
    │ NL governance  │  │ catalog views  │
    └────────────────┘  └────────────────┘
```

**Delta tables are the contract.** Every consumer reads the same tables. No APIs between layers.

---

## Quickstart (30 minutes)

### Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- **Databricks CLI** v0.230+ (`databricks --version`)
- A catalog where Watchdog can create a `watchdog` schema
- A SQL warehouse (Starter Warehouse works)

### Step 1: Clone and configure

```bash
git clone https://github.com/stuagano/databricks-watchdog.git
cd databricks-watchdog
```

Edit `engine/databricks.yml` — add a target for your workspace:

```yaml
targets:
  my-workspace:
    mode: development
    default: true
    workspace:
      host: https://your-workspace.cloud.databricks.com
      profile: your-profile
    variables:
      catalog: your_catalog
      schema: watchdog
```

### Step 2: Deploy the engine

```bash
cd engine
databricks bundle validate -t my-workspace
databricks bundle deploy -t my-workspace
```

### Step 3: Run your first scan

```bash
databricks bundle run watchdog_adhoc_scan -t my-workspace
```

This crawls all resources, classifies them, evaluates 37+ policies, and writes violations. Takes 2-3 minutes.

### Step 4: Check results

```sql
-- How many violations?
SELECT severity, COUNT(*) FROM your_catalog.watchdog.violations
WHERE status = 'open' GROUP BY severity;

-- Who owns the most?
SELECT owner, COUNT(*) as violations
FROM your_catalog.watchdog.violations
WHERE status = 'open' GROUP BY owner ORDER BY violations DESC LIMIT 10;

-- What's the compliance posture by domain?
SELECT * FROM your_catalog.watchdog.v_domain_compliance;
```

### Step 5: Deploy the Lakeview dashboard (optional)

```bash
python engine/dashboards/lakeview/deploy_dashboard.py \
  --profile your-profile \
  --catalog your_catalog \
  --schema watchdog \
  --warehouse-id your_warehouse_id \
  --publish
```

5-page governance dashboard: Compliance Overview, Owner Accountability, Resource Compliance, Access Governance, Data Quality.

### Step 6: Deploy the MCP server (optional)

Edit `mcp/databricks.yml` with your workspace target, then:

```bash
cd mcp
databricks bundle deploy -t my-workspace
databricks apps start watchdog-mcp-my-workspace --profile your-profile
# Wait for compute to be ACTIVE, then:
databricks apps deploy watchdog-mcp-my-workspace \
  --source-code-path /Workspace/Users/you@company.com/.bundle/watchdog-mcp/my-workspace/files \
  --profile your-profile
```

Connect from Claude Code: `https://<app-url>/mcp/sse`

### Step 7: Deploy the Genie Space (optional)

```bash
python mcp/genie/deploy_genie_space.py \
  --catalog your_catalog \
  --schema watchdog \
  --warehouse-id your_warehouse_id \
  --profile your-profile
```

Business users can ask: "Who has the most critical violations?" or "Which PII tables lack a data steward?"

### Step 8: Deploy AI guardrails (optional)

Edit `guardrails/databricks.yml` with your workspace target, then:

```bash
cd guardrails
databricks bundle deploy -t my-workspace
databricks apps start mcp-ai-guardrails-my-workspace --profile your-profile
# Wait for ACTIVE, then deploy code (same pattern as MCP server)
```

AI agents get build-time governance: "Is this table safe to use in my agent?"

---

## Industry Policy Packs

Drop-in YAML packs for regulated industries. Copy into `engine/ontologies/` and `engine/policies/`:

| Pack | Policies | Ontology Classes | Covers |
|---|---|---|---|
| `library/healthcare/` | 10 (POL-HIPAA-*) | 4 (PhiAsset, EphiAsset, HipaaAuditAsset, DeIdentifiedDataset) | HIPAA: PHI stewardship, encryption, access logging, retention, BAA, minimum necessary, breach notification |
| `library/financial/` | 12 (POL-SOX-*, POL-PCI-*, POL-GLBA-*) | 6 (FinancialReportingAsset, PciAsset, GlbaAsset, ...) | SOX audit trails, separation of duties. PCI encryption/masking. GLBA privacy notices. |
| `library/defense/` | 8 (POL-NIST-*, POL-CMMC-*, POL-ITAR-*) | 5 (CuiAsset, ItarAsset, CmmcLevel2Asset, ...) | NIST 800-171, CMMC Level 2, ITAR export control |
| `library/general/` | 10 (POL-GEN-*) | 5 (UntaggedAsset, StaleAsset, UndocumentedAsset, ...) | CIS-style benchmarks: classification, documentation, cost attribution, lifecycle, monitoring |

Each pack includes ontology classes, rule primitives, policies, and dashboard SQL queries.

---

## What's Deployed (Full Stack)

| Component | What | Deploy Command |
|---|---|---|
| **Engine** | Daily scan job — crawl, classify, evaluate, track violations | `cd engine && databricks bundle deploy` |
| **Lakeview Dashboard** | 5-page governance posture dashboard | `python engine/dashboards/lakeview/deploy_dashboard.py` |
| **Watchdog MCP** | 9 AI tools for compliance queries | `cd mcp && databricks bundle deploy` + app deploy |
| **Genie Space** | NL governance exploration (19 tables: Watchdog + UC system tables) | `python mcp/genie/deploy_genie_space.py` |
| **Guardrails MCP** | 9 build-time governance tools for AI agents | `cd guardrails && databricks bundle deploy` + app deploy |
| **Ontos Adapter** | Pluggable governance module for Ontos business catalog | Drop-in to Ontos fork |

---

## Data Model

### Tables

| Table | Purpose |
|---|---|
| `resource_inventory` | All discovered resources per scan (14 types, tags, metadata, metastore_id) |
| `resource_classifications` | Ontology class assignments (resource_id → class_name with ancestor chain) |
| `policies` | Policy definitions — YAML-synced + user-created (hybrid management) |
| `policies_history` | Append-only audit trail of policy changes |
| `scan_results` | Every (resource, policy) evaluation result per scan |
| `violations` | Open violations — deduplicated, with status lifecycle (open/resolved/exception) |
| `exceptions` | Approved policy exceptions with expiration dates |
| `notification_queue` | Per-owner notification digests (CDF-enabled) |

### Semantic Views

| View | Purpose |
|---|---|
| `v_resource_compliance` | Per-resource violation counts by severity |
| `v_class_compliance` | Compliance % per ontology class |
| `v_domain_compliance` | Executive posture by governance domain |
| `v_tag_policy_coverage` | Tag policy satisfaction per resource |
| `v_data_classification_summary` | Classification coverage % by catalog |
| `v_dq_monitoring_coverage` | DQM/LHM monitoring status per table |
| `v_cross_metastore_compliance` | Compliance % per metastore (multi-metastore) |
| `v_cross_metastore_inventory` | Resource counts per metastore |

---

## MCP Tools

### Watchdog MCP (compliance posture queries)

| Tool | Description |
|---|---|
| `get_violations` | Filter violations by status, severity, resource_type, policy, owner, metastore |
| `get_governance_summary` | High-level compliance metrics across all domains |
| `get_policies` | List all active policies |
| `get_scan_history` | View recent scan results |
| `get_resource_violations` | Full compliance history for a specific resource |
| `get_exceptions` | List approved exceptions |
| `explain_violation` | Plain-language explanation with remediation steps |
| `what_if_policy` | Simulate a proposed policy against current inventory |
| `list_metastores` | List scanned metastores with resource counts |

### Guardrails MCP (AI build-time governance)

| Tool | Description |
|---|---|
| `validate_table_usage` | Check if a table is safe for an AI agent to use |
| `discover_governed_assets` | Find assets with ontology classes and compliance status |
| `check_policy_compliance` | Evaluate resource against all applicable policies |
| `build_safely` | Combined classification + violation + policy check |
| + 5 more | SQL validation, cost estimation, column safety, audit logging |

---

## Customization

### Adding ontology classes

```yaml
# engine/ontologies/resource_classes.yml
derived_classes:
  HipaaAsset:
    parent: ConfidentialAsset
    description: "Subject to HIPAA regulations"
    classifier:
      tag_equals:
        regulatory_domain: "HIPAA"
```

Classifiers: `tag_equals`, `tag_in`, `tag_exists`, `tag_matches`, `metadata_equals`, `metadata_matches`, `all_of`, `any_of`, `none_of`.

### Writing policies

```yaml
# engine/policies/my_policies.yml
policies:
  - id: POL-CUSTOM-001
    name: "PII must have a data steward"
    applies_to: PiiAsset
    domain: SecurityGovernance
    severity: critical
    description: "Every PII asset needs a named steward"
    remediation: "Add a 'data_steward' tag"
    active: true
    rule:
      ref: has_data_steward
```

Rules support: `tag_exists`, `tag_equals`, `tag_in`, `metadata_equals`, `all_of`, `any_of`, `none_of`, `if_then`, `metadata_gte`, and references to named primitives.

### Multi-metastore scanning

Set `WATCHDOG_METASTORE_IDS=ms-123,ms-456` and use the `crawl_all_metastores` entrypoint. All results go to the same tables with a `metastore_id` discriminator.

---

## Why Watchdog Exists

| What the Platform Does | What Watchdog Does |
|---|---|
| ABAC masks a column at query time | Measures "what % of PII tables have ABAC coverage" |
| Tag Policies reject invalid values | Evaluates cross-tag rules ("if PII then must have steward AND retention") |
| DQ Monitoring detects anomalies | Evaluates "do all gold tables have DQ monitors enabled" |
| Governance Hub shows dashboards | Tracks violations as stateful objects with owner accountability |
| Nothing | Provides cross-domain compliance posture over time |
| Nothing | Ontology-based classification with policy inheritance |
| Nothing | Per-owner violation digests with remediation steps |
| Nothing | AI interface (MCP) for governance posture queries |

The platform is the immune system — it blocks bad things at runtime.
Watchdog is the annual physical — it measures overall health, tracks trends, and tells you what to fix.

---

## Directory Structure

```
databricks-watchdog/
├── engine/                          # Core — DAB bundle for the governance scanner
│   ├── databricks.yml               #   Bundle config (add your workspace targets here)
│   ├── src/watchdog/                #   Engine source: crawler, ontology, rules, violations
│   ├── ontologies/                  #   Classification hierarchy + rule primitives (YAML)
│   ├── policies/                    #   Governance policies by domain (YAML)
│   ├── dashboards/                  #   Lakeview dashboard template + SQL queries
│   └── resources/                   #   Workflow job definitions
│
├── mcp/                             # Watchdog MCP server (Databricks App)
│   ├── src/watchdog_mcp/            #   9 governance tools over SSE
│   └── genie/                       #   Genie Space template + deploy script
│
├── guardrails/                      # AI DevKit guardrails MCP (Databricks App)
│   └── src/ai_devkit/               #   9 build-time governance tools
│
├── ontos-adapter/                   # Governance module for Ontos business catalog
│   └── src/watchdog_governance/     #   GovernanceProvider protocol + routers
│
├── library/                         # Industry policy packs
│   ├── healthcare/                  #   HIPAA (10 policies, 4 classes)
│   ├── financial/                   #   SOX/PCI/GLBA (12 policies, 6 classes)
│   ├── defense/                     #   NIST/CMMC/ITAR (8 policies, 5 classes)
│   └── general/                     #   CIS benchmarks (10 policies, 5 classes)
│
├── terraform/                       # Infrastructure as Code (SP, catalog, grants)
├── template/                        # Blank starting point for new customers
├── customer/                        # Worked example
├── docs/                            # Roadmap, positioning, integration plan
└── tests/                           # 289 unit tests
```

## Testing

```bash
pip install pytest pyyaml
python -m pytest tests/unit/ -q
# 289 passed
```

## Acknowledgments

Original concept by Ben Sivoravong. Engine and platform implementation by Stuart Gano.
