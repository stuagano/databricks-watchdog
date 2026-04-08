# Databricks Watchdog — Governance Scanner

A config-driven governance scanner for Databricks. Write policies in YAML, Watchdog scans your workspace daily and writes results to Delta tables. Query compliance through AI/BI dashboards or an MCP server that plugs into any AI assistant.

## What You Get

- **Resource crawler** — enumerates 12 resource types (tables, jobs, clusters, pipelines, warehouses, volumes, catalogs, schemas, users, groups, service principals) via SDK + Unity Catalog
- **Ontology engine** — tag-based classification hierarchy (e.g., a table tagged `data_classification=pii` becomes a `PiiAsset` which inherits all `DataAsset` policies)
- **Policy engine** — declarative rules evaluated against resource tags/metadata with support for composition (`all_of`, `any_of`, `none_of`, `if_then`)
- **Violation tracking** — deduplication via MERGE, status lifecycle (open → resolved/exception), exception management with expiration
- **Notification service** — dual-path: Delta queue for enterprise email integration + optional Azure Communication Services direct email
- **AI/BI dashboards** — 8 pre-built SQL queries for Lakeview dashboards (compliance summary, violations by owner, data quality coverage)
- **MCP server** — 6 governance tools (get_violations, get_governance_summary, get_policies, get_scan_history, get_resource_violations, get_exceptions) with on-behalf-of auth
- **Terraform module** — provisions service principal, secret scope, catalog, schema, and UC grants

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Daily Scan Job (Databricks Workflow)                           │
│                                                                 │
│  Task 1: crawl_resources                                        │
│    SDK + information_schema → resource_inventory (Delta)         │
│                                                                 │
│  Task 2: evaluate_policies                                      │
│    ontology classify → rule engine evaluate → violations MERGE   │
│    YAML policies synced to policies table                        │
│                                                                 │
│  Task 3: send_notifications                                     │
│    Per-owner digests → notification_queue + optional ACS email   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────┐  ┌──────────────────────────────────────┐
│  AI/BI Dashboards    │  │  MCP Server (Databricks App)          │
│  (Lakeview)          │  │  On-behalf-of auth — each user's      │
│  SQL queries against │  │  UC grants govern data access          │
│  watchdog schema     │  │  6 governance tools for AI assistants  │
└─────────────────────┘  └──────────────────────────────────────┘
```

### Data Model

| Table | Purpose |
|-------|---------|
| `resource_inventory` | All discovered resources per scan (scan_id, resource_type, resource_id, tags, metadata) |
| `resource_classifications` | Ontology class assignments per scan (resource_id → class_name with ancestors) |
| `policies` | Policy definitions — YAML-synced + user-created (hybrid management) |
| `policies_history` | Append-only audit trail of policy changes |
| `scan_results` | Every (resource, policy) evaluation result per scan |
| `violations` | Open violations — deduplicated, with status lifecycle |
| `exceptions` | Approved policy exceptions with expiration dates |
| `notification_queue` | Per-owner notification digests (CDF-enabled for downstream consumers) |
| `audit_log` | All system activity |

### Semantic Views

| View | Purpose |
|------|---------|
| `v_resource_compliance` | Per-resource, per-class compliance posture |
| `v_class_compliance` | Aggregated by ontology class ("how are GoldTables doing?") |
| `v_domain_compliance` | Aggregated by governance domain (executive summary) |

## Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- **Databricks CLI** v0.230+ with bundle support (`databricks bundle --version`)
- **Terraform** >= 1.5 (for infrastructure provisioning)
- **Azure service principal** credentials (client_id, client_secret, tenant_id)
- Python 3.10+ (for local testing)

## Quickstart

### 1. Clone and configure

```bash
git clone <this-repo>
cd databricks-watchdog
```

### 2. Provision infrastructure (Terraform)

The Terraform module creates the service principal registration, secret scope, platform catalog, watchdog schema, and UC grants.

```bash
cd terraform/modules/watchdog

# Create a terraform.tfvars with your values:
cat > terraform.tfvars <<EOF
service_principal_application_id = "<SP_APP_ID>"
service_principal_secret         = "<SP_SECRET>"
tenant_id                        = "<AZURE_TENANT_ID>"
subscription_id                  = "<AZURE_SUBSCRIPTION_ID>"
catalog_name                     = "platform"
schema_name                      = "watchdog"
EOF

terraform init
terraform plan
terraform apply
cd ../../..
```

### 3. Customize for your customer

**Required:**
- Edit `engine/databricks.yml` — replace `<YOUR_*>` placeholders with workspace URLs
- Edit `engine/ontologies/compliance_domains.yml` — set escalation emails

**Optional (recommended):**
- Add customer-specific ontology classes to `engine/ontologies/resource_classes.yml`
- Add customer-specific policies to `engine/policies/` (any `.yml` file is auto-loaded)
- Add customer-specific rule primitives to `engine/ontologies/rule_primitives.yml`

Use `template/` as a starting point — it has skeleton files with commented examples. See `customer/` for a complete worked example (HIPAA/SOX regulatory, business unit scoping).

### 4. Deploy the engine

```bash
cd engine
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

### 5. Verify

Run the ad-hoc scan job to confirm everything works:

```bash
databricks bundle run watchdog_adhoc_scan -t dev
```

Check results:
```sql
SELECT * FROM platform.watchdog.resource_inventory LIMIT 10;
SELECT * FROM platform.watchdog.violations WHERE status = 'open';
```

### 6. Deploy the MCP server (optional)

```bash
cd ../mcp
# Edit databricks.yml with workspace URL and warehouse ID
databricks bundle deploy -t nonprod
```

## Directory Structure

```
databricks-watchdog/
├── engine/                          # DAB bundle root — the governance scanner
│   ├── databricks.yml               #   Bundle manifest (customize targets here)
│   ├── setup.py                     #   Python package definition
│   ├── src/watchdog/                #   Core engine source code
│   │   ├── crawler.py               #     Resource enumeration (12 types)
│   │   ├── ontology.py              #     Tag-based classification engine
│   │   ├── rule_engine.py           #     Declarative rule evaluator
│   │   ├── policy_engine.py         #     Two-pass evaluation orchestrator
│   │   ├── policy_loader.py         #     YAML + Delta hybrid policy loading
│   │   ├── violations.py            #     MERGE logic + exception handling
│   │   ├── views.py                 #     Semantic compliance views
│   │   ├── notifications.py         #     Delta queue + ACS email
│   │   ├── ontology_export.py       #     OWL/Turtle export for Ontos
│   │   └── entrypoints.py           #     CLI entrypoints for workflow tasks
│   ├── src/run_task.py              #   Task dispatcher (used by job YAML)
│   ├── ontologies/                  #   Classification hierarchy + rule primitives
│   ├── policies/                    #   Governance policies by domain (YAML)
│   ├── dashboards/                  #   AI/BI dashboard SQL queries
│   ├── notebooks/                   #   Exception management notebooks
│   └── resources/                   #   Job + warehouse definitions
│
├── mcp/                             # Separate DAB — MCP server (Databricks App)
│   ├── databricks.yml
│   ├── src/watchdog_mcp/
│   └── resources/
│
├── ontos-adapter/                   # Pluggable governance UI for Ontos
│   ├── pyproject.toml               #   watchdog-governance package
│   ├── src/watchdog_governance/     #   Provider protocol + FastAPI routers
│   └── frontend/                    #   React views (drop into Ontos fork)
│
├── guardrails/                      # AI DevKit guardrails MCP server
│   ├── databricks.yml               #   DAB bundle (mcp-ai-guardrails app)
│   ├── src/ai_devkit/               #   9 MCP tools + watchdog integration
│   │   ├── server.py                #     FastAPI + SSE transport
│   │   ├── tools/governance.py      #     Validate, discover, build safely
│   │   ├── watchdog_client.py       #     Reads classifications + violations
│   │   ├── guardrails.py            #     Defense-in-depth rules
│   │   └── audit.py                 #     Structured compliance logging
│   └── resources/                   #   App resource definition
│
├── terraform/                       # Infrastructure as Code
│   └── modules/watchdog/            #   Reusable TF module (SP, catalog, grants)
│
├── customer/                        # Worked example (regulatory + business unit classes)
│   ├── ontologies/                  #   Example classes, domains, primitives
│   └── policies/                    #   Example regulatory policies
│
├── template/                        # Blank starting point for new customers
│   ├── ontologies/                  #   Skeleton with commented examples
│   └── policies/
│
├── library/                         # Industry policy packs (future)
│   ├── defense/
│   ├── financial/
│   └── healthcare/
│
└── tests/                           # Unit + integration + E2E tests
    ├── unit/
    └── integration/
```

## Customization Guide

### Adding ontology classes

Classes go in `engine/ontologies/resource_classes.yml` under `derived_classes`. Each class specifies a parent (for policy inheritance) and a classifier (tag-based rules):

```yaml
# Example: classify tables by regulatory domain
HipaaAsset:
  parent: ConfidentialAsset
  description: "Subject to HIPAA regulations"
  classifier:
    tag_equals:
      regulatory_domain: "HIPAA"
```

Classifier operators: `tag_equals`, `tag_in`, `tag_exists`, `tag_matches`, `metadata_equals`, `metadata_matches`, `all_of`, `any_of`, `none_of`.

### Writing policies

Policies go in any `.yml` file under `engine/policies/`. Each policy targets an ontology class (or `"*"` for all resources) and defines a rule:

```yaml
policies:
  - id: POL-HIPAA-001
    name: "HIPAA assets must have a data steward"
    applies_to: HipaaAsset
    domain: RegulatoryCompliance
    severity: critical
    description: "HIPAA data requires a named steward"
    remediation: "Add a 'data_steward' tag"
    active: true
    rule:
      ref: has_data_steward          # reference a reusable primitive
```

Rules can be inline or reference named primitives from `engine/ontologies/rule_primitives.yml`. Inline rules support the same operators as classifiers plus `if_then` conditionals and `metadata_gte` (version-aware comparison).

### Hybrid policy management

Policies have two origins:
- **YAML** (origin=`yaml`) — version-controlled in git, synced to Delta on each deploy
- **User** (origin=`user`) — created directly in the policies Delta table by platform admins

The YAML sync never overwrites user-created policies. Both are merged at evaluation time.

## MCP Server

The MCP server exposes Watchdog governance data as tools for AI assistants. It uses on-behalf-of authentication — each request runs as the calling user's identity, with UC grants on the watchdog schema governing access.

**Tools:**
| Tool | Description |
|------|-------------|
| `get_violations` | Filter violations by status, severity, resource_type, policy, owner |
| `get_governance_summary` | High-level compliance metrics |
| `get_policies` | List all active policies |
| `get_scan_history` | View recent scan results |
| `get_resource_violations` | Full compliance history for a specific resource |
| `get_exceptions` | List approved exceptions |

Deploy as a Databricks App:
```bash
cd mcp
databricks bundle deploy -t nonprod
```

Connect from Claude Code or any MCP client using the app's SSE endpoint: `https://<app-url>/mcp/sse`

## Ontos Integration

The `ontos-adapter/` directory contains a pluggable governance UI module for [Ontos](https://github.com/databrickslabs/ontos) (Databricks Labs governance platform). It follows the **Prometheus/Grafana pattern**:

- **Watchdog** (engine) = Prometheus — crawls resources, evaluates policies, writes violations to Delta tables
- **Ontos adapter** = Grafana data source plugin — reads those tables and exposes a governance UI
- **Delta tables** = the wire protocol — the contract between engine and UI

The adapter defines a `GovernanceProvider` protocol that any backend can implement. The default `WatchdogProvider` reads from `platform.watchdog.*` Delta tables. Ontos consumes it with 3 additive lines in its fork.

See [`ontos-adapter/README.md`](ontos-adapter/README.md) for integration details.

## Testing

```bash
# Unit tests (no Spark required)
cd tests && pip install pytest pyyaml
pytest unit/

# Integration tests (requires Spark session)
pytest integration/
```

## Acknowledgments

Original concept by Ben Sivoravong. Engine implementation by Stuart Gano.
