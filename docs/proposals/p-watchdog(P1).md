# p-watchdog — Data Platform Watchdog

**Date:** 2026-03-13 (updated 2026-04-02)
**Status:** Complete — Phases 1-4 done, ready for review
**Branch:** `proposals/stuart-handoff/p-watchdog`
**Author:** Ben Sivoravong (PRD), Stuart Gano (implementation plan)
**PRD:** `use_cases/data-platform-watchdog.md`
**Dependencies:** None — bundle provisions its own serverless SQL warehouse and compute

## Problem

As the customer's data platform scales, manually verifying that all resources comply with governance policies (tagging, cost attribution, access control, runtime versions) is unscalable. There is no automated mechanism to detect non-compliant resources, track violations over time, or route remediation to the right people.

## Scope

Watchdog is a **config-driven governance scanner** — a daily job that reads YAML policy files, checks your resources, and writes results to Delta tables. You manage policies by editing YAML; you interact with results through MCP tools that plug into any AI assistant.

**What it is:**
1. A YAML file per governance domain (cost, security, quality, regulatory)
2. A scan job that evaluates resources against those policies
3. Delta tables with results, violations, and exceptions
4. MCP tools so any AI assistant can query compliance, explain policies, and manage exceptions

**What it is not:**
- Not a new platform or UI to maintain
- Not a heavyweight ontology system (it uses simple tag-based classification under the hood)
- Not coupled to any specific tool — policies check UC tags, not vendor APIs

Full PRD in `use_cases/data-platform-watchdog.md`.

## Current Status

| Component | Branch | Status | Layer |
|-----------|--------|--------|-------|
| Terraform (SP, catalog, schema, grants) | main | Done | 1 |
| Resource crawler (12 resource types + DQM/LHM) | d-watchdog | Done | 1 / 4 |
| Starter policies (4 inlined, MVP-safe) | d-watchdog | Done | 1 |
| Scan orchestrator + violation MERGE | d-watchdog | Done | 1 |
| AI/BI dashboard v1 (admin view) | d-watchdog | Done | 1 |
| Rule primitives (26 reusable checks) | d-watchdog | Done | 2 |
| Domain policies (34 across 5 domains) | d-watchdog | Done | 2 |
| Hybrid policy management (YAML + Delta) | d-watchdog | Done | 2 |
| Ontology framework (28 resource classes) | d-watchdog | Done | 3 |
| Declarative rule engine (composite rules) | d-watchdog | Done | 3 |
| OWL/Turtle export for Ontos | d-watchdog | Done | 3 |
| DQ policies (DQM/LHM/DQX coverage) | d-watchdog | Done | 4 |
| Dashboard v2/v3 queries (owner + DQ views) | d-watchdog | Done | 4 |
| MCP agents (watchdog, dqx, ai-devkit) | d-watchdog | Done | 5 |
| Dual-path notifications (Delta queue + ACS) | d-watchdog | Done | 6 |
| Exception notebooks (approve/revoke) | d-watchdog | Done | 1 |
| Progressive mode detection (MVP ↔ full) | d-watchdog | Done | 1 |
| Ontos integration | — | Not started (optional) | — |

## Architecture

### What lives where

```
┌─────────────────────────────────────────────────────────┐
│  Terraform (customer-infra-main)                          │
│                                                         │
│  ┌─────────────────────┐  ┌──────────────────────────┐  │
│  │ Service Principal    │  │ platform catalog         │  │
│  │ spn-watchdog         │  │  └─ watchdog schema      │  │
│  │ • workspace read     │  │     ├─ resource_inventory│  │
│  │ • UC read (all cats) │  │     ├─ scan_results      │  │
│  │ • UC write (watchdog)│  │     ├─ violations        │  │
│  └─────────────────────┘  │     ├─ exceptions        │  │
│                            │     ├─ policies          │  │
│                            │     └─ audit_log         │  │
│                            └──────────────────────────┘  │
│  ┌─────────────────────┐                                 │
│  │ UC Grants            │                                │
│  │ • SP → all catalogs  │                                │
│  │   (USE_CATALOG,      │                                │
│  │    USE_SCHEMA, SELECT)│                                │
│  │ • SP → watchdog schema│                                │
│  │   (MODIFY, CREATE_TABLE)                              │
│  └─────────────────────┘                                 │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  DABs (separate repo: watchdog-bundle)                  │
│                                                         │
│  ┌─────────────────────┐  ┌──────────────────────────┐  │
│  │ Python Application   │  │ Policy Definitions       │  │
│  │ • resource_crawler   │  │ • YAML files in repo     │  │
│  │ • policy_engine      │  │ • synced to policies     │  │
│  │ • scan_orchestrator  │  │   Delta table at deploy  │  │
│  │ • notification_svc   │  │                          │  │
│  └─────────────────────┘  └──────────────────────────┘  │
│                                                         │
│  ┌─────────────────────┐  ┌──────────────────────────┐  │
│  │ Workflow             │  │ Dashboard                │  │
│  │ • daily scheduled    │  │ • AI/BI (Lakeview)       │  │
│  │ • ad-hoc trigger     │  │ • role-scoped views      │  │
│  └─────────────────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Data model (watchdog schema)

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `resource_inventory` | All discovered resources per scan | `resource_type`, `resource_id`, `resource_name`, `owner`, `domain`, `tags`, `scan_id`, `discovered_at` |
| `policies` | Policy definitions (YAML + user-created) | `policy_id`, `policy_name`, `applies_to`, `domain`, `severity`, `rule_json`, `origin` (yaml/user), `active` |
| `resource_classifications` | Ontology class assignments per scan | `scan_id`, `resource_id`, `class_name`, `class_ancestors`, `root_class` |
| `scan_results` | Every policy evaluation | `scan_id`, `resource_id`, `policy_id`, `result` (pass/fail/exception), `evaluated_at`, `details` |
| `violations` | Open violations (deduplicated) | `violation_id`, `resource_id`, `policy_id`, `first_detected`, `last_detected`, `status` (open/resolved/exception), `owner`, `notified_at` |
| `exceptions` | Approved policy exceptions | `exception_id`, `violation_id`, `approved_by`, `justification`, `approved_at`, `expires_at` |
| `policies_history` | Append-only audit trail of every policy change | `policy_id`, `version`, `rule_json`, `change_type` (created/updated/deactivated), `origin` (yaml/user), `changed_by`, `changed_at` |
| `audit_log` | All system activity | `event_type`, `event_detail`, `actor`, `timestamp` |

### Relationship to existing infra

| Existing component | How Watchdog uses it |
|-------------------|---------------------|
| SAT (`01-regional-infra/sat.tf`) | **Pattern only** — follow the SP provisioning + secret scope model. SAT covers security posture; Watchdog covers governance/tagging/compliance. Don't merge them. |
| Cluster policies (`modules/cluster_policies`) | Watchdog can verify jobs actually *run on* policy-compliant clusters. Policies enforce at creation; Watchdog audits at runtime. |
| Entra ID groups (`iam.tf`) | Watchdog reads group membership to evaluate RBAC-based policies (e.g., "us-only datasets not accessible by non-US groups") |
| System tables (`system.billing.usage`, etc.) | Watchdog can cross-reference resource inventory with billing data for cost attribution validation |
| Hub workspace (d-h8nqr) | Watchdog runs in hub (central visibility across all spokes). SP needs cross-workspace read access. |

## Implementation Plan

### Phase 1 — Foundation (Terraform + minimal DABs) ✓ Complete

**Terraform changes** (in `customer-infra-main`):

1. **Service Principal** — new resource in `01-regional-infra/` or `02-workspaces/`:
   - `azuread_application` + `azuread_service_principal` for Watchdog
   - `databricks_service_principal` to register in Databricks account
   - Workspace-level group membership (read-only role)
   - Secret scope for SP credentials

2. **Platform catalog + watchdog schema**:
   - Option A: Add `platform` to the catalogs list in tfvars (alongside bronze/silver/gold)
   - Option B: Create a standalone `databricks_catalog` resource for platform tools
   - Either way: `databricks_schema` for `watchdog` within it

3. **UC grants**:
   - SP gets `USE_CATALOG` + `USE_SCHEMA` + `SELECT` on all workspace catalogs (crawl access)
   - SP gets `USE_CATALOG` + `USE_SCHEMA` + `MODIFY` + `CREATE_TABLE` on `platform.watchdog`

**DABs** (new repo):

4. **Skeleton bundle** with one task:
   - Crawl `information_schema` across all catalogs
   - Write `resource_inventory` table
   - Validate: runs in alpha, data appears in SQL editor

**Exit criteria:** `SELECT * FROM platform.watchdog.resource_inventory` returns rows.

### Phase 2 — Policy Engine (DABs only) ✓ Complete

5. **Policy framework**:
   - YAML policy definitions (3-5 starter rules):
     - All tables must have `owner` tag
     - All tables must have `cost_center` tag
     - All jobs must have `cost_center` tag
     - Python jobs must use runtime >= 15.4
     - No tables with PII tag accessible by groups outside `PII_Readers`
   - Python evaluator: load policy → apply to resource metadata → return pass/fail + guidance

6. **Scan orchestrator**:
   - Run all crawlers → run all policies → deduplicate → write results
   - Delta merge for `violations` (upsert — don't duplicate across scans)
   - History tracking: append to `scan_results`, merge to `violations`

7. **Dashboard v1** (AI/BI):
   - Platform Admin view: all violations by severity, resource type, owner
   - Drill-down to individual resource + violation history

**Exit criteria:** Dashboard shows real violations. Admin can identify non-compliant resources and their owners.

### Phase 3 — Notifications + Exceptions (DABs) ✓ Complete

8. **Email notifications** ✓ (dual-path):
   - **Path 1 — Delta queue (always):** `notification_queue` table with CDF enabled. the customer's enterprise email pipeline (Azure Communication Services, internal SMTP, etc.) consumes new rows via streaming or scheduled query. Each row = one owner digest with severity summary, violation IDs, and dashboard URL.
   - **Path 2 — ACS direct (optional):** When `acs_connection_string` + `acs_sender_address` are set in the secret scope, the notify entrypoint sends plain-text digest emails via Azure Communication Services. One email per owner, grouped by severity.
   - Both paths run on every scan. Path 1 is the durable handoff; Path 2 is convenience for platform admins.

9. **Exception workflow** ✓:
   - Parameterized notebooks: `approve_exception.py` (approve with justification + expiry) and `revoke_exception.py` (deactivate)
   - Notebooks use `current_user()` for audit trail — no impersonation possible
   - `exceptions` table with `approved_by`, `justification`, `expires_at`
   - Evaluator checks exceptions before creating new violation alerts (done in Phase 2)
   - MCP `grant_exception` tool provides AI-driven approval path (done in Phase 2)

10. **Dashboard v2** ✓:
    - Compliance summary KPIs (compliance %, open by severity, exception counts)
    - Resource Owner view (violations grouped by owner/domain/severity)
    - Violation detail drilldown (days open, remediation, exception status)
    - Active exceptions panel (with expiring-soon urgency flag)
    - Exception audit trail (active, expired, revoked history)
    - SQL queries in `dashboards/v2_*.sql` — import into AI/BI Lakeview dashboard

**Exit criteria:** Resource owners receive daily digest emails. Exceptions suppress repeat alerts.

## Terraform Backlog Items

| ID | Item | Phase | Status |
|----|------|-------|--------|
| b-wd01 | Provision Watchdog service principal + secret scope | Phase 1 | Done |
| b-wd02 | Create `platform` catalog (or extend existing catalogs) | Phase 1 | Done |
| b-wd03 | Create `watchdog` schema + UC grants for SP | Phase 1 | Done |
| b-wd04 | ~~Notification destination resource~~ → replaced by ACS + Delta queue | Phase 3 | N/A |

## DABs Backlog Items

| ID | Item | Phase | Status |
|----|------|-------|--------|
| b-wd05 | Resource crawler (information_schema + REST APIs) | Phase 1 | Done |
| b-wd06 | Policy YAML framework + evaluator | Phase 2 | Done |
| b-wd07 | Scan orchestrator + dedup logic | Phase 2 | Done |
| b-wd08 | AI/BI dashboard v1 (admin view) | Phase 2 | Done |
| b-wd09 | Notification service (Delta queue + ACS email) | Phase 3 | Done (d-watchdog) |
| b-wd10 | Exception workflow (notebooks + MCP tool) | Phase 3 | Done (d-watchdog) |
| b-wd11 | Dashboard v2 (role-scoped views + exceptions) | Phase 3 | Done (d-watchdog) |
| b-wd16 | Deploy watchdog-mcp agent (Databricks App) | Phase 2 | Done (d-watchdog) |
| b-wd17 | ~~Deploy dqx-mcp agent~~ | — | Future — not yet built |
| b-wd18 | ~~Deploy ai-devkit-mcp agent~~ | — | Moved to d-ai-devkit (governance extension, not standalone app) |
| b-wd19 | Register watchdog-mcp as UC HTTP Connection | Phase 2 | Done (d-watchdog) |
| b-wd20 | Declarative rule engine + ontology framework (32 policies) | Phase 2.5 | Done (d-watchdog) |
| b-wd21 | Hybrid policy management (origin='yaml' / 'user') | Phase 2.5 | Done (d-watchdog) |
| b-wd22 | DQM system table crawler + LHM detection | Phase 4 | Done (d-watchdog) |
| b-wd23 | DQM schema enablement policy (POL-Q008) | Phase 4 | Done (d-watchdog) |
| b-wd24 | Lakehouse Monitoring enforcement policy (POL-Q009) | Phase 4 | Done (d-watchdog) |
| b-wd25 | Dashboard v3 (DQ coverage, anomalies, summary) | Phase 4 | Done (d-watchdog) |

## Design Decisions

| Question | Options | Recommendation |
|----------|---------|----------------|
| Where does Watchdog code live? | Same repo vs separate | **Separate repo** — independent release cadence from infra |
| Catalog placement | `gold.watchdog` vs new `platform` catalog | **`platform` catalog** — Watchdog is tooling, not business data |
| Which workspace runs Watchdog? | Each spoke vs hub only | **Hub** — central visibility, SP needs cross-workspace access |
| Compute for crawler job | Job cluster vs serverless | **Job cluster** (Python SDK calls); dedicated serverless SQL warehouse for dashboard queries (provisioned by bundle) |
| Policy storage | YAML only vs YAML + Delta vs Delta-first | **Hybrid** — YAML seeds SA-managed baseline (origin='yaml'), users create/tune policies directly in Delta (origin='user'). YAML sync never overwrites user policies. Append-only `policies_history` table tracks every change from both sources. |
| SP scope | Account admin vs minimal | **Minimal** — workspace read + UC grants. Account admin is overpowered. |
| Email delivery | Notification destinations vs ACS vs enterprise handoff | **Dual-path** — Delta `notification_queue` (always) for enterprise email pipeline + Azure Communication Services (optional) for immediate admin alerts. Databricks notification destinations not used. |

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| SP over-provisioned (security) | Watchdog SP could access/modify data if misconfigured | Minimal grants: read-only on all catalogs, write only to watchdog schema |
| Policy engine complexity | "Very complex logic" per PRD could make policies hard to maintain | Start with simple tag-check policies. Complex policies (RBAC cross-referencing) in Phase 2+ |
| Cross-workspace crawling | Hub SP needs to see spoke resources | Requires account-level SP or per-workspace registration. Design in Phase 1. |
| Email deliverability | Databricks notification destinations may have limits | Test with small group in alpha. Fallback: Azure Communication Services |
| Dashboard performance | Large scan_results table at scale | Partition by `scan_id` (date). Aggregate views for dashboard. |

## Estimated Effort

| Phase | Effort | Dependencies | Status |
|-------|--------|--------------|--------|
| Phase 1 (Foundation) | 3-4 days | None | Done |
| Phase 2 (Policy Engine) | 5-7 days | Phase 1 complete | Done |
| Phase 2.5 (Classification + Rule Engine) | 3-4 days | Phase 2 complete | Done (d-watchdog) |
| Phase 3 (Notifications + Exceptions UI) | 3-4 days | Phase 2 complete | Done (d-watchdog) |
| Phase 4 (DQ Integration) | 2-3 days | DQM enabled on workspace, gold-tier tables exist | Done (d-watchdog) |

**Remaining:** Merge d-watchdog branch to main. All phases complete.

## MCP Agent Layer

Watchdog exposes governance capabilities through a dedicated MCP agent deployed as a Databricks App. Any AI assistant (Databricks AI, VS Code Copilot, Claude, etc.) can query compliance in natural language.

### Agent Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  UC HTTP Connections (Agent Discovery)                        │
│  Supervisor Agent routes to the right MCP server             │
└──────┬──────────────────────────────────────────────────────┘
       │
┌──────▼──────┐
│ watchdog-mcp│    Complementary agents (separate branches):
│             │    • AI DevKit governance extension (d-ai-devkit)
│ get_violations    │      — developer-facing guardrails at query time
│ get_governance_summary  • DQM/DQX agent — future, not yet built
│ get_policies    │
│ get_scan_history│
│ get_resource_violations│
│ get_exceptions  │
└─────────────┘
  Governance
```

### Agent Details

| Agent | Location | Purpose | Tools |
|-------|----------|---------|-------|
| **watchdog-mcp** | `databricks/watchdog-mcp/` (this branch) | Governance policy compliance | get_violations, get_governance_summary, get_policies, get_scan_history, get_resource_violations, get_exceptions |

**Related agents (separate proposals):**
- **AI DevKit governance extension** — developer-facing guardrails (validate_ai_query, safe_columns, etc.). See `p-ai-devkit` branch. Extends the official AI Dev Kit MCP server, not a standalone app.
- **DQX agent** — row-level data quality (DQM/DQX). Future work, not yet built.

### Deployment

The watchdog-mcp agent is a standalone Databricks App deployed via DABs:

```bash
databricks apps deploy watchdog-mcp --source-code-path ./databricks/watchdog-mcp
```

### UC HTTP Connection Registration

Register the agent as a UC HTTP Connection so Supervisor Agents can discover and route to it:

```sql
CREATE CONNECTION watchdog_mcp TYPE HTTP
  URL 'https://<app-url>/api/mcp'
  OPTIONS (scope = 'governance');
```

### Example Interactions

```
User: "Are there any critical violations for the dosimetry team?"
→ watchdog-mcp.get_violations(domain="dosimetry", severity="critical")

User: "Why does POL-R002 exist and how do I fix it?"
→ watchdog-mcp.get_policies(policy_id="POL-R002")

User: "What's our overall compliance posture?"
→ watchdog-mcp.get_governance_summary()

User: "Show me exceptions that are about to expire"
→ watchdog-mcp.get_exceptions(status="expiring")
```

## Resource Classification Engine (Phase 2.5)

Under the hood, Watchdog uses tag-based resource classification to determine which policies apply to which resources. This is not a separate system to manage — it's built into the scan job and driven entirely by the same YAML config.

### How It Works

A policy says "this rule applies to GoldTable resources." The classifier looks at a table's UC tags (`data_layer=gold`) and determines it's a GoldTable. That's the whole mechanism — tag matching, not a separate ontology system.

```yaml
# Example: one policy, one rule, tag-driven targeting
- policy_id: POL-Q001
  name: Gold tables must have comments
  applies_to: GoldTable          # ← classifier matches data_layer=gold
  severity: medium
  rule:
    type: has_comment
```

### File Layout

```
ontologies/                       # Resource classification config
├── resource_classes.yml          # 28 classes (tag-based matching rules)
├── compliance_domains.yml        # 6 governance domains with owners
└── rule_primitives.yml           # 26 reusable rule building blocks

policies/                         # One YAML per governance domain
├── cost_governance.yml           # 7 policies (POL-C001–C007)
├── security_governance.yml       # 5 policies (POL-S001–S005)
├── data_quality.yml              # 7 policies (POL-Q001–Q007)
├── operational.yml               # 5 policies (POL-O001–O005)
└── regulatory.yml                # 8 policies (POL-R001–R008)

src/watchdog/                     # ~500 lines of Python
├── ontology.py                   # Tag-based classifier
├── rule_engine.py                # Composite rule evaluator
├── policy_engine.py              # Classify → evaluate pipeline
├── policy_loader.py              # YAML loader
└── violations.py                 # Delta table writer
```

### Resource Classes (tag-based)

```
DataAsset (table, volume, catalog, schema)
├── PiiAsset                  tag: data_classification=pii
│   └── PiiTable              tag: data_classification=pii + resource_type=table
├── ConfidentialAsset         tag: data_classification IN [confidential,restricted,pii]
│   ├── ExportControlledAsset tag: export_classification IN [ITAR,EAR]
│   │   ├── ItarAsset         tag: export_classification=ITAR
│   │   └── EarAsset          tag: export_classification=EAR
│   ├── NrcRegulatedAsset     tag: regulatory_domain=NRC
│   └── DoeRegulatedAsset     tag: regulatory_domain=DOE
├── DosimetryAsset            tag: business_unit=dosimetry
├── DetectionAsset            tag: business_unit=detection
├── MedicalAsset              tag: business_unit=medical
├── GoldTable / SilverTable / BronzeTable   tag: data_layer=gold/silver/bronze

ComputeAsset (job, cluster, warehouse, pipeline)
├── ProductionJob             tag: environment=prod + resource_type=job
│   └── CriticalJob           tag: environment=prod + criticality=high
├── ProductionPipeline        tag: environment=prod + resource_type=pipeline
├── DevelopmentCompute        tag: environment IN [dev,sandbox,test]
├── InteractiveCluster        resource_type=cluster + NOT cluster_type=job
├── UnattributedAsset         NOT tag: cost_center
└── SharedCompute             tag: shared=true
```

### How the Scan Works

1. **Classify**: Look at a resource's tags → determine what classes it belongs to (e.g., `data_layer=gold` → GoldTable)
2. **Evaluate**: Each policy targets a class → if the resource matches, run the rule
3. **Write**: Results go to Delta tables — `scan_results`, `violations`, `resource_classifications`

### Policy Count: 32 across 5 domains

| Domain | Policies | Coverage |
|--------|----------|----------|
| CostGovernance | 7 | Ownership, cost_center, BU, environment, auto-termination |
| SecurityGovernance | 5 | Data classification, PII stewardship, runtime compliance |
| DataQuality | 7 | Comments, data layer, gold table stewardship, DQX integration |
| OperationalGovernance | 5 | Alerting, SLA tiers, runtime, team attribution |
| RegulatoryCompliance | 8 | ITAR/EAR/NRC/DOE tagging, export classification, retention |

### customer-specific Regulatory Policies

- **POL-R001**: Export-controlled assets must have data_steward + regulatory_domain + retention_days
- **POL-R002/R003**: ITAR/EAR assets must have valid export_classification
- **POL-R004/R005**: NRC/DOE assets must have steward + retention
- **POL-R006**: All confidential assets must declare export classification (NONE/EAR/ITAR)
- **POL-R007**: All data assets must have data_classification tag
- **POL-R008**: PII assets must have retention policy

## Optional: Ontos Integration

> **This is an optional add-on.** Watchdog works standalone with MCP tools and AI/BI dashboards. Ontos adds a dedicated governance UI if you want one later.

Watchdog is the **scan engine**, [Ontos](https://github.com/databrickslabs/ontos) (Databricks Labs) is an optional **governance platform UI**.

### Architecture

```
┌──────────────────────────────────────────────┐
│  Ontos (Databricks App)                      │
│  • React UI for compliance dashboards        │
│  • Semantic models (knowledge graph)         │
│  • Data product / contract management        │
│  • Compliance DSL authoring + execution      │
│  • Review workflows with audit trail         │
│  • MCP server for AI assistant access        │
│                                              │
│  Reads from: platform.watchdog.*             │
│  Imports: watchdog-ontology.ttl              │
└───────────────────┬──────────────────────────┘
                    │
     violations, classifications, scan_results
                    │
┌───────────────────▼──────────────────────────┐
│  Watchdog (DABs Workflow Job)                │
│  • Resource crawler (UC + workspace)         │
│  • Ontology classifier (tag-based)           │
│  • Rule engine (composite, declarative)      │
│  • Violation lifecycle (MERGE + exceptions)  │
│  • Runs daily on schedule                    │
│                                              │
│  Writes to: platform.watchdog.*              │
│  Exports: watchdog-ontology.ttl for Ontos    │
└───────────────────┬──────────────────────────┘
                    │
┌───────────────────▼──────────────────────────┐
│  Unity Catalog (Source of Truth)             │
│  Tags, metadata, resource_inventory          │
└──────────────────────────────────────────────┘
```

### Integration Points

| Watchdog Output | Ontos Consumption |
|----------------|-------------------|
| `resource_classifications` table | Semantic linking: map Watchdog classes to Ontos concepts |
| `violations` table (with domain, resource_classes) | Compliance dashboards grouped by domain |
| `watchdog-ontology.ttl` | Import as knowledge collection via simple_owl handler |
| `policies` table (with rule_json, applies_to) | Policy browsing + Compliance DSL authoring |
| `scan_results` (with resource_classes) | Historical compliance scoring per Ontos entity |

### Delta Tables (Ontos-Compatible Schema)

| Table | New Columns (for Ontos) |
|-------|------------------------|
| `violations` | `domain`, `resource_classes` |
| `scan_results` | `domain`, `severity`, `resource_classes` |
| `resource_classifications` | **New table**: `scan_id`, `resource_id`, `class_name`, `class_ancestors`, `root_class` |
| `policies` | `applies_to`, `domain`, `rule_json` |

### Deployment Recommendation

1. Deploy **Watchdog** first (DABs workflow job — no UI dependency)
2. Deploy **Ontos** as a Databricks App (React + FastAPI + Lakebase)
3. Import `watchdog-ontology.ttl` into Ontos as a knowledge collection
4. Configure Ontos to read from `platform.watchdog.*` tables
5. Ontos Compliance DSL policies can complement Watchdog's scheduled scans with on-demand checks

### Backlog Additions

| ID | Item | Phase | Status |
|----|------|-------|--------|
| b-wd26 | Deploy Ontos as Databricks App | Phase 3+ | — |
| b-wd27 | Import Watchdog ontology into Ontos semantic models | Phase 3+ | — |
| b-wd28 | Configure Ontos to read Watchdog Delta tables | Phase 3+ | — |
| b-wd29 | Port Watchdog YAML policies to Ontos Compliance DSL (optional) | Phase 3+ | — |

## How It All Fits Together

Watchdog handles **metadata governance** (tags, ownership, compliance). It bridges to **row-level data quality** (DQM/DQX) through UC tags — and optionally feeds a **governance UI** (Ontos) if you want one later.

```
                MCP Tools (AI Assistant Access)
                ┌──────────┬──────────┬──────────┐
                │watchdog  │ dqx-mcp  │ai-devkit │
                │-mcp      │          │-mcp      │
                └────┬─────┴────┬─────┴────┬─────┘
                     │          │          │
┌────────────────────▼──────────▼──────────▼─────────────────┐
│  Watchdog (DABs Scan Job)          │  DQM / DQX            │
│  ─────────────────────────         │  ──────────            │
│  • Metadata governance             │  • Row-level quality   │
│  • 32 YAML policies                │  • Nulls, ranges, etc. │
│  • Tag-based classification        │  • Stamps UC tags      │
│  • Writes: violations, scan_results│  • dqx_quality_score   │
│                                    │                        │
│  Reads DQM/DQX tags ◄─────────────┤  Writes UC tags        │
└────────────────────────────────────┴────────────────────────┘
                     │
              ┌──────▼──────┐
              │ Optional:   │
              │ Ontos UI    │
              │ (Phase 3+)  │
              └─────────────┘
```

### Division of Responsibility

| Concern | Owner | Example |
|---------|-------|---------|
| Tags, ownership, cost attribution | **Watchdog** | "All gold tables must have an `owner` tag" |
| Row-level data content | **DQM / DQX** | "No nulls in `customer_id` column" |
| Governance UI + dashboards | **AI/BI Dashboard** (default) or **Ontos** (optional) | Compliance views by domain/owner |
| AI assistant access | **MCP agents** | "Show critical violations for my team" |

### The UC Tag Contract

Watchdog and DQM/DQX communicate through UC tags — no API coupling:

| Tag | Written By | Read By | Purpose |
|-----|-----------|---------|---------|
| `dqx_enabled` | DQM/DQX pipeline | Watchdog (POL-Q006) | Confirm quality checks are configured |
| `dqx_quality_score` | DQM/DQX pipeline | Watchdog (POL-Q007) | Verify score meets threshold |
| `owner`, `cost_center`, etc. | Platform admin / CI | Watchdog | Metadata governance policies |

This is **tool-agnostic** — whether the quality pipeline is DQM, DQX, or a custom notebook, it just needs to stamp the tags.

### Row-Level Quality Integration (DQM / DQX)

> **DQM** (Data Quality Management) is the supported Databricks product surface for row-level quality — health indicators, quality system tables, and eventually a native DQX-style API. **DQX** (databrickslabs/dqx) is the Labs/OSS option with the most advanced capabilities today. The two are intentionally converging; use DQM where you need supported product surface, DQX where you need advanced capabilities and can tolerate Labs trade-offs.

Two policies bridge Watchdog and the quality layer (in `policies/data_quality.yml`):

- **POL-Q006** (high): Gold tables must have `dqx_enabled=true` — ensures row-level checks exist
- **POL-Q007** (medium): Gold tables with quality checks enabled must maintain `dqx_quality_score >= 95`

These policies are **tag-based, not tool-coupled** — they check UC tags, not DQX or DQM directly. Whether the quality pipeline is DQM, DQX, or a custom notebook, it just needs to stamp the tags. This makes the integration future-proof as DQX converges into DQM.

## Hybrid Policy Management

Policies have two origins — YAML (SA-managed) and Delta (user-managed). The policy engine evaluates both at scan time.

| Origin | Source of Truth | Who Edits | Lifecycle |
|--------|----------------|-----------|-----------|
| `yaml` | YAML files in `policies/` dir, version-controlled in git | SA via PR | Synced to Delta on each deploy. Removed YAML → auto-deactivated. |
| `user` | `platform.watchdog.policies` Delta table directly | the customer platform admins via SQL/notebook | Never touched by YAML sync. Full admin control. |

**How it works:**
- `sync_policies_to_delta()` MERGEs YAML policies with `WHEN MATCHED AND target.origin = 'yaml'` — user rows are skipped
- Deactivation of removed YAML policies filters `WHERE origin = 'yaml'` — user policies are never deactivated by a deploy
- `load_delta_policies()` reads `WHERE origin = 'user' AND active = true` and combines with YAML policies before evaluation

**Creating a user policy:**
```sql
INSERT INTO platform.watchdog.policies
  (policy_id, policy_name, applies_to, domain, severity,
   description, remediation, rule_json, origin, active, updated_at)
VALUES
  ('POL-U001', 'SAP tables must have refresh_sla tag', 'DataAsset',
   'DataQuality', 'high',
   'SAP-sourced tables need a refresh SLA for monitoring',
   'Add a refresh_sla tag with the expected cadence (daily, hourly, etc.)',
   '{"tag_exists": {"tag": "refresh_sla"}}',
   'user', true, current_timestamp())
```

This gives the customer a "database way" to manage policies while the SA baseline stays in git.

### Policy Change History

Both YAML deploys and user edits write to an append-only `policies_history` table. Every mutation — creation, rule change, severity change, deactivation — is captured with a version number and timestamp.

```
policies_history (append-only)
├── policy_id       — Which policy changed
├── version         — Auto-incrementing version per policy
├── rule_json       — The rule definition at this version
├── severity        — Severity at this version
├── applies_to      — Target class at this version
├── active          — Whether the policy was active at this version
├── origin          — yaml or user
├── change_type     — created, updated, or deactivated
├── changed_by      — Who made the change (null for YAML deploys)
├── changed_at      — When the change was recorded
```

**Change detection** compares `rule_json`, `severity`, `applies_to`, and `active` against the current `policies` table. Cosmetic edits to description or remediation text don't generate history rows — only behavioral changes that affect evaluation results.

**YAML policies** get history rows written during `sync_policies_to_delta()` — before the MERGE updates current state. This means the history table captures what changed on each deploy, even though the YAML source of truth is git.

**User policies** get history rows when admins call `_record_policy_changes()` from a notebook or the MCP `grant_exception` tool. (The helper function is the same one the YAML sync uses.)

**Example queries:**
```sql
-- What changed in the last 7 days?
SELECT policy_id, version, change_type, changed_at, origin
FROM platform.watchdog.policies_history
WHERE changed_at > current_timestamp() - INTERVAL 7 DAYS
ORDER BY changed_at DESC

-- Full history for a specific policy
SELECT version, severity, applies_to, active, change_type, changed_at
FROM platform.watchdog.policies_history
WHERE policy_id = 'POL-C002'
ORDER BY version

-- Who's been creating user policies?
SELECT policy_id, changed_by, changed_at
FROM platform.watchdog.policies_history
WHERE origin = 'user' AND change_type = 'created'
ORDER BY changed_at DESC
```

## Deployment Model — Progressive Layers

Watchdog is designed to deploy incrementally. Each layer adds capability without reworking what's already running. V4C can deploy Layer 1 on day one and add layers as the platform matures.

### Why this matters

The meeting aligned on an MVP: starter policies, notebook-based exceptions, admin-only dashboard, single workspace. The codebase includes significantly more (ontology, 34 policies, MCP, DQM/LHM). Rather than forking an "MVP branch," the system detects what's present and adapts. Deploy the minimum files → get MVP behavior. Add more files → get full behavior. No config flags, no feature toggles, no rework.

### Layer 1: MVP (deploy immediately)

**What to deploy:** `watchdog-bundle/` with only these policy files:
- `policies/starter_policies.yml` — 4 inlined rules (owner, comment, cost_center)

**What to remove (or not deploy):** Everything in `ontologies/` and the domain policy files (`cost_governance.yml`, `security_governance.yml`, etc.) can be excluded from the initial bundle. The system detects their absence and logs:

```
Watchdog: MVP mode — missing resource_classes.yml, rule_primitives.yml.
Using resource_type fallback. 4 policies (4 YAML + 0 user)
```

**How it works without ontology:**
- `_policy_applies` uses a static `_CLASS_TYPE_FALLBACK` map: `DataAsset` → `{table, volume, catalog, schema}`, `ComputeAsset` → `{job, cluster, warehouse, pipeline}`. No classification pass runs.
- Starter policy rules are inlined (not `ref:` references), so they evaluate without `rule_primitives.yml`.
- Crawler, violation MERGE, exception handling, dashboard v1 all work unchanged.

**What you get:** Daily scan → 4 policies evaluated → violations table → admin dashboard. The exact scope the meeting described.

### Layer 2: Rule primitives (add when ready for domain policies)

**What to add:** `ontologies/rule_primitives.yml`

**What changes:** Policies that use `ref: has_owner`, `ref: has_cost_center`, etc. now resolve. You can deploy the domain policy files:
- `policies/cost_governance.yml` — 7 cost attribution policies
- `policies/operational.yml` — 5 runtime/operational policies
- Add one file at a time. Each is independent.

**System logs:**
```
Watchdog: MVP mode — missing resource_classes.yml.
Using resource_type fallback. 16 policies (16 YAML + 0 user)
```

Policies with `applies_to: GoldTable` or other derived classes still use the resource_type fallback (GoldTable → table). They'll apply to ALL tables, not just gold-tagged ones. This is conservative — no gold table escapes the check.

### Layer 3: Ontology (add when tag-based classification matters)

**What to add:** `ontologies/resource_classes.yml`

**What changes:** The ontology engine classifies resources by tags. A table tagged `data_layer=gold` becomes a `GoldTable`; a table tagged `data_classification=pii` becomes a `PiiTable`. Policies with `applies_to: GoldTable` now correctly skip non-gold tables. System logs:

```
Watchdog: full mode — ontology (28 classes), rule engine (26 primitives),
34 policies (34 YAML + 0 user)
```

**Why wait:** Classification only matters when tables have tags. Until the team standardizes on `data_layer`, `data_classification`, etc., the fallback (apply to all tables of the right type) is actually safer.

### Layer 4: DQM/LHM integration (add when DQM is enabled)

**What to add:** Deploy `data_quality.yml` policies (POL-Q006 through POL-Q009). The crawler already includes `_crawl_dqm_status` and `_crawl_lhm_status` — they silently return empty results when system tables aren't available.

**What changes:** Once DQM is enabled on a workspace, the crawler starts enriching `resource_inventory` tags with `dqm_enabled`, `lhm_enabled`, `dqm_anomalies`. The DQ policies then evaluate against those tags.

### Layer 5: MCP agents (add when AI consumption is ready)

**What to deploy:** `watchdog-mcp/` and optionally `ai-devkit-mcp/` as separate DABs bundles. These are independent apps — they read from the same Delta tables but have no coupling to the scan job.

### Layer 6: Notifications (add when email decision is made)

**What to configure:** Set `acs_connection_string` and `acs_sender_address` in the secret scope to enable ACS emails. The `notification_queue` Delta table always gets written regardless — enterprise email pipelines can consume it via CDF.

### Summary table

| Layer | Files Added | Prerequisite | What You Get |
|-------|------------|--------------|--------------|
| 1. MVP | `starter_policies.yml` only | Workspace + SP | 4 policies, daily scan, admin dashboard |
| 2. Primitives | `rule_primitives.yml` + domain YAMLs | Layer 1 | 16-34 policies across 5 domains |
| 3. Ontology | `resource_classes.yml` | Layer 2 + consistent tagging | Tag-based classification, precise policy targeting |
| 4. DQM/LHM | `data_quality.yml` policies | DQM enabled on workspace | Freshness/completeness monitoring, anomaly detection |
| 5. MCP | `watchdog-mcp/` bundle | Layer 1 + Databricks Apps | AI-queryable governance |
| 6. Notifications | Secret scope config | Layer 1 + ACS or email pipeline | Owner digests via email |

Each layer is additive. No layer requires rework of a previous layer.

## Remaining Work

All code is built on `proposals/stuart-handoff/p-watchdog`. What remains is deployment and customer-side decisions:

| Item | Status | Depends On |
|------|--------|-----------|
| Merge `proposals/stuart-handoff/p-watchdog` to main | Ready | SA review |
| Deploy Layer 1 (MVP) to beta workspace | Ready to deploy | Beta workspace created (W2), SP permissions confirmed |
| Russell: ACS vs internal mail server decision | Pending Russell Pierce | — |
| Ontos integration (optional) | Not started — exploratory | Watchdog deployed to production |

## PRD Clarifying Questions — Resolved

From Ben's PRD, the following questions are now answered by implementation:

| # | Question | Resolution |
|---|----------|------------|
| 1 | Email Service | **Dual-path** — Delta `notification_queue` always written (enterprise pipeline handoff); ACS emails optional when secret scope configured. Russell Pierce to finalize primary path. |
| 2 | Policy Management | **Hybrid** — YAML seeds baseline (git-controlled), users extend via Delta table (SQL/notebook) |
| 3 | Exception Workflow | **SQL INSERT into exceptions table** with `approved_by`, `justification`, `expires_at`. Evaluator auto-checks exceptions. |
| 4 | Dashboard Strategy | **Single dashboard with role-scoped views** (data model supports filtering by owner/domain) |
| 5 | Job Ownership | **Data Platform Admin** is operator; ad-hoc scan available to admins via `watchdog_adhoc_scan` job |
| 6 | Host Workspace & Catalog | **Hub workspace (d-h8nqr)** + **`platform` catalog** with `watchdog` schema |

## Relationship to V4C Implementation

V4C (implementation partner) has deployed a Layer 1 scanner to the alpha workspace
(`adb-2098490958867582`, catalog `customer_dev.watchdog`) based on the Databricks SA design.
The core engine — crawl, evaluate, MERGE — matches this proposal's architecture.

**What V4C has that this proposal also has:** crawler, rule engine, ontology, violations
MERGE, exception manager, notification queue.

**What this proposal adds:** YAML-per-domain policy files (audit trail in git), Ontos
semantic sync (`p-ontos` branch), MCP server (Layer 3), multi-environment targets.

**Deploy-alongside pattern:** Layers 2 and 3 from this proposal can deploy against V4C's
existing Delta tables by pointing `catalog: customer_dev` and `schema: watchdog` in the
bundle targets. No changes to V4C's scanner required. See `docs/watchdog-platform.md`
for the deployment sequence and the full three-layer architecture.

**Known V4C scaling risk:** Policy evaluation is driver-side O(N×M). Acceptable on alpha;
will degrade at live workspace scale. Flag to V4C before live deployment.

---

## Related

- `use_cases/data-platform-watchdog.md` — Full PRD (Ben Sivoravong)
- `databricks/mcp/watchdog/` — Watchdog governance MCP agent
- `databricks/mcp/dqx/` — Data quality MCP agent (DQM/DQX)
- `databricks/mcp/ai-devkit/` — Platform operations MCP agent
- `databricks/01-regional-infra/sat.tf` — SAT SP pattern to follow
- `docs/operating-model.md` — Deployment model (alpha → beta → prod)
- d-h8nqr — Hub as central data platform (Watchdog can run here or in any spoke)
- Bundle provisions its own serverless SQL warehouse — no external dependency on D5
- [Ontos (Databricks Labs)](https://github.com/databrickslabs/ontos) — Optional governance UI (Phase 3+)
- `ontologies/export/watchdog-ontology.ttl` — OWL/Turtle for Ontos import
- [DQM](https://docs.databricks.com/en/data-quality-management/) — Databricks Data Quality Management (supported product)
- [DQX (Databricks Labs)](https://github.com/databrickslabs/dqx) — Advanced row-level quality (Labs/OSS, converging into DQM)
- [`dbx-agent-app`](https://github.com/databrickslabs/sandbox) — `@app_agent` decorator for MCP agents (vendored as `databricks/mcp/dbx_agent_app.py`)
