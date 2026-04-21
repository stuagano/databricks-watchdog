# Watchdog Roadmap

> What Watchdog is, what it isn't, and what's built.
>
> Last updated: 2026-04-21

## Identity

Watchdog is a **compliance posture evaluator** for Databricks Unity Catalog — and increasingly, for **AI agents running on the platform**.

The platform enforces governance at query time (ABAC masks a column, a tag policy rejects an invalid value). Watchdog answers the question nobody else answers: **"across all my policies, how compliant is my estate right now, who owns the gaps, and is it getting better or worse?"**

### What Watchdog does

- **Crawls** workspace resources daily (12+ types via SDK + information_schema)
- **Classifies** resources through an ontology hierarchy (tag-based, with inheritance)
- **Evaluates** declarative policies against classified resources (composable rules: `all_of`, `any_of`, `if_then`)
- **Tracks violations** with a lifecycle (open → resolved/exception), deduplication, and owner attribution
- **Notifies** resource owners with per-owner digests and remediation guidance
- **Exposes** compliance posture to AI assistants via MCP server (13 tools)
- **Feeds downstream consumers** — Ontos (business catalog), Guardrails (AI build-time enforcement), Governance Hub (native UI), Lakeview dashboards
- **Ships** industry policy packs (healthcare, financial, defense) as reusable YAML

### What Watchdog does NOT do

- **Enforce access control** — that's ABAC, governed tags, row filters, column masks (native platform). Watchdog can *detect drift* from a declared expected state (see Drift Detection below), but never creates or revokes grants.
- **Manage tags or grants** — that's the Governance Hub UI
- **Auto-classify PII** — that's [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) (GA)
- **Auto-generate documentation** — that's [AI-Generated Documentation](https://www.databricks.com/blog/announcing-public-preview-ai-generated-documentation-databricks-unity-catalog) (PuPr)
- **Rate-limit or filter PII at the gateway** — that's [AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/overview-serving-endpoints) (GA)
- **Create or manage DQ monitors** — that's Lakehouse Monitoring (PuPr)
- **Provide a native workspace UI** — that's the Governance Hub (GA)
- **Model business semantics** — that's Ontos (domains, data contracts, ODCS)
- **Handle bulk tag/grant operations** — that's the Governance Hub
- **Manage access requests (RFA)** — that's the Governance Hub

### The analogy

The platform is the **immune system** — it blocks bad things at runtime.
Watchdog is the **annual physical** — it measures overall health, tracks trends, and tells you what to fix.
Ontos is the **org chart** — it models business structure, ownership, and domain semantics.
Guardrails is the **safety briefing** — it tells AI agents what's safe to use before they start building.

---

## Three-Layer Architecture

Watchdog sits at the center of a three-layer stack. Each layer serves a different persona and none replaces the other:

```
┌─────────────────────────────────────────────────────────────┐
│  Ontos (Business Catalog / Data Products)                    │
│  Persona: data product owners, domain leads                  │
│  ODCS, Data Mesh, domains, data contracts, business glossary │
│  Reads Watchdog: classification data for governance views    │
│  NOT replaced by Governance Hub — Hub does admin metadata,   │
│  Ontos does business semantics the Hub won't attempt         │
└──────────────────────────┬──────────────────────────────────┘
                           │ reads classifications + violations
┌──────────────────────────▼──────────────────────────────────┐
│  Watchdog Engine (Compliance Posture)                        │
│  Persona: CDOs, platform admins, governance teams            │
│  Ontology + rules + violations + lifecycle                   │
│  NOT replaced by Governance Hub — Hub does dashboards +      │
│  management, Watchdog does cross-domain policy evaluation    │
└────────┬─────────────────────────────────┬──────────────────┘
         │ feeds                           │ feeds
┌────────▼────────────┐    ┌───────────────▼──────────────────┐
│  Governance Hub      │    │  Guardrails / AI DevKit MCP      │
│  (native UI)         │    │  Persona: AI agent developers    │
│  reads Delta tables  │    │  9 MCP tools for agents building │
│  for dashboards      │    │  on the lakehouse — inherits     │
│                      │    │  Watchdog classifications +      │
│  Lakeview Dashboards │    │  violations to enforce governance│
│  Genie Space         │    │  at build time                   │
│  Watchdog MCP Server │    │  "Is this table safe to use?"    │
└──────────────────────┘    └──────────────────────────────────┘
```

### Why three layers, not one

The Governance Hub [Labs Proposal for Ontos](https://docs.google.com/document/d/1WXcjpwKXnUifODy65Mvp0J2n1GQRP4bhSIaSGejAF2k) makes this explicit:

> *"While we are adding some rudimentary features to our platform now (Databricks One, Discover Page, Governance Hub, Certification, Domains, Metrics), they are not sufficient for enterprise users to model their business efficiently."*

The Hub is an **admin management plane**. Ontos is a **business catalog**. Watchdog is a **compliance evaluator**. They share Delta tables, not responsibilities.

---

## Platform Landscape (Updated Q3 FY27)

What's shipping natively and what it means for Watchdog scope:

| Native Capability | Status | Watchdog Implication |
|---|---|---|
| Governed Tags + Tag Policies | **GA** | Don't build tag management. Evaluate tag compliance. |
| ABAC (row filters, column masks) | **GA** | Don't build ABAC creation. Evaluate ABAC coverage. |
| [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) | **GA** | Don't build PII detection. Evaluate classification coverage. |
| Detect → Tag → Mask pipeline | **GA** | Don't replicate. Evaluate whether the pipeline ran and covers what it should. |
| [AI-Generated Documentation](https://www.databricks.com/blog/announcing-public-preview-ai-generated-documentation-databricks-unity-catalog) | **PuPr** | Don't build DocAgent. Platform auto-generates table/column descriptions. |
| Data Quality Monitoring | **PuPr** | Don't build monitors. Evaluate DQM coverage + quality score thresholds. |
| Tag Propagation through lineage | **In progress** (EDC-913) | Don't build. Evaluate propagation completeness once available. |
| Governance Hub (unified UI) | **GA** | Don't build UI. Feed Delta tables that the Hub (or Lakeview dashboards) can consume. |
| [AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/overview-serving-endpoints) | **GA** | Don't build rate limiting or PII filtering. Add policy-based governance on top (ontology, violations, risk scoring). |
| [AI Gateway inference tables](https://docs.databricks.com/aws/en/ai-gateway/inference-tables) | **GA** | Future: read inference tables for richer agent execution data alongside `endpoint_usage`. |
| [OpenTelemetry telemetry](https://docs.databricks.com/aws/en/release-notes/product/2026/march) | **GA** (Mar 2026) | Future: read OTel traces for deeper agent compliance monitoring. |
| [AI Governance Framework (DAGF)](https://www.databricks.com/blog/introducing-databricks-ai-governance-framework) | **Published** | Framework is guidance. Watchdog is programmatic execution of the monitoring/compliance pillar. |

---

## What's Built

All capabilities below are implemented and operational.

### Core Engine
- **16+ crawlers** — tables, views, volumes, models, functions, grants, service principals, agents, agent traces, row filters, column masks, group members, pipeline freshness, and more via SDK + information_schema
- **Ontology classification** — tag-based hierarchy with inheritance (e.g., `HipaaAsset → ConfidentialAsset → DataAsset`), 8 base classes, 20+ derived classes
- **Declarative rule engine** — 16 rule types including composable operators (`all_of`, `any_of`, `if_then`), metadata checks (`metadata_gte`, `metadata_lte`), and `drift_check` for expected-state comparison
- **Violation lifecycle** — open → resolved/exception, deduplication, owner attribution, per-owner digests, `remediation_status` integration
- **Compliance trend tracking** — `scan_summary` table + `v_compliance_trend` view with LAG() deltas and rolling averages
- **14 compliance views** — domain compliance, class compliance, resource compliance, tag policy coverage, data classification summary, DQ monitoring coverage, agent inventory, agent execution compliance, agent risk heatmap, AI Gateway cost governance, cross-metastore compliance/inventory
- **4 remediation views** — remediation funnel, agent performance, proposal outcomes
- **Multi-metastore support** — `metastore_id` on all tables, `crawl_all_metastores()` entrypoint, cross-metastore views
- **CDF enabled** on `resource_inventory` (with deletion vectors)
- **10 CLI entrypoints** — crawl, evaluate, notify, adhoc, crawl-all-metastores, compile, deploy, remediate, apply, verify

### Access Governance
- **Grants crawler** — `information_schema.*_privileges` + SDK `w.grants.get()` (catalog-level)
- **Service principal crawler** — application ID, active status, entitlements
- **Access governance policies** — no ALL PRIVILEGES on prod, no direct user grants, SP entitlement checks, group membership rules
- **Ontology classes** — `GrantAsset`, `OverprivilegedGrant`, `DirectUserGrant`

### AI Agent Runtime Governance
- **Agent crawler** — Databricks Apps + model serving endpoints, per-endpoint usage from `system.serving.endpoint_usage`
- **Agent ontology classes** — `AgentAsset`, `AgentWithPiiAccess`, `AgentWithExternalAccess`, `AgentWithDataExport`, `UngovernedAgent`, `ProductionAgent`, `HighRiskExecution`
- **Agent governance policies** — POL-AGENT-001 through POL-AGENT-005, POL-EXEC-001, POL-EXEC-002
- **Runtime guardrails (4 tools)** — `check_before_access`, `log_agent_action`, `get_agent_compliance`, `report_agent_execution` with session-level risk calculation
- **AI Gateway integration** — entity type, task, token cost governance view, cost risk flags (`ungoverned_high_cost`, `rate_limited`, `high_error_rate`)
- **Agent compliance dashboard** — overview KPIs, inventory detail, execution compliance, risk heatmap

### Watchdog MCP Server (13 tools)
`get_violations`, `get_governance_summary`, `get_policies`, `get_scan_history`, `get_resource_violations`, `get_exceptions`, `explain_violation`, `what_if_policy`, `list_metastores`, `suggest_policies`, `policy_impact_analysis`, `explore_governance`, `suggest_classification`

### Guardrails MCP (13 tools: 9 build-time + 4 runtime)
Build-time: `validate_table_usage`, `discover_governed_assets`, `check_policy_compliance`, `build_safely`, and 5 more. Runtime: `check_before_access`, `log_agent_action`, `get_agent_compliance`, `report_agent_execution`. Full ontology class awareness with grant violation checks and classification escalation.

### Ontos Adapter
GovernanceProvider protocol + WatchdogProvider implementation. Reads classification + violation data for governance views. Multi-metastore aware.

### Genie Space
Deployed with 27 tables (all 13 compliance views + UC system tables + `system.serving.endpoint_usage`). 5 agent SQL datasets. Instructions cover agent governance concepts, risk tiers, common agent questions.

### Industry Policy Packs
- `library/healthcare/` — HIPAA (PHI stewardship, access logging, encryption)
- `library/financial/` — SOX, PCI-DSS, GLBA
- `library/defense/` — NIST 800-171, CMMC, ITAR
- `library/general/` — CIS benchmarks, data lifecycle, cost governance
- Each pack: ontology classes + rule primitives + policies + dashboard SQL

### Drift Detection
- **`drift_check` rule type** — implemented in the rule engine dispatch table, evaluates resources against externally declared expected state
- **v1:** Grant drift detection (`POL-DRIFT-001`) — extra, missing, and modified grants
- **v2:** Row filter drift (`POL-DRIFT-002`), column mask drift (`POL-DRIFT-003`), group membership drift (`POL-DRIFT-004`)
- **Expected state loader** — reads `expected_state.json` from UC volume, injects into resource metadata for policy evaluation
- **OPA bundle loader** — alternative expected state source for Open Policy Agent bundles
- **New crawlers:** `_crawl_row_filters`, `_crawl_column_masks`, group member enrichment via `_crawl_groups`
- **New ontology base classes:** `RowFilterAsset`, `ColumnMaskAsset`, `GroupMemberAsset`
- **Key principle:** Watchdog detects drift but never remediates. External systems own expected state and remediation.
- **Policy namespace:** External systems use `POL-PERM-*` or `POL-DRIFT-*` prefixes to avoid collisions with Watchdog's built-in `POL-A*`, `POL-AGENT-*` policies.

### Remediation Pipeline
- **Dispatcher** — routes open violations to the first matching remediation agent, idempotent (skips existing proposals)
- **Protocol** — `RemediationAgent` protocol with `handles`, `version`, and `propose()` contract
- **Tables:** `remediation_agents` (registry), `remediation_proposals` (proposals with status lifecycle), `remediation_applied` (execution audit trail)
- **Review queue** — state machine: pending_review → approved/rejected → applied → verified
- **Applier** — executes approved proposals with dry-run support
- **Verifier** — batch verify against latest scan, rollback support
- **4 compliance views** — remediation funnel, agent performance, proposal outcomes
- **`remediation_status` column** on violations table for lifecycle integration
- **Reference agents:**
  - `StewardAgent` — assigns data steward tags (e2e tested)
  - `ClusterTaggerAgent` — adds missing cost attribution tags
  - `DQMonitorScaffoldAgent` — scaffolds data quality monitoring
  - `JobOwnerAgent` — assigns job ownership
- **CLI entrypoints:** `watchdog-remediate`, `watchdog-apply`, `watchdog-verify`

### Review UI (Ontos Adapter)
- **RemediationInbox** — split-panel review queue for approving/rejecting proposals
- **RemediationDashboard** — funnel visualization, agent cards, reviewer table
- **ProposalDiff** — before/after state comparison component
- **FastAPI router** — 6 REST endpoints for review workflow
- **GovernanceProvider** — remediation methods added to protocol

### Compile-Down Pipeline
- **Policy compiler** — policies with `compile_to` blocks compile to runtime enforcement artifacts
- **Compile targets:**
  - `uc_tag_policy` — Unity Catalog tag policies
  - `uc_abac` — Unity Catalog ABAC column masks
  - `guardrails` — Guardrails MCP configuration
- **Manifest** — `compile_output/manifest.json` tracks all compiled artifacts with checksums
- **Drift detection** — `check_drift()` compares manifest against on-disk artifacts (in_sync, drifted, missing)
- **Scanner integration** — `get_policy_artifact_state()` enriches scan results with compile-down posture; `compliance_pct` weighted by artifact state
- **Meta-violations** — PolicyEngine emits violations for drifted/missing compile-down artifacts
- **Deployer** — `deploy_artifacts()` pushes compiled artifacts to UC (tag policies, ABAC column masks) with dry-run support
- **CLI entrypoints:** `watchdog-compile`, `watchdog-deploy`

### Hub Integration
- **Hub contract YAML** — declared schema contracts for 6 compliance views consumed by the Governance Hub
- **Hub contract integration tests** — validate view schemas match contracts
- **Hub smoke test notebook** — post-deployment validation
- **Delta policy persistence** — `sync_policies_to_delta()` for Hub view JOINs

### Medallion Governance
- **Bronze/Silver/Gold policies** — governance rules scoped to medallion layer classes
- **Rule primitives** — medallion-specific reusable rule definitions
- **`metadata_lte` rule type** — maximum threshold check (complement of `metadata_gte`)
- **Pipeline freshness crawler** — enriches resources with system table pipeline data

### Policy Pack Install
- **`install_pack.sh`** — one-command policy pack installation for new customers
- **YAML merge helper** — `merge_primitives()` and `copy_policies()` for non-destructive pack install
- **E2e dry-run test** — validates healthcare pack install end-to-end

---

## Dropped from Scope

Items that are native platform territory — Watchdog does not build these:

| Item | Reason |
|---|---|
| `bulk_operations.py` (bulk tag/grant writes) | Hub Phase 2 owns bulk management |
| `access_requests.py` (RFA approval workflows) | Hub Phase 2 owns access requests |
| ABAC policy creation | Native ABAC is GA with UDF-based policies. Drift detection (comparing actual ABAC state against declared expected state) is in scope — see Drift Detection above. |
| PII auto-classification | Mosaic AI Data Classification is GA |
| DQ monitor creation/management | Lakehouse Monitoring is PuPr |
| Cost observability dashboards | Hub absorbing cost governance dashboards |
| Recommendations engine (`recommendations.py`) | Violations + MCP `explain_violation` serve the same purpose without a separate module |

---

## Repo Structure (Target State)

```
databricks-watchdog/
├── engine/                    # Core — ontology + rules + violations + crawlers
│   ├── src/watchdog/          #   The compliance posture engine
│   │   ├── compiler.py        #   Policy compile-down (compile_to → artifacts)
│   │   ├── deployer.py        #   Artifact deployment to UC tag policies / ABAC
│   │   ├── drift.py           #   Expected-state drift detection
│   │   └── remediation/       #   Remediation pipeline package
│   │       ├── dispatcher.py  #   Violation → agent routing
│   │       ├── applier.py     #   Proposal execution (dry-run support)
│   │       ├── verifier.py    #   Post-apply verification
│   │       └── agents/        #   Reference remediation agents
│   ├── ontologies/            #   Classification hierarchy + rule primitives
│   ├── policies/              #   Governance policies by domain (YAML)
│   ├── dashboards/            #   AI/BI dashboard SQL queries
│   └── resources/             #   Job + warehouse definitions
│
├── mcp/                       # Watchdog MCP server — AI governance interface
│   └── src/watchdog_mcp/      #   13 tools for AI assistants
│
├── ontos-adapter/             # Pluggable governance module for Ontos
│   ├── src/watchdog_governance/  GovernanceProvider protocol + WatchdogProvider
│   └── frontend/              #   Remediation review UI (React)
│
├── guardrails/                # AI DevKit MCP — build-time governance for agents
│   └── src/ai_devkit/         #   9 MCP tools + watchdog_client.py integration
│
├── library/                   # Industry policy packs (HIPAA, SOX, NIST, etc.)
├── scripts/                   # install_pack.sh, deployment helpers
├── terraform/                 # Infrastructure as Code
├── template/                  # Blank starting point for new customers
├── customer/                  # Worked example
└── tests/
```

---

## Success Criteria

Watchdog is succeeding if:

1. A CDO can answer "what % of my PII tables have a data steward?" in under 30 seconds
2. Resource owners get weekly violation digests with clear remediation steps
3. An SA can deploy Watchdog + HIPAA policy pack to a new customer in under 2 hours
4. AI assistants (Claude, Genie) can query governance posture via MCP without custom integration
5. AI agents building on the lakehouse get build-time governance checks via Guardrails
6. Ontos displays compliance posture in its governance views via the adapter
7. Compliance posture trends are visible over 30/60/90 day windows
8. The engine runs daily on 10,000+ resources in under 15 minutes
9. An AI agent accessing PII is flagged before the query executes (runtime governance)
10. A compliance officer can see "which agents accessed sensitive data this week" in one query
11. Agent executions that violate governance policies produce tracked violations with the same lifecycle as data violations

---

## Backlog

Future capabilities not yet built. Ordered by priority within each category.

### Pipeline Governance
- [ ] **SDP/DLT Expectations coverage policy** — Evaluate whether gold/silver tables have Spark Declarative Pipeline expectations defined. Watchdog doesn't create expectations (that's SDP's job), but it can answer: "which production tables are missing data quality expectations?" Policy: `IF gold_table OR silver_table THEN must have SDP expectations defined`. Requires crawler enrichment to detect DLT pipeline association and expectation presence per table. (Note: medallion governance and pipeline freshness crawler provide groundwork.)

### Compile-Down Extensions
- [ ] **UC row filter compile target** — Extend the compiler to emit row filter UDF definitions for ABAC enforcement
- [ ] **OPA bundle compile target** — Emit Open Policy Agent bundles from Watchdog policies for external policy engines
- [ ] **Compile-down CI integration** — GitHub Actions workflow to run `watchdog-compile` on PR and diff artifacts

### Observability
- [ ] **OpenTelemetry trace ingestion** — Read OTel traces from `system.serving` tables for deeper agent compliance monitoring (platform support GA as of Mar 2026)
- [ ] **AI Gateway inference table integration** — Read inference tables for richer agent execution data alongside `endpoint_usage`

### Remediation Extensions
- [ ] **LLM-powered remediation agents** — Agents that use foundation models for complex remediation proposals (e.g., generating column mask UDFs)
- [ ] **Remediation SLA tracking** — Time-to-remediate metrics per policy/domain/owner
- [ ] **Slack/Teams integration for review workflow** — Notify reviewers of pending proposals via messaging
