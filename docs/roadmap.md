# Watchdog Roadmap

> What Watchdog is, what it isn't, and where it's going.
>
> Last updated: 2026-04-09

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

- **Enforce access control** — that's ABAC, governed tags, row filters, column masks (native platform)
- **Manage tags or grants** — that's the Governance Hub UI and AI DevKit MCP
- **Auto-classify PII** — that's Mosaic AI Data Classification (GA Q1 FY27)
- **Create or manage DQ monitors** — that's Lakehouse Monitoring (PuPr)
- **Provide a native workspace UI** — that's the Governance Hub
- **Model business semantics** — that's Ontos (domains, data contracts, ODCS)
- **Handle bulk tag/grant operations** — that's the Governance Hub Phase 2
- **Manage access requests (RFA)** — that's the Governance Hub Phase 2

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

## Platform Landscape (as of Q1 FY27)

What's shipping natively and what it means for Watchdog scope:

| Native Capability | Status | Watchdog Implication |
|---|---|---|
| Governed Tags + Tag Policies | **GA** | Don't build tag management. Evaluate tag compliance. |
| ABAC (row filters, column masks) | **GA** | Don't build ABAC creation. Evaluate ABAC coverage. |
| Mosaic AI Data Classification | **GA** | Don't build PII detection. Evaluate classification coverage. |
| Auto-classify → auto-tag → auto-mask pipeline | **GA** | Don't replicate. Evaluate whether the pipeline ran and covers what it should. |
| Data Quality Monitoring | **PuPr** | Don't build monitors. Evaluate DQM coverage + quality score thresholds. |
| Tag Propagation through lineage | **In progress** (EDC-913) | Don't build. Evaluate propagation completeness once available. |
| Governance Hub (unified UI) | **Beta Q1 FY27** | Don't build UI. Feed Delta tables that the Hub (or Lakeview dashboards) can consume. |
| Governance Hub — cost/perf/AI observability | **Planned FY27** | Defer cost dashboard investment. Keep cost *policy evaluation*. |

---

## Roadmap

### Phase 1 — Core Engine Hardening ✅ Complete

Focus: make the existing engine robust, reusable, and easy to deploy.

**Engine improvements** ✅
- ✅ `metastore_id` column on `resource_inventory` schema (nullable, prep for multi-metastore)
- ✅ Grants crawler (`_crawl_grants()`) — `information_schema.*_privileges` + SDK `w.grants.get()` (non-inherited only + catalog-level via SDK)
- ✅ Service principal crawler (`_crawl_service_principals()`) — application ID, active status, entitlements
- ✅ Semantic views: `v_tag_policy_coverage`, `v_data_classification_summary`, `v_dq_monitoring_coverage` (9 views total)
- ✅ CDF enabled on `resource_inventory` (with deletion vectors)
- ✅ Compliance trend tracking: `scan_summary` table + `v_compliance_trend` view with LAG() deltas and rolling averages

**Access governance policies** ✅
- ✅ `policies/access_governance.yml` — no ALL PRIVILEGES on prod, no direct user grants, SP entitlement checks, group membership rules

**Ontology classes** ✅
- ✅ `GrantAsset`, `OverprivilegedGrant`, `DirectUserGrant` in `resource_classes.yml`

**Watchdog MCP server** ✅ (9 tools)
- ✅ `get_violations` — query by status, severity, resource_type, policy, owner
- ✅ `get_governance_summary` — executive overview with trends
- ✅ `get_policies` — list all policies with status
- ✅ `get_scan_history` — recent scan results
- ✅ `get_resource_violations` — full compliance history per resource
- ✅ `get_exceptions` — approved waivers
- ✅ `explain_violation` — NL explanation with remediation steps
- ✅ `what_if_policy` — simulate proposed policy impact (originally Phase 2)
- ✅ `list_metastores` — scanned metastores

**Ontos adapter** ✅
- ✅ GovernanceProvider protocol + WatchdogProvider implementation
- ✅ Reads classification + violation data for governance views

**Guardrails** ✅
- ✅ `watchdog_client.py` integration — reads classifications + violations from Delta tables
- ✅ 9 MCP tools: `validate_table_usage`, `discover_governed_assets`, `check_policy_compliance`, etc.

### Phase 2 — AI-Assisted Governance (Next)

Focus: make Watchdog the AI interface for governance posture.

**New MCP tools (Watchdog MCP server)** ✅
- ✅ `what_if_policy` — simulate violations a proposed policy would produce (built during Phase 1)
- ✅ `suggest_policies` — analyze inventory + violation landscape, identify tag gaps, unclassified resources, and access patterns; returns suggested policy YAML
- ✅ `policy_impact_analysis` — analyze impact of deactivating, changing severity, or changing scope of an existing policy; shows affected owners and violation counts
- ✅ `explore_governance` — free-form read-only SQL against Watchdog tables with write-operation safety guard
- ✅ `suggest_classification` — find unclassified resources, analyze tag patterns, propose new ontology classes

**Genie Space integration**
- ✅ Genie Space deployed with Watchdog Delta tables
- Expand Genie Space datasets to cover all 9 semantic views

**Guardrails enhancements**
- Guardrails tools call Watchdog MCP for richer policy context (not just Delta reads)
- `build_safely` tool gains awareness of ontology classes — "this table is a HipaaAsset, here are the policies that apply"

### Phase 3 — Multi-Metastore + Cross-Account

Focus: enterprise-scale posture across metastores.

- ✅ `metastore_id` column in inventory schema (nullable)
- ✅ `crawl_all_metastores()` entrypoint — iterates configs, runs `crawl_all()` per metastore
- ✅ Cross-metastore views: `v_cross_metastore_compliance`, `v_cross_metastore_inventory`
- ✅ MCP tools accept optional `metastore` parameter
- `metastore_id` column active on remaining tables: `scan_results`, `violations`, `exceptions`, `resource_classifications`, `policies`, `policies_history`
- `WATCHDOG_METASTORE_IDS` env var for multi-metastore scanning config
- Ontos adapter + Guardrails get metastore-aware queries

### Phase 4 — Industry Policy Packs ✅ Complete

Focus: opinionated, regulation-specific policy sets that customers can adopt in minutes.

- ✅ `library/healthcare/` — HIPAA policies (PHI stewardship, access logging, encryption requirements)
- ✅ `library/financial/` — SOX, PCI-DSS, GLBA policies
- ✅ `library/defense/` — NIST 800-171, CMMC, ITAR policies
- ✅ `library/general/` — CIS benchmarks, data lifecycle, cost governance
- ✅ Each pack: ontology classes + rule primitives + policies + dashboard SQL

### Phase 5 — AI Agent Runtime Governance

Focus: extend Watchdog from data compliance to **agent compliance** — govern AI agent behavior at runtime, not just data assets at rest.

The platform governs data access (ABAC at query time). MLflow traces agent execution. But nobody governs **agent behavior against policies** — does this agent's data access pattern comply with our governance rules? Did it access PII without approval? Did it export sensitive data?

**5A: Agent Crawler** ✅

- ✅ `_crawl_agents()` — Databricks Apps + model serving endpoints (heuristic keyword match)
- ✅ `_crawl_agent_traces()` — per-endpoint usage from `system.serving.endpoint_usage` (7-day window, per-requester aggregation, token counts, error rates)
- ✅ `resource_type = "agent"` and `resource_type = "agent_execution"` in inventory

**5B: Agent Ontology Classes** ✅

- ✅ `AgentAsset` base class + derived: `AgentWithPiiAccess`, `AgentWithExternalAccess`, `AgentWithDataExport`, `UngovernedAgent`, `ProductionAgent`
- ✅ Execution-level: `HighRiskExecution`

**5C: Agent Governance Policies** ✅

- ✅ `policies/agent_governance.yml` — POL-AGENT-001 through POL-AGENT-005, POL-EXEC-001, POL-EXEC-002
- ✅ Rule primitives for agent governance in `rule_primitives.yml`

**5D: Runtime Guardrails (agent middleware)**

Extend the guardrails MCP with runtime tools that agents call during execution:

- `check_before_access(agent_id, table, operation, columns)` — real-time governance check before the agent reads data. Returns allow/deny with reason and suggested alternative (e.g., masked view).
- `log_agent_action(agent_id, action, target, metadata)` — structured audit logging of agent behavior for post-execution compliance.
- `get_agent_compliance(agent_id)` — current compliance status of an agent based on its recent execution traces.
- `report_agent_execution(trace_id)` — post-execution compliance report: which policies were triggered, which data was accessed, risk score.

**5E: Agent Compliance Dashboard**

New Lakeview dashboard page and Genie Space datasets:
- Agent inventory with governance status (governed/ungoverned, PII access, external access)
- Execution compliance: % of executions that triggered policy violations
- PII access patterns: which agents access PII most frequently
- Risk heatmap: agents × data sensitivity × access frequency
- Trend: agent compliance posture over 30/60/90 days

**5F: Integration with AI Gateway**

Read AI Gateway audit logs and usage data:
- Model routing decisions correlated with data sensitivity
- Cost governance per agent (token usage × data classification)
- Rate limiting enforcement for agents accessing sensitive data

---

## Dropped from Scope

Items that are native platform territory — Watchdog does not build these:

| Item | Reason |
|---|---|
| `bulk_operations.py` (bulk tag/grant writes) | Hub Phase 2 owns bulk management |
| `access_requests.py` (RFA approval workflows) | Hub Phase 2 owns access requests |
| ABAC policy creation | Native ABAC is GA with UDF-based policies |
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
│   ├── ontologies/            #   Classification hierarchy + rule primitives
│   ├── policies/              #   Governance policies by domain (YAML)
│   ├── dashboards/            #   AI/BI dashboard SQL queries
│   └── resources/             #   Job + warehouse definitions
│
├── mcp/                       # Watchdog MCP server — AI governance interface
│   └── src/watchdog_mcp/      #   13 tools for AI assistants
│
├── ontos-adapter/             # Pluggable governance module for Ontos
│   └── src/watchdog_governance/  GovernanceProvider protocol + WatchdogProvider
│
├── guardrails/                # AI DevKit MCP — build-time governance for agents
│   └── src/ai_devkit/         #   9 MCP tools + watchdog_client.py integration
│
├── library/                   # Industry policy packs (HIPAA, SOX, NIST, etc.)
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
