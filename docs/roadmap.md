# Watchdog Roadmap

> What Watchdog is, what it isn't, and where it's going.
> **All phases complete as of 2026-04-10.**
>
> Last updated: 2026-04-13

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

### Phase 2 — AI-Assisted Governance ✅ Complete

Focus: make Watchdog the AI interface for governance posture.

**New MCP tools (Watchdog MCP server)** ✅
- ✅ `what_if_policy` — simulate violations a proposed policy would produce (built during Phase 1)
- ✅ `suggest_policies` — analyze inventory + violation landscape, identify tag gaps, unclassified resources, and access patterns; returns suggested policy YAML
- ✅ `policy_impact_analysis` — analyze impact of deactivating, changing severity, or changing scope of an existing policy; shows affected owners and violation counts
- ✅ `explore_governance` — free-form read-only SQL against Watchdog tables with write-operation safety guard
- ✅ `suggest_classification` — find unclassified resources, analyze tag patterns, propose new ontology classes

**Genie Space integration** ✅
- ✅ Genie Space deployed with Watchdog Delta tables
- ✅ Expanded to all 13 semantic views + system.serving.endpoint_usage (27 tables total)
- ✅ 5 new agent SQL datasets: inventory, risk heatmap, executions, remediation, compliance trend
- ✅ Updated instructions with agent governance concepts, risk tiers, common agent questions

**Guardrails enhancements** ✅
- ✅ `validate_ai_query` already has full ontology class awareness — reads `gov.classes`, checks `is_pii`, `is_export_controlled`, `is_restricted`, `is_confidential`, shows ontology classes in findings
- ✅ Grant violation checks via `gov.grant_violations`
- ✅ Classification escalation when resource has overprivileged grants

### Phase 3 — Multi-Metastore + Cross-Account ✅ Complete

Focus: enterprise-scale posture across metastores.

- ✅ `metastore_id` column on all 9 tables: `resource_inventory`, `scan_summary`, `scan_results`, `violations`, `exceptions`, `resource_classifications`, `policies`, `policies_history`, `notification_queue`
- ✅ `crawl_all_metastores()` entrypoint — iterates configs, runs `crawl_all()` per metastore
- ✅ Cross-metastore views: `v_cross_metastore_compliance`, `v_cross_metastore_inventory`
- ✅ MCP tools accept optional `metastore` parameter
- ✅ `WATCHDOG_METASTORE_IDS` env var in `WatchdogConfig` — comma-separated list, `is_multi_metastore` property
- ✅ Write paths propagate metastore_id: violations MERGE, scan_results INSERT, resource_classifications INSERT
- ✅ Ontos adapter: `set_active_metastore()`, `list_metastores()`, `_metastore_clause()` on all queries
- ✅ Guardrails watchdog_client: all query functions accept `metastore_id` parameter

### Phase 4 — Industry Policy Packs ✅ Complete

Focus: opinionated, regulation-specific policy sets that customers can adopt in minutes.

- ✅ `library/healthcare/` — HIPAA policies (PHI stewardship, access logging, encryption requirements)
- ✅ `library/financial/` — SOX, PCI-DSS, GLBA policies
- ✅ `library/defense/` — NIST 800-171, CMMC, ITAR policies
- ✅ `library/general/` — CIS benchmarks, data lifecycle, cost governance
- ✅ Each pack: ontology classes + rule primitives + policies + dashboard SQL

### Phase 5 — AI Agent Runtime Governance ✅ Complete

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

**5D: Runtime Guardrails (agent middleware)** ✅

Four runtime tools that agents call during execution:
- ✅ `check_before_access(agent_id, table, operation, columns)` — deny/warn/allow with reasons, PII detection, masked view suggestions, sensitive column checks, session tracking
- ✅ `log_agent_action(agent_id, action, target, metadata)` — structured audit events with UUID, session counter
- ✅ `get_agent_compliance(agent_id)` — session state snapshot (checks passed/denied/warned, tables accessed, risk level)
- ✅ `report_agent_execution(agent_id)` — compliance status (compliant/needs_review/non_compliant), risk level, recommendations, session cleanup

Session management with `_calculate_risk_level()`: critical (PII + denied), high (PII + warned, or denied), medium (warned), low (all passed). Deployed to fe-stable as Databricks App.

**5E: Agent Compliance Dashboard** ✅

Three semantic views + four dashboard SQL query sets:
- ✅ `v_agent_inventory` — per-agent governance status, source, violations, ontology classes
- ✅ `v_agent_execution_compliance` — per-execution usage metrics, compliance status, risk flags
- ✅ `v_agent_risk_heatmap` — sensitivity × volume risk scoring with tier classification
- ✅ Dashboard queries: overview KPIs, agent inventory detail, execution compliance, risk heatmap
- ✅ PII access patterns, top consumers, error rates, violation-by-policy breakdown

**5F: Integration with AI Gateway** ✅

- ✅ Crawler enriched with AI Gateway metadata: `entity_type` (FOUNDATION_MODEL/CUSTOM_MODEL/EXTERNAL_MODEL), `task` (chat/completions/embeddings), `endpoint_creator`, `rate_limited_count`
- ✅ `v_ai_gateway_cost_governance` view: per-(endpoint, requester) token consumption with estimated DBU cost, cost tiers, model routing breakdown, governance status cross-reference
- ✅ Cost risk flags: `ungoverned_high_cost`, `rate_limited`, `high_error_rate`
- ✅ Dashboard SQL queries: cost by model, cost by requester, entity type breakdown, ungoverned high-cost consumers, rate-limited requesters, model routing analysis

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
