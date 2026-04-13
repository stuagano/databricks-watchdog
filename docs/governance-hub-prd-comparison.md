# Governance Hub vs. Watchdog vs. Ontos — Positioning & Overlap

> How Watchdog, Ontos, and Guardrails relate to the Databricks Governance Hub and native UC governance features.
>
> Sources: [Governance Hub PRD](https://docs.google.com/document/d/1CovxUt4GKJIEVcdCDInKnVUrptmFpYCGMwbSs9N5lSo), [Hub Private Preview doc](https://docs.google.com/document/d/1CrARgC5r8K1KYwO61LVB295MZyRkYJ6kHLVG0lCKf9c), [Q2 FY27 Roadmap Baseline](https://docs.google.com/document/d/1ibZOjJAv-rbteHeCTMdLcqBh_wxPDCxyWENewYJ6CCY), [Labs Proposal: Ontos](https://docs.google.com/document/d/1WXcjpwKXnUifODy65Mvp0J2n1GQRP4bhSIaSGejAF2k)
>
> Last updated: 2026-04-13. Note: All Watchdog roadmap phases are now complete. AI-assisted governance (13 MCP tools), agent runtime enforcement, and AI Gateway integration are live.

## The One-Liner

**The Governance Hub manages. Watchdog evaluates. Ontos models. Guardrails enforces at AI build time.**

None of these replace each other:

| Component | Persona | Question It Answers |
|---|---|---|
| **Governance Hub** | Metastore admins, platform teams | "Manage my tags, permissions, metastore settings. Show me usage and access dashboards." |
| **Watchdog** | CDOs, governance teams | "Across all my policies, how compliant is my estate? Who owns the gaps? Is it getting better?" |
| **Ontos** | Data product owners, domain leads | "Model my business domains, data products, contracts, and ownership — beyond what UC metadata provides." |
| **Guardrails** | AI agent developers | "Is this table safe to use in my agent? What policies apply? Build safely." |

---

## Why Ontos Is Not Replaced by the Governance Hub

The [Labs Proposal for Ontos](https://docs.google.com/document/d/1WXcjpwKXnUifODy65Mvp0J2n1GQRP4bhSIaSGejAF2k) makes this explicit:

> *"While we are adding some rudimentary features to our platform now (Databricks One, Discover Page, Governance Hub, Certification, Domains, Metrics), they are not sufficient for enterprise users to model their business efficiently."*

The Hub is an **admin management plane** — metastore settings, tag policies, ABAC, dashboards from system tables. Ontos is a **business catalog** — ODCS data contracts, Data Mesh domains, business glossary, data product lifecycle. They share UC metadata but serve fundamentally different users doing fundamentally different work.

Ontos consumes Watchdog data via the `ontos-adapter/` GovernanceProvider to show compliance posture in its governance views — "this data product has 3 open violations" alongside domain ownership and contract status.

---

## Native Platform Capabilities (Updated Q3 FY27)

These are GA or near-GA. They define what the platform owns — Watchdog, Ontos, and Guardrails complement but do not rebuild these:

| Capability | Status | What It Does | Watchdog relationship |
|---|---|---|---|
| **Governed Tags + Tag Policies** | GA | Account-level tag constraints, allowed values, permissions | Watchdog evaluates tag *compliance*. Complementary. |
| **ABAC** (row filters, column masks) | GA | Dynamic access control using governed tags + UDFs | Watchdog evaluates ABAC *coverage*. Complementary. |
| **[Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification)** | GA | AI agent auto-classifies sensitive columns, auto-tags | Watchdog evaluates classification *coverage*, not the classification itself. Complementary. |
| **Detect → Tag → Mask pipeline** | GA | Classification auto-tags, tags trigger ABAC | Watchdog evaluates whether the pipeline ran and covers what it should. Complementary. |
| **[AI-Generated Documentation](https://www.databricks.com/blog/announcing-public-preview-ai-generated-documentation-databricks-unity-catalog)** | PuPr | LLM auto-generates table/column descriptions | **Replaces** Watchdog's proposed DocAgent (remediation PRD). Don't rebuild. |
| **Tag Propagation** | In progress (EDC-913) | Governed tags flow through lineage | Watchdog will evaluate propagation completeness once available. |
| **Lakehouse Monitoring (DQM)** | PuPr | Anomaly detection, quality metrics, root cause analysis | Watchdog evaluates DQM *coverage*, not the monitoring itself. |
| **Governance Hub** | GA | Unified UI: dashboards, tag policy management, metastore admin | Watchdog feeds Hub via Delta tables. Hub manages, Watchdog evaluates. |
| **[AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/overview-serving-endpoints)** | GA | Rate limits, fallbacks, PII detection, payload logging per endpoint | Watchdog adds *policy-based* governance (ontology, violations, risk scoring). Gateway is runtime enforcement, Watchdog is posture + compliance. |
| **[AI Gateway inference tables](https://docs.databricks.com/aws/en/ai-gateway/inference-tables)** | GA | Request/response/trace capture as UC Delta tables | Watchdog currently reads `system.serving.endpoint_usage`. Could also read inference tables for richer agent monitoring. |
| **[OpenTelemetry endpoint telemetry](https://docs.databricks.com/aws/en/release-notes/product/2026/march)** | GA (Mar 2026) | Persists logs, traces, metrics from serving endpoints into UC Delta | Future Watchdog enhancement: read OTel traces for deeper agent compliance. |
| **[AI Governance Framework (DAGF)](https://www.databricks.com/blog/introducing-databricks-ai-governance-framework)** | Published | 5-pillar framework, 43 considerations for responsible AI | Framework is guidance. Watchdog is programmatic execution of the monitoring/compliance pillar. |

---

## Where Each Tool Adds Value the Platform Does Not Provide

### Watchdog — Compliance Posture

1. **Cross-domain compliance posture measurement** — the platform enforces per-domain (ABAC for access, DQM for quality, Tag Policies for metadata). Nobody answers "across ALL my policies, what % of my estate is compliant?"
2. **Ontology-based classification with policy inheritance** — hierarchical: `HipaaAsset → ConfidentialAsset → DataAsset`. Add a policy to the parent, every child inherits it. Tag Policies constrain values; the ontology classifies resources into a taxonomy.
3. **Violation lifecycle management** — stateful violations (open → resolved → exception), deduplication, exception expiration, per-owner digests, 30/60/90 day trends. The platform has no "violation" concept.
4. **Declarative composable rules** — `all_of`, `any_of`, `if_then`, `metadata_gte`, named reusable primitives. Tag Policies enforce "this tag must use these values." Watchdog rules express arbitrary cross-tag logic.
5. **AI governance interface (MCP server)** — 6+ tools for AI assistants to query compliance posture. Hub PRD puts "AI-assisted governance" in Future Phases.
6. **Industry policy packs** — HIPAA, SOX, NIST as drop-in YAML. Platform ships generic primitives.

### Ontos — Business Catalog

1. **Business semantics** — domains, sub-domains, data products, ODCS contracts, business glossary, ownership beyond UC's `owner` tag
2. **Data Mesh modeling** — data product lifecycle (draft → published → deprecated), SLAs, quality contracts
3. **Enterprise catalog for non-technical users** — business-friendly discovery and organization that UC metadata alone doesn't provide
4. **Governance views powered by Watchdog** — compliance posture surfaced alongside business context ("this data product has 3 open critical violations")

### Guardrails — AI Build-Time Governance

1. **Agent-time policy checks** — "is this table safe to use?" before an AI agent reads it, not after
2. **Watchdog-aware tool validation** — reads Watchdog classifications and violations to inform guardrail decisions
3. **Defense-in-depth rules** — layered governance checks beyond what ABAC provides (which only fires at query time)
4. **Structured compliance logging** — audit trail of what AI agents accessed and what governance checks they passed

---

## Feature-by-Feature Comparison with Governance Hub

### Hub Phase 1 — Foundational Data Governance UI

| Hub Feature | Watchdog | Ontos | Guardrails |
|---|---|---|---|
| **Governance Hub entry point** (consolidated UI) | N/A — headless engine. Feeds Hub via Delta. | N/A — separate app with its own UI for business users. | N/A — MCP tools, no UI. |
| **Tag Policies page** | Evaluates tag compliance (are tags present? valid?). | Surfaces tag compliance in data product views. | Checks tag policies before agent uses a table. |
| **Metastore administration** | Out of scope — read-only. | Out of scope. | Out of scope. |
| **Embedded Governance Dashboards** | Compliance/violations/quality posture dashboards. Complementary to Hub's usage/access dashboards. | Business-context dashboards (domain health, product lifecycle). | N/A. |
| **Data Classification page** | Evaluates "does classification exist? does PII have a steward?" | Surfaces classification in data product catalog. | Checks classification before agent accesses sensitive data. |
| **Data Quality monitoring page** | Evaluates DQM/LHM coverage ("do gold tables have monitors?"). | Shows DQ status per data product. | Checks DQ status before agent trusts a table. |
| **ABAC policy builder** | Evaluates "does sensitive data have ABAC coverage?" | Out of scope. | Validates ABAC is in place before agent accesses data. |

### Hub Phase 2A — Bulk Management & Recommendations

| Hub Feature | Watchdog | Ontos | Guardrails |
|---|---|---|---|
| **Permissions list** | Crawls grants for policy evaluation. | Out of scope. | Checks grant status for agent identity. |
| **Bulk operations** | Out of scope — read-only. | Out of scope. | Out of scope. |
| **Access Requests (RFA)** | Out of scope. | Could surface RFA status per data product (future). | Out of scope. |
| **Curated actions / recommendations** | **Core value.** Violation lifecycle + owner digests = curated actions. | Surfaces Watchdog violations as actions per data product. | N/A. |

### Hub Phase 2B — Account Level

| Hub Feature | Watchdog | Ontos | Guardrails |
|---|---|---|---|
| **Cross-metastore aggregation** | Phase 3: `metastore_id` on all tables + multi-metastore scanning. | Consumes Watchdog's cross-metastore data. | Metastore-aware policy checks. |

### Future Phases

| Hub Feature | Watchdog | Ontos | Guardrails |
|---|---|---|---|
| **AI-assisted governance** | **Live today.** 13 MCP tools (suggest_policies, policy_impact_analysis, explore_governance, suggest_classification). | Could wire Genie to Ontos data. | **Live today.** 13 tools: 9 build-time + 4 runtime agent governance. |
| **Cost Governance** | Cost policy evaluation (keep). AI Gateway cost governance view live. Cost dashboards (defer to Hub). | Cost per data product (future). | Out of scope. |
| **AI/ML Governance** | **Live today.** Agent crawling, execution traces, risk scoring, compliance dashboard (10 pages). | Model governance in data product catalog (future). | **Live today.** `check_before_access`, `report_agent_execution`, session tracking. |
| **Auto-documentation** | **Defer to platform.** AI-Generated Documentation is now PuPr. Remediation agents PRD DocAgent is superseded. | Could consume platform-generated docs. | Out of scope. |

---

## Summary: Built / Keep / Defer / Drop

All phases complete as of 2026-04-10.

| Category | Status | Owner |
|---|---|---|
| Ontology engine + classification hierarchy | **Built** ✅ — unique, no native equivalent | Watchdog engine |
| Declarative rule engine + composition | **Built** ✅ — unique | Watchdog engine |
| Violation lifecycle + owner digests | **Built** ✅ — ahead of Hub Phase 2A | Watchdog engine |
| Grants crawler + access governance | **Built** ✅ | Watchdog engine |
| Watchdog MCP (13 AI governance tools) | **Built** ✅ — ahead of Hub future phases | Watchdog MCP |
| Guardrails MCP (13 tools: 9 build + 4 runtime) | **Built** ✅ | Guardrails |
| AI agent runtime enforcement | **Built** ✅ — check_before_access, report_agent_execution | Guardrails |
| AI Gateway cost governance | **Built** ✅ — token cost views, risk flags | Watchdog engine |
| Industry policy packs (4 packs) | **Built** ✅ — reusable IP the platform won't provide | Watchdog library |
| Multi-metastore scanning | **Built** ✅ — metastore_id on all 9 tables | Watchdog engine |
| Ontos adapter (GovernanceProvider) | **Keep** — Ontos reads Watchdog data | ontos-adapter |
| Cost policy evaluation | **Keep** — defer cost dashboards to Hub | Watchdog engine |
| DQ policy evaluation | **Keep** — don't build monitors, evaluate coverage | Watchdog engine |
| Auto-documentation (DocAgent) | **Drop** — [AI-Generated Documentation](https://www.databricks.com/blog/announcing-public-preview-ai-generated-documentation-databricks-unity-catalog) is now PuPr on the platform | Platform |
| Bulk operations (tag/grant writes) | **Drop** — Hub Phase 2 | Governance Hub |
| Access request workflows (RFA) | **Drop** — Hub Phase 2 | Governance Hub |
| PII auto-classification | **Drop** — [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) GA | Platform |
| ABAC policy creation | **Drop** — native ABAC GA | Platform |
| AI Gateway PII detection/filtering | **Complementary** — Gateway does runtime filtering, Watchdog does posture + policy evaluation | Platform + Watchdog |
