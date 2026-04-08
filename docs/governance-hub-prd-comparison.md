# Governance Hub vs. Watchdog vs. Ontos — Positioning & Overlap

> How Watchdog, Ontos, and Guardrails relate to the Databricks Governance Hub and native UC governance features.
>
> Sources: [Governance Hub PRD](https://docs.google.com/document/d/1CovxUt4GKJIEVcdCDInKnVUrptmFpYCGMwbSs9N5lSo), [Hub Private Preview doc](https://docs.google.com/document/d/1CrARgC5r8K1KYwO61LVB295MZyRkYJ6kHLVG0lCKf9c), [Q2 FY27 Roadmap Baseline](https://docs.google.com/document/d/1ibZOjJAv-rbteHeCTMdLcqBh_wxPDCxyWENewYJ6CCY), [Labs Proposal: Ontos](https://docs.google.com/document/d/1WXcjpwKXnUifODy65Mvp0J2n1GQRP4bhSIaSGejAF2k)
>
> Last updated: 2026-04-08

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

## Native Platform Capabilities (Q1 FY27)

These are GA or near-GA. They define what the platform owns — none of the three tools (Watchdog, Ontos, Guardrails) should rebuild these:

| Capability | Status | What It Does |
|---|---|---|
| **Governed Tags + Tag Policies** | GA | Account-level tag constraints — enforce allowed values, delegate permissions, restrict who can apply tags |
| **ABAC** (row filters, column masks) | GA | Dynamic access control using governed tags + UDFs — masks/filters data at query time |
| **Mosaic AI Data Classification** | GA | Auto-detects PII across UC, tags columns, creates ABAC policies to auto-mask |
| **Detect → Tag → Mask pipeline** | GA | Classification auto-tags, tags trigger ABAC — fully automated enforcement chain |
| **Tag Propagation** | In progress (EDC-913) | Governed tags flow through lineage to derived tables — admin-configurable |
| **Lakehouse Monitoring (DQM)** | PuPr | Anomaly detection, quality metrics, root cause analysis for data quality |
| **Governance Hub** | Beta Q1 FY27 | Unified metastore-level UI: dashboards (usage, access, metadata health), tag policy management, metastore admin |
| **Governance Hub — cost/perf/AI** | Planned FY27 | Expanding to "unified observability and governance hub" across data, cost, performance, AI domains |

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
| **AI-assisted governance** | MCP server delivers this today. | Could wire Genie to Ontos data. | Guardrails IS AI-assisted governance at build time. |
| **Cost Governance** | Cost policy evaluation (keep). Cost dashboards (defer to Hub). | Cost per data product (future). | Out of scope. |
| **AI/ML Governance** | Out of scope for now. | Model governance in data product catalog (future). | AI agent governance is in scope today. |

---

## Summary: Build / Keep / Defer / Drop

| Category | Verdict | Owner |
|---|---|---|
| Ontology engine + classification hierarchy | **Build** — unique, no native equivalent | Watchdog engine |
| Declarative rule engine + composition | **Build** — unique | Watchdog engine |
| Violation lifecycle + owner digests | **Build** — ahead of Hub Phase 2A | Watchdog engine |
| Grants crawler (for policy evaluation) | **Build** — enables access governance policies | Watchdog engine |
| Watchdog MCP server + AI governance tools | **Build** — ahead of Hub future phases | Watchdog MCP |
| Industry policy packs | **Build** — reusable IP the platform won't provide | Watchdog library |
| Ontos adapter (GovernanceProvider) | **Keep** — Ontos reads Watchdog data for governance views | ontos-adapter |
| Guardrails (AI build-time enforcement) | **Keep** — inherits Watchdog classifications + violations | guardrails |
| Multi-metastore scanning | **Build** (Phase 3) | Watchdog engine |
| Cost policy evaluation | **Keep** — defer cost dashboards to Hub | Watchdog engine |
| DQ policy evaluation | **Keep** — don't build monitors, evaluate coverage | Watchdog engine |
| Bulk operations (tag/grant writes) | **Drop** — Hub Phase 2 | Governance Hub |
| Access request workflows (RFA) | **Drop** — Hub Phase 2 | Governance Hub |
| PII auto-classification | **Drop** — Mosaic AI Data Classification GA | Platform |
| ABAC policy creation | **Drop** — native ABAC GA | Platform |
