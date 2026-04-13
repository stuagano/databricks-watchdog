# Watchdog Integration Plan

> How Watchdog feeds its three consumers: Governance Hub, Ontos, and Guardrails.
>
> Last updated: 2026-04-13
> Companion docs: [roadmap.md](./roadmap.md), [governance-hub-prd-comparison.md](./governance-hub-prd-comparison.md)

## Design Principles

1. **Delta tables are the universal contract.** All consumers (Hub, Ontos, Guardrails) read Watchdog's Delta tables. No consumer-specific APIs — just tables and views.
2. **Watchdog is read-only.** It crawls and evaluates. It never writes tags, grants, or ABAC policies. All write operations belong to the platform.
3. **Three integration surfaces, one data model.** Hub reads Delta for dashboards. Ontos reads Delta via GovernanceProvider for business catalog views. Guardrails reads Delta via `watchdog_client.py` for AI build-time checks. Same tables, different consumers.
4. **MCP server is the AI gateway.** AI assistants and Guardrails tools can query governance posture through Watchdog MCP. Centralizes AI governance logic.
5. **Multi-metastore is a filter, not a partition.** All metastores write to the same tables with a `metastore_id` discriminator.

---

## Consumer Map

```
                    ┌─────────────────────┐
                    │   Watchdog Engine    │
                    │   (Delta tables)     │
                    └───┬─────┬────────┬──┘
                        │     │        │
           ┌────────────┘     │        └────────────┐
           ▼                  ▼                      ▼
┌──────────────────┐ ┌───────────────┐ ┌─────────────────────┐
│  Governance Hub   │ │  Ontos        │ │  Guardrails         │
│                   │ │               │ │                     │
│  Reads:           │ │  Reads via:   │ │  Reads via:         │
│  Delta tables     │ │  Governance   │ │  watchdog_client.py │
│  directly (SQL)   │ │  Provider     │ │                     │
│                   │ │  protocol     │ │  Provides:          │
│  Surfaces:        │ │               │ │  AI build-time      │
│  Dashboards,      │ │  Surfaces:    │ │  governance checks  │
│  tag mgmt,        │ │  Compliance   │ │  for agents         │
│  metastore admin  │ │  posture in   │ │                     │
│                   │ │  business     │ │  9 MCP tools incl:  │
│  Persona:         │ │  catalog      │ │  validate_table     │
│  Platform admins  │ │  views        │ │  discover_governed  │
│                   │ │               │ │  check_compliance   │
│                   │ │  Persona:     │ │  build_safely       │
│                   │ │  Domain leads │ │                     │
│                   │ │               │ │  Persona:           │
│                   │ │               │ │  AI developers      │
└──────────────────┘ └───────────────┘ └─────────────────────┘
```

---

## Consumer 1: Governance Hub

The Hub reads Watchdog's Delta tables directly — no adapter, no API layer.

### Table → Panel Mapping

| Hub Panel | Watchdog Source | Notes |
|---|---|---|
| Governance Dashboard — compliance overlay | `v_domain_compliance`, `v_class_compliance` | Posture % by domain and ontology class. Supplements Hub's usage/access/metadata panels. |
| Drill-down by resource | `v_resource_compliance` + `violations` | Per-resource violation history with status, severity, remediation. |
| Data Classification coverage | `resource_classifications` | Which resources are classified, into which ontology classes. Hub surfaces classification results; Watchdog evaluates coverage. |
| Data Quality coverage | Scan results for DQ policies | "Do gold tables have DQM? Do critical tables meet quality score thresholds?" |
| Tag compliance | `v_tag_policy_coverage` (new) | Per-resource tag policy compliance state. |
| Curated actions / recommendations | `violations` filtered by status=open | Watchdog violations ARE the curated actions — each has owner, severity, remediation guidance. |

---

## Consumer 2: Ontos

Ontos reads Watchdog via the `ontos-adapter/` GovernanceProvider protocol.

### What Ontos Gets from Watchdog

| Data | Source | How Ontos Uses It |
|---|---|---|
| Resource classifications | `resource_classifications` table | Shows ontology class per data product ("this table is a HipaaAsset") |
| Compliance posture | `v_resource_compliance` view | "This data product has 3 open critical violations" in the product detail view |
| Violation details | `violations` table | Drill-down from data product to specific policy failures |
| Policy definitions | `policies` table | Lists which policies apply to a data product's ontology class |
| Exception status | `exceptions` table | Shows approved exceptions alongside violations |

### GovernanceProvider Protocol

The `ontos-adapter/src/watchdog_governance/provider.py` protocol is the contract. The `WatchdogProvider` implementation reads from `platform.watchdog.*` Delta tables. Ontos codes against the protocol, not the implementation.

Current protocol surface (maintain and evolve with engine schema):

- `list_violations(filters)` → violations by resource, severity, status, policy
- `compliance_summary()` → domain-level and class-level compliance percentages
- `resource_compliance(resource_id)` → full compliance detail for one resource
- `list_policies()` → all active policies
- `list_exceptions(filters)` → approved exceptions
- `scan_history()` → recent scan results and status

### What Ontos Does NOT Get from Watchdog

- Business semantics (domains, contracts, glossary) — that's Ontos's own data
- Tag management — that's the platform
- Access control — that's ABAC

---

## Consumer 3: Guardrails

Guardrails reads Watchdog via `guardrails/src/ai_devkit/watchdog_client.py`.

### What Guardrails Gets from Watchdog

| Data | Source | How Guardrails Uses It |
|---|---|---|
| Resource classifications | `resource_classifications` table | "This table is a HipaaAsset — additional governance checks apply" |
| Open violations | `violations` table (status=open) | "This table has 2 open critical violations — warn the agent developer" |
| Policy applicability | `policies` + `resource_classifications` | "These policies apply to this table based on its ontology class" |
| Compliance status | `v_resource_compliance` view | Pass/fail signal for `check_policy_compliance` tool |

### Guardrails MCP Tools That Use Watchdog Data

| Tool | Watchdog Integration |
|---|---|
| `validate_table_usage` | Checks classifications + violations before allowing agent to use a table |
| `discover_governed_assets` | Returns assets with their ontology classes and compliance status |
| `check_policy_compliance` | Evaluates whether a specific resource passes all applicable policies |
| `build_safely` | Combines classification, violation, and policy checks into a single "is it safe?" answer |

### Defense-in-Depth Model

Guardrails provides layered governance that the platform's runtime enforcement (ABAC) doesn't cover:

1. **ABAC** fires at query time — masks/filters data the agent shouldn't see
2. **Guardrails** fires at build time — warns/blocks before the agent even attempts the query
3. **Watchdog** provides the posture data that informs both layers

An agent developer using Guardrails gets: "This table is classified as PII, has ABAC coverage (good), but is missing a data steward (open violation POL-SEC-003). Proceed with caution."

---

## Engine Work for All Consumers

### New semantic views

Add to `engine/src/watchdog/views.py`:

| View | Purpose | Primary Consumer |
|---|---|---|
| `v_tag_policy_coverage` | Per-resource: which tag policies are satisfied/violated/N/A | Hub, Ontos |
| `v_data_classification_summary` | Aggregated classification posture by catalog/schema | Hub, Ontos |
| `v_dq_monitoring_coverage` | Which tables have DQM, LHM, both, or neither | Hub, Ontos |

### New crawlers

**Grants Crawler** — `_crawl_grants()`
- Source: `system.information_schema.table_privileges` + `schema_privileges` + SDK `w.grants.get()` for catalog-level
- Produces: `resource_type = "grant"`, `resource_id = "{securable_type}:{securable_id}:{grantee}:{privilege}"`
- Metadata: securable_type, securable_id, grantee, privilege, grantor, inherited_from
- Consumers: all three (Hub for dashboards, Ontos for product compliance, Guardrails for access checks)

**Service Principal Crawler** — `_crawl_service_principals()`
- Source: SDK `w.service_principals.list()`
- Produces: `resource_type = "service_principal"` rows
- Consumers: Hub, Guardrails (agent identity checks)

### New policies

`engine/policies/access_governance.yml`:

| Policy ID | Rule | Severity |
|---|---|---|
| POL-A001 | No ALL PRIVILEGES on production data | critical |
| POL-A002 | No direct user grants on catalogs (must use groups) | high |
| POL-A003 | Service principals must not have workspace admin entitlements | critical |
| POL-A004 | Groups with MANAGE on catalogs must have at least 2 members | medium |

### New ontology classes

- `GrantAsset` — base class for grants (`matches_resource_types: [grant]`)
- `OverprivilegedGrant` — derived, classifier: metadata contains ALL PRIVILEGES or MANAGE
- `DirectUserGrant` — derived, classifier: grantee is not a group

### Schema changes

- Add nullable `metastore_id STRING` to all table schemas (defaults to current metastore)
- Enable CDF on `resource_inventory` and `dq_status`

---

## MCP Server Enhancements

These benefit all three consumers (Hub could call MCP for AI features, Ontos for NL queries, Guardrails for richer context):

| Tool | Purpose | Priority |
|---|---|---|
| `explain_violation` | NL explanation of what a violation means, why it matters, how to fix it | P0 |
| `what_if_policy` | Simulates violations a proposed policy would produce against current inventory | P1 |
| `suggest_policies` | Proposes new policy YAML based on violation landscape and metadata gaps | P1 |
| `policy_impact_analysis` | Predicts how many new violations a policy change would create | P1 |
| `explore_governance` | Free-form NL → SQL against Watchdog tables | P2 |
| `suggest_classification` | Suggests ontology classes based on tags, name patterns, similar resources | P2 |

### Genie Space

Pre-built Genie Space wired to Watchdog Delta tables. Business users query governance posture in natural language. Complements the MCP server (Genie for humans, MCP for agents/tools).

---

## What We Are NOT Building

| Dropped Item | Reason | Native Owner |
|---|---|---|
| Bulk operations (`bulk_apply_tags()`, `bulk_update_grants()`) | Hub Phase 2A owns bulk management | Governance Hub |
| Access requests module (`access_requests.py`) | Hub Phase 2A owns RFA workflows | Governance Hub |
| ABAC policy crawler | Native ABAC is GA — UC already surfaces this | Platform |
| Recommendations engine (`recommendations.py`) | Violations + `explain_violation` MCP tool serve the purpose | Watchdog violations + MCP |
| Access audit log crawler | Hub surfaces access patterns natively via system tables | Governance Hub |
| User crawler | Hub owns user/identity management | Governance Hub |

