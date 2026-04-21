# Watchdog Deployment Guide

## What Watchdog Is

Watchdog is a **compliance posture evaluator** for Databricks Unity Catalog. It extends the platform's built-in enforcement (ABAC, tag policies, row filters, column masks) with the measurement layer that enforcement alone cannot provide.

The platform **enforces** governance at query time -- a column mask hides PII, a tag policy rejects an invalid value. But enforcement is not posture. No native capability answers: *"across all policies, all domains, all resources -- how compliant is the estate right now? Who owns the gaps? Is it getting better or worse?"*

Watchdog answers that question. It crawls workspace resources daily, classifies them through an ontology hierarchy, evaluates declarative policies, tracks violations with owner accountability, and produces a single compliance percentage that goes up or down over time.

The analogy: the platform is the **immune system** -- it blocks threats at runtime. Watchdog is the **annual physical** -- it measures overall health, tracks trends, and identifies what to fix before symptoms appear.

## Who It Is For

- **Chief Data Officers** -- cross-domain compliance posture, trend reporting, board-ready metrics
- **Platform administrators** -- resource inventory, policy coverage gaps, operational hygiene
- **Governance teams** -- violation lifecycle management, exception workflows, audit trails
- **AI agent developers** -- build-time governance checks, runtime guardrails, agent compliance reporting

## What Watchdog Provides

- **Cross-domain evaluation.** One scan measures security, data quality, cost, operations, and AI agent governance together.
- **Ontology classification with inheritance.** One policy on `ConfidentialAsset` automatically covers PII, HIPAA, SOX, and every child class -- no flat tag duplication.
- **Composable declarative rules.** `IF pii THEN must have steward AND retention` -- logic the platform cannot express natively.
- **Violation lifecycle.** Open, resolved, exception -- with deduplication, first-detected dates, owner attribution, and remediation steps.
- **Compliance trends.** Scan-over-scan deltas, direction indicators, and rolling averages across 30/60/90-day windows.
- **AI agent governance.** Crawls agents and execution traces, classifies managed vs. customer endpoints, evaluates agent-specific policies, produces prioritized remediation.
- **AI interface.** 13 MCP tools so AI assistants and autonomous agents can query and act on governance posture programmatically.
- **Notifications.** Per-owner violation digests with remediation guidance, delivered via Delta queue or email.
- **Industry policy packs.** Pre-built YAML policies for healthcare (HIPAA), financial services (SOX, PCI-DSS, GLBA), defense (NIST 800-171, CMMC, ITAR), and general benchmarks.

## What Watchdog Does Not Do

Watchdog is deliberately scoped to **measurement and posture evaluation**. It does not replace or duplicate native platform capabilities:

| Responsibility | Platform Owner | Watchdog Role |
|---|---|---|
| Enforce access control (ABAC, row filters, column masks) | Unity Catalog | Evaluate ABAC coverage and detect drift |
| Manage tags and grants | Governance Hub | Evaluate tag compliance across policies |
| Auto-classify PII | Mosaic AI Data Classification | Evaluate classification coverage |
| Auto-generate documentation | AI-Generated Documentation | Evaluate documentation completeness |
| Rate-limit or filter PII at the gateway | AI Gateway | Add ontology-aware governance on top |
| Create or manage DQ monitors | Lakehouse Monitoring | Evaluate monitoring coverage |
| Provide a native workspace UI | Governance Hub | Feed Delta tables that dashboards consume |
| Model business semantics | Business catalog tools | Provide classification data for governance views |
| Handle bulk tag/grant operations | Governance Hub | Not in scope |
| Manage access requests | Governance Hub | Not in scope |

## Architecture at a Glance

Watchdog operates in a three-tier model:

```
Tier 3: AI Interface Layer
  Watchdog MCP (13 tools) -- compliance queries, simulation, exploration
  Guardrails MCP (13 tools) -- build-time + runtime governance for AI agents
  Business catalog adapter -- GovernanceProvider protocol

Tier 2: Observability Layer
  Watchdog Engine -- daily scan producing Delta tables
  18 compliance + remediation views -- domain compliance, agent inventory, trends
  Violation lifecycle -- open / resolved / exception with MERGE dedup

Tier 1: Control Plane (Databricks Platform)
  Unity Catalog -- tags, grants, ABAC, information_schema
  System tables -- system.serving.endpoint_usage, served_entities
  SDK APIs -- jobs, clusters, warehouses, service principals, apps
```

The control plane enforces. The observability layer measures. The AI interface layer queries.

## Guide Structure

This guide is organized into four sections:

### [Concepts](concepts/architecture.md)

How Watchdog works -- architecture, ontology, policies, violations.

- [Architecture](concepts/architecture.md) -- the observability layer model, components, data flow
- [Ontology](concepts/ontology.md) -- resource classification with inheritance
- [Policies](concepts/policies.md) -- declarative rules, severity, composition, hybrid management
- [Violations](concepts/violations.md) -- lifecycle, deduplication, exceptions, trends

### [Getting Started](getting-started/prerequisites.md)

Deploy Watchdog and run a first scan in 30 minutes.

- [Prerequisites](getting-started/prerequisites.md) -- workspace, permissions, tooling
- [Quickstart](getting-started/quickstart.md) -- clone, configure, deploy, scan
- [First Dashboard](getting-started/first-dashboard.md) -- views, dashboards, example queries

### How-To (coming soon)

Task-oriented guides for common operations -- adding policies, customizing the ontology, configuring notifications, deploying MCP servers.

### Reference (coming soon)

Complete reference for all tables, views, rule types, policy fields, MCP tools, and CLI commands.
