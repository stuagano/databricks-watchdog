# Imported Industry Proposals

This directory holds 14 design proposals imported from an external industry deployment that informed the design of Watchdog. They are kept here for historical reference and to provide context for the design decisions captured in `docs/superpowers/specs/`.

## Provenance

These proposals originated in a customer-specific repository as a Pn-prefixed roadmap (`p-watchdog`, `p-uc-isolation`, `p-uc-abac`, etc.). They were drafted in early to mid 2026 while Watchdog was being extracted into this standalone repo. Customer-specific names have been scrubbed during the import.

## Scrubbing

Mechanical substitutions applied during import (replacement targets shown; original customer strings deliberately not enumerated here):

| Category | Scrubbed to |
|---|---|
| Customer company name | `the customer` |
| Customer's data-platform org identifier | `CustomerDataPlatform` |
| Customer's branded catalog product name | `Customer Catalog` |
| Customer's hub catalog identifier | `hub_catalog` / `hub-catalog` |
| Customer's catalog identifier | `customer_catalog` / `customer-catalog` |
| Customer's regional metastore name | `regional-metastore` / `ucm-regional` |
| Customer's cloud region | `region` |
| Customer's mythology-themed spoke workspace names | `spoke_a`, `spoke_b` |
| Customer's ML workspace name | `ml_spoke` |
| Customer's storage account prefixes | `stcustomer*`, `sthubregional`, `strawingest` |
| Customer's group-name templates | `customer-{domain}-stewards` |
| Customer's env-var prefixes | `CUSTOMER_CATALOG_*` |

Generic industry context (HIPAA, ITAR, NRC, regulated data domains, PII/PHI examples) was left intact since it's not customer-identifying.

**Not scrubbed:** workspace names that double as English words (`alpha`, `beta`, `sandbox`, `promotion`). These remain as-is; readers should infer them as illustrative spoke/role labels rather than customer choices.

## Status

Each proposal's frontmatter declares its status. As of the 2026-05-28 import:

| Proposal | Status | Notes |
|---|---|---|
| `p-watchdog(P1).md` | Complete | Parent proposal — Watchdog itself. Implemented across the codebase. |
| `p-watchdog-dashboards(P2).md` | ✅ Superseded | Implemented in `engine/dashboards/` and `engine/src/watchdog/views.py`. |
| `p-watchdog-discover(P3).md` | ❌ Dropped | Consumer-browse `discover_domain` concept was not built and is not on the roadmap. The Ontos adapter (`ontos-adapter/`) covers a different surface. |
| `p-watchdog-docs(P2).md` | ✅ Superseded | Doc tree at `docs/`. |
| `p-watchdog-exceptions(P2).md` | ✅ Superseded | Exception lifecycle moved to `ontos-adapter/`'s FastAPI router (see `docs/superpowers/specs/2026-05-28-exception-request-api-contract-design.md`). |
| `p-watchdog-mcp(P3).md` | ✅ Superseded | Tool reference at `docs/guide/reference/mcp-tools.md`; 13 tools (proposal called for 8). |
| `p-watchdog-policies(P2).md` | ✅ Superseded | Implemented across `engine/policies/`, `engine/ontologies/rule_primitives.yml`, `engine/ontologies/resource_classes.yml`. Counts diverged from the proposal. |
| `p-watchdog-tests(P2).md` | ✅ Superseded | Test suite under `engine/tests/`. |
| `p-ai-devkit(P1).md` | Ready for review (historical) | Became Watchdog Guardrails (Guardrails MCP — 13 tools in `docs/guide/reference/guardrails-tools.md`). |
| `p-data-classification(P2).md` | Coded on branch (historical) | Pack landed at `library/data-classification/` (see commit 772adb4). Crawler gap captured in `docs/superpowers/specs/2026-05-28-data-classification-crawler-design.md`. |
| `p-datasphere(P3).md` | Ready for data team (historical) | SAP/DLT ingestion design. Out of Watchdog scope — kept here for historical reference only. |
| `p-customer-catalog(P3).md` | Coded on branch (historical) | Customer-facing catalog UI design. UI is out of Watchdog scope; the exception-request portion was generalized in `docs/superpowers/specs/2026-05-28-exception-request-api-contract-design.md`. |
| `p-uc-abac(P3).md` | Proposal (no code) | Generalized into three specs: `2026-05-28-abac-policy-templates-design.md`, `2026-05-28-abac-audit-summary-design.md`, `2026-05-28-abac-dynamic-views-target-design.md`. The existing `2026-04-20-uc-abac-compile-target-design.md` covers the compile target. |
| `p-uc-isolation(P2).md` | Coded on branch (historical) | Workspace-isolation pieces (IAM, network, Terraform) are out of Watchdog scope. The governance-relevant parts were generalized into `2026-05-28-hub-publishing-governance-design.md`, `2026-05-28-hub-single-writer-policy-design.md`, and `2026-05-28-isolation-mode-policy-design.md`. |

## How to use these

- **For context on a watchdog feature**: when reading a spec in `docs/superpowers/specs/2026-05-28-*.md`, find the corresponding proposal here for the original motivation and customer-facing requirements.
- **For comparison against current implementation**: a proposal's "What this adds" section captures the original intent. Counts, names, and shapes have evolved — `docs/guide/reference/*.md` is the source of truth for what's actually shipped.
- **As a backlog of ideas that didn't ship**: `p-datasphere`, parts of `p-uc-isolation`, and the customer-catalog UI sections are deliberately out of Watchdog scope. They live here as reference if a future Watchdog extension wants to re-engage the use case.

## Why these are in this repo

The customer-side repository where these proposals originated is being archived. To keep the design history accessible alongside the implementation, the proposals were imported here on 2026-05-28. From now on, all Watchdog-related design work happens in this repo.
