# p-data-classification — Automated Data Classification & Tag Taxonomy

**Date:** 2026-03-19
**Status:** Coded on branch
**Branch:** `proposals/stuart-handoff/p-data-classification`
**Implements:** d-pmqv6 (flexible tagging/access-control instead of rigid classification rules)
**Resolves:** b-dc01, b-dc02, b-dc03
**Roadmap:** Phase 2 (Governance & Scale) — ABAC step 1 + step 2; prerequisite for row filters + column masks in Phase 3
**Priority:** Tier 2 — Enables ABAC, regulatory compliance, and data stewardship

## Problem

The platform has no automated way to identify which tables contain PII, PHI, ITAR-controlled, or otherwise sensitive data. Classification is manual (if it happens at all), which means:

1. **Compliance risk** — the customer operates in nuclear/radiation markets with ITAR and export control requirements. Sensitive columns go untagged.
2. **ABAC can't start** — Tag-based access policies (Phase 3) require tags to exist first.
3. **Watchdog policies are incomplete** — POL-S001 through POL-S004 check for classification tags, but those tags don't get applied automatically.

## Scope

### b-dc01 — Enable Auto-Classification on All Catalogs

Databricks Data Classification (Public Preview/Beta on Azure) uses agentic AI to scan tables and auto-tag columns with `system.classifier.*` governed tags. Results land in `system.data_classification.results`.

**Terraform:** Add `databricks_data_classification_catalog_config` resource to the catalog module. Enabled per-catalog with a boolean gate.

**What gets tagged automatically:**
- PII (names, emails, SSNs, phone numbers, addresses)
- PHI (medical record numbers, diagnosis codes, treatment data)
- Financial (credit card numbers, bank accounts)
- Network identifiers (IP addresses, MAC addresses)

**Prerequisite:** Compliance Security Profile is already enabled (HIPAA + PCI_DSS) — confirmed in `modules/workspace/main.tf`.

### b-dc02 — Platform Tag Taxonomy

Define the standard tags the platform enforces beyond auto-classification:

| Tag Key | Values | Applied By | Purpose |
|---------|--------|------------|---------|
| `system.classifier.*` | Auto-detected (PII, PHI, etc.) | Databricks auto-classification | Column-level sensitivity |
| `data_classification` | public, internal, confidential, restricted, pii | Data steward (manual) | Table-level sensitivity |
| `data_steward` | email address | Data steward (manual) | Accountability |
| `data_layer` | bronze, silver, gold | Pipeline (automated) | Medallion layer |
| `retention_days` | integer | Data steward (manual) | Data lifecycle |
| `export_classification` | NONE, EAR, ITAR | Data steward (manual) | Export control |
| `regulatory_domain` | HIPAA, ITAR, SOX, GDPR | Data steward (manual) | Regulatory scope |

Auto-classification handles column-level detection. Manual tags handle table-level governance context that AI can't infer (export control, retention, stewardship).

### b-dc03 — Watchdog Classification Enforcement Policies

Extend watchdog to enforce that auto-classification results are acted on:

| Policy | Severity | What it enforces |
|--------|----------|-----------------|
| POL-S009 | Critical | Tables with auto-detected PII columns must have a `data_steward` tag |
| POL-S010 | High | Tables with auto-detected PHI columns must have `regulatory_domain = HIPAA` |
| POL-S011 | High | Auto-classification must be enabled on all catalogs (no unscanned catalogs) |
| POL-R009 | Critical | ITAR-tagged tables must have `data_steward` + `export_classification = ITAR` |

## Dependencies

| Dependency | Status | Notes |
|------------|--------|-------|
| Compliance Security Profile | ✅ Done | HIPAA + PCI_DSS enabled on all workspaces |
| Unity Catalog catalogs exist | ✅ Done (alpha) | Alpha has `bronze/silver/gold` |
| Watchdog (p-watchdog) | ✅ Coded | Policies extend existing security_governance.yml |
| Data Classification Preview | ✅ Available | Public Preview/Beta on Azure |

## Risks

| Risk | Mitigation |
|------|------------|
| Data Classification is Preview/Beta | Feature is stable enough for non-blocking governance — tags are informational, not enforcement. ABAC enforcement comes later in Phase 3. |
| False positives in auto-classification | Watchdog policies alert data stewards to review, not auto-block access. Human-in-the-loop for remediation. |
| Performance impact of scanning | Classification scans run asynchronously, new data scanned within 24 hours. No impact on query performance. |

## Implementation

### Terraform (b-dc01)

New resource in `modules/catalog/main.tf`:

```hcl
resource "databricks_data_classification_catalog_config" "this" {
  for_each = var.enable_data_classification ? var.catalogs : {}

  catalog_name = databricks_catalog.this[each.key].name
  enabled      = true

  provider = databricks.workspace
}
```

New variable in `modules/catalog/variables.tf`:

```hcl
variable "enable_data_classification" {
  type        = bool
  description = "Enable auto-classification on all catalogs in this module"
  default     = false
}
```

### Tag Taxonomy (b-dc02)

Document in `docs/tag-taxonomy.md` — single reference for all platform tags, who applies them, and what watchdog enforces.

### Watchdog Policies (b-dc03)

Add POL-S009 through POL-S011 and POL-R009 to `security_governance.yml` and `regulatory.yml` on the p-watchdog branch. New rule primitive: `has_pii_columns` (checks `system.data_classification.results` for the catalog).

## Order of Operations

1. **b-dc01** — Terraform config (gated by boolean, safe to merge)
2. **b-dc02** — Tag taxonomy doc (design artifact, no infrastructure)
3. **b-dc03** — Watchdog policies (extends p-watchdog branch)

## Estimated Effort

- Terraform config: 30 minutes
- Tag taxonomy doc: 1 hour
- Watchdog policies: 1 hour

**Total: ~2.5 hours**
