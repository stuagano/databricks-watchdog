# Hub Catalog Single-Writer Policy

**Status:** Proposed
**Date:** 2026-05-28
**Source:** Imported from an external industry proposal (`p-uc-isolation` asymmetric-writer pattern).
**Builds on:** `2026-05-28-hub-publishing-governance-design.md` (HubTable ontology).

---

## Problem

The hub-publishing model is intentionally asymmetric: many readers, one writer. A single "promotion" service principal holds `MODIFY` on the hub catalog; everyone else holds `SELECT`. This is the property that makes the promotion pipeline a *choke point* — every write is funneled through a code path that runs scans, validates lineage, and emits audit. A side-channel grant (an admin adding `MODIFY` to a second principal "just for a one-off backfill" and forgetting) silently bypasses the choke point.

Today `access_governance.yml` has policies for least-privilege (no ALL PRIVILEGES on production, no direct user grants, etc.) but none of them assert the structural invariant *"this catalog has exactly one write-capable principal."* The grants drift slowly; the misconfig appears only when an audit looks closely.

## Goal

A targeted policy that catches single-writer drift on hub catalogs the moment a second writer is granted. Two additions:

1. **Rule primitive `has_single_writer_principal`** — counts distinct principals holding any write-equivalent grant across all workspace bindings of a hub catalog, asserts the count is exactly 1.
2. **Policy POL-HUB-004** in the hub-publishing policy pack — applies to `HubTable` (defined in the hub-publishing-governance spec) and binds the primitive with a configurable list of grants treated as "writer" grants.

## Non-Goals

- **Detecting bypass writes after the fact.** That's audit territory (`system.access.audit`). This spec catches the *grant* misconfiguration; audit catches the *use* of the misconfiguration.
- **Remediation.** Watchdog never writes — it flags. Remediation is operator-driven (revoke the extra grant, route through the promotion pipeline).
- **Workspace-binding-level grants.** If a hub catalog is bound `READ_WRITE` to two workspaces but only one principal holds `MODIFY` across both, that's still single-writer. The primitive counts principals, not bindings.
- **Service principal pool detection.** A "promotion-SP-group" with N members technically has one *group* writer but N humans behind it. v1 treats the group as one writer; the SoD concern is addressed by `POL-A004` (groups with MANAGE require multiple members), not here.

---

## Rule Primitive

In `engine/ontologies/rule_primitives.yml`:

```yaml
has_single_writer_principal:
  description: >
    For a UC catalog or schema, asserts that exactly one principal holds any grant
    in the configured 'writer' set, across all workspace bindings.
  params:
    writer_grants: [MODIFY, ALL_PRIVILEGES, MANAGE]
  evaluates: |
    let writers = distinct(
      grants
        .where(grant.privilege in writer_grants)
        .where(grant.target == resource.fqn)
        .pluck(grant.principal)
    )
    writers.count == 1
```

The grants table is already populated by `engine/src/watchdog/crawler.py` (which reads `information_schema.privilege_assignments` for catalogs and schemas). No new crawler work needed.

`writer_grants` defaults to `[MODIFY, ALL_PRIVILEGES, MANAGE]` — these are the grants that allow `WRITE`/`INSERT`/`UPDATE`/`DELETE` or other state changes. Deployments can override (e.g., a customer who has retired `ALL PRIVILEGES` per POL-A001 can drop it from the list to reduce evaluation cost).

## Policy

In `engine/policies/hub_publishing.yml` (file created by the hub-publishing-governance spec):

```yaml
- id: POL-HUB-004
  name: "Hub catalogs must have exactly one writer principal"
  applies_to: HubTable
  domain: SecurityGovernance
  severity: critical
  description: >
    Hub-style catalogs operate on a "many readers, one writer" model. The single
    writer principal is the promotion pipeline's service principal — every write
    to the hub goes through the pipeline's validation and audit path. Multiple
    writer principals enable side-channel writes that bypass governance.
  remediation: >
    1. List all grants on the hub catalog:
       SELECT principal, privilege FROM system.information_schema.catalog_privileges
       WHERE catalog_name = '{hub_catalog}' AND privilege IN ('MODIFY', 'ALL PRIVILEGES', 'MANAGE')
    2. Identify the promotion SP (or pipeline service principal).
    3. Revoke writer grants from all other principals.
    4. If a second writer is required (rare), document the exception and approve via
       the exception workflow before revoking the flag.
  rule:
    ref: has_single_writer_principal
```

Severity `critical` — multiple writers on a hub catalog is one of the small set of misconfigs that *silently* breaks governance, so the policy lands at the top of the actionable queue rather than blending into the noise.

## Configuration

The default `writer_grants` list is good for most deployments. To override per deployment, customers can either:

- Set the primitive's default at the policy level:

  ```yaml
  rule:
    ref: has_single_writer_principal
    params:
      writer_grants: [MODIFY, ALL_PRIVILEGES]   # excluded MANAGE
  ```

- Or set a global default in `engine/databricks.yml`:

  ```yaml
  rule_defaults:
    has_single_writer_principal:
      writer_grants: [MODIFY, ALL_PRIVILEGES]
  ```

The primitive falls back to its declared default if neither is provided.

## Interaction With Existing Policies

| Existing | Interaction |
|---|---|
| POL-A001 (no ALL PRIVILEGES) | Removes ALL PRIVILEGES from the realistic writer set; POL-HUB-004 still works because it consults the configured list (which may or may not include ALL PRIVILEGES depending on customer state). |
| POL-A002 (no direct user grants) | Independent — this is about *count* of principals, not their type. |
| POL-A004 (groups with MANAGE need >=2 members) | Complementary — POL-HUB-004 ensures only one *group* holds writer; POL-A004 ensures that group has enough humans. |

No existing policy is invalidated. POL-HUB-004 attaches a new structural invariant that the others don't cover.

## Dependencies

- `2026-05-28-hub-publishing-governance-design.md` — defines `HubTable` ontology class. POL-HUB-004 has no resources to apply to without it.
- Existing grants crawl (`engine/src/watchdog/crawler.py` already reads `information_schema.privilege_assignments`).

## Risks

| Risk | Mitigation |
|---|---|
| Customers with a multi-writer hub pattern get false-positive critical violations | Policy is opt-in by being attached only to `HubTable` — customers without hub catalogs see nothing. Customers with multi-writer hubs can disable POL-HUB-004 or approve a long-term exception with rationale. |
| `writer_grants` default misses a vendor-specific equivalent | Configurable per deployment; documented in the policy comment. |
| Grant evaluation cost on large catalogs | The primitive operates on already-crawled grants table; no fresh SQL per evaluation. |
| Single-writer principal rotates (sp-publisher → sp-publisher-v2) | Transient state during rotation triggers two writers briefly. Mitigate by approving a short exception during rotation, or by rotation procedure that revokes old before granting new. |

## Order of Operations

1. Add `has_single_writer_principal` rule primitive.
2. Add POL-HUB-004 to `engine/policies/hub_publishing.yml`.
3. Unit tests: fixture grants with 0, 1, 2, 3 writer principals → expected violation/no-violation.
4. Docs: extend the hub-publishing concept doc with the single-writer invariant.

## Estimated Effort

| Phase | Effort |
|---|---|
| Rule primitive | 0.5 days |
| Policy + tests | 0.5 days |
| Docs | 0.25 days |
| **Total** | **~1.25 days** |
