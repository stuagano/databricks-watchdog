# Violations

## What a Violation Is

A violation is a record that a specific resource failed a specific policy during a scan. The `violations` table contains one row per `(resource_id, policy_id)` combination with the current status and timestamps.

Each violation includes:

| Field | Description |
|---|---|
| `violation_id` | Unique identifier (UUID) |
| `resource_id` | The failing resource |
| `resource_type` | Table, grant, agent, cluster, etc. |
| `policy_id` | The policy that was violated |
| `severity` | critical, high, medium, low |
| `domain` | Governance domain (SecurityGovernance, AgentGovernance, etc.) |
| `detail` | Human-readable description of what failed |
| `remediation` | Steps to fix the violation |
| `owner` | Resource owner (attributed from UC metadata or tags) |
| `resource_classes` | Ontology classes assigned to the resource |
| `first_detected` | Timestamp of the scan that first found this violation |
| `last_detected` | Timestamp of the most recent scan where the violation was still active |
| `resolved_at` | Timestamp when the violation was resolved (null if still open) |
| `status` | Current state: open, resolved, or exception |

## Violation Lifecycle

Violations move through three states:

```
  New failure               Still failing            No longer failing
  detected                  on next scan             on next scan
     |                          |                         |
     v                          v                         v
 +--------+  last_detected  +--------+  resolved_at  +----------+
 |  OPEN  |--------------->|  OPEN  |-------------->| RESOLVED |
 +--------+   updated       +--------+               +----------+
     |                          |
     |  exception approved      |  exception approved
     v                          v
 +-----------+             +-----------+
 | EXCEPTION |             | EXCEPTION |
 | (waiver)  |             | (waiver)  |
 +-----------+             +-----------+
      |
      | exception expires
      v
   +--------+
   |  OPEN  |  (re-opened)
   +--------+
```

**Open.** The resource currently fails the policy. Created when a new failure is first detected. Remains open as long as the resource continues to fail on subsequent scans, with `last_detected` updated each time.

**Resolved.** The resource now passes the policy. The engine marks a violation as resolved when the `(resource_id, policy_id)` pair is no longer in the current scan's failure set. `resolved_at` records the resolution timestamp. `first_detected` is preserved for historical tracking.

**Exception.** An approved waiver overrides the violation status. The resource may still fail the policy, but the violation is acknowledged and accepted. Exceptions have an optional `expires_at` timestamp. When an exception expires, the violation reverts to open status.

## Deduplication

The `violations` table uses MERGE semantics keyed on `(resource_id, policy_id)`. This means:

- **One row per resource-policy pair.** A table that violates three policies produces three violation rows. The same table violating the same policy across five consecutive scans produces one row with updated `last_detected`.
- **`first_detected` is immutable.** Set on the initial insert and never overwritten. This enables violation age tracking -- "this critical violation has been open for 47 days."
- **`last_detected` updates every scan.** Confirms the violation is still active. A stale `last_detected` indicates the resource may have been removed from the inventory.

The `scan_results` table provides the append-only audit trail. Every `(resource, policy)` evaluation on every scan is recorded there. `violations` is the deduplicated current-state view; `scan_results` is the complete history.

## Resolution Scoping

When the engine resolves violations (marking open violations as resolved because they no longer appear in the failure set), resolution is scoped to the current scan's metastore. Scanning metastore A does not resolve violations from metastore B. This prevents cross-metastore interference in multi-metastore deployments.

## Owner Attribution

Every violation carries an `owner` field. The engine populates this from:

1. The UC metadata `owner` field on the resource (set by Unity Catalog for tables, schemas, catalogs).
2. The `owner` tag on the resource (for resources that use tags for ownership).

Owner attribution enables per-owner violation digests, accountability dashboards ("which owners have the most open critical violations?"), and targeted notification delivery.

## Exceptions

Exceptions are time-bounded waivers stored in the `exceptions` table. Each exception links a `resource_id` and `policy_id` to an approval record:

| Field | Description |
|---|---|
| `exception_id` | Unique identifier |
| `resource_id` | The resource being exempted |
| `policy_id` | The policy being waived |
| `approved_by` | Person who approved the exception |
| `justification` | Documented reason for the waiver |
| `approved_at` | When the exception was approved |
| `expires_at` | When the exception expires (null = indefinite) |
| `active` | Whether the exception is currently active |

During violation merge, the engine checks for active, non-expired exceptions. Violations with matching exceptions are set to `exception` status instead of `open`. When an exception expires or is deactivated, the next scan reverts the violation to `open`.

Exceptions enable governance teams to acknowledge known issues with documentation and time limits rather than suppressing violations permanently.

## Compliance Trends

The `scan_summary` table captures posture metrics at scan time:

- Total resources scanned
- Open, resolved, and exception violation counts
- Severity breakdown (critical, high, medium, low)
- Compliance percentage (resources with zero open violations / total resources)

The `v_compliance_trend` view adds LAG-based deltas and direction indicators:

```
scan_date  | compliance_pct | delta  | direction | critical_open | delta
2026-04-01 | 82.3%          | +1.2%  | improving | 8             | -2
2026-03-25 | 81.1%          | -0.4%  | declining | 10            | +1
2026-03-18 | 81.5%          | +0.8%  | improving | 9             | -3
```

Rolling averages across 30, 60, and 90-day windows smooth out scan-to-scan noise and reveal long-term posture trends. This view answers the question executives ask most: "is it getting better or worse?"
