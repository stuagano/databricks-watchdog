# p-watchdog-exceptions — Exception Management Workflows

**Date:** 2026-04-14
**Status:** ✅ Superseded by [p-customer-catalog(P3)](./p-customer-catalog\(P3\).md) gap-1 (2026-05-12)
**Branch:** `proposals/stuart-handoff/p-watchdog-exceptions`
**Dependencies:** `p-watchdog` deployed (provides `platform.watchdog.violations` and `exceptions` tables, scan pipeline)

> **Superseded — see [`docs/customer-catalog/exceptions-workflow.md`](../docs/customer-catalog/exceptions-workflow.md).**
>
> This proposal's design was *notebook-based* approve/revoke flows. The Customer Catalog implementation instead routes the same lifecycle through a FastAPI + React UI that writes directly to the same `{watchdog_catalog}.watchdog.exceptions` Delta table via the SQL Statement Execution API. Net result:
>
> - **Same data model** (`exceptions` table schema, scan-pipeline lifecycle, expiry semantics) — kept compatible.
> - **Different surface** — approvers click through the catalog UI instead of opening notebooks. Better discoverability and a real audit trail (`x-forwarded-user`).
> - **Different ownership** — exception lifecycle is owned by the Customer Catalog SP, governed by UC `MODIFY` on `watchdog.exceptions` per the SOD decision in gap-1.
>
> Keeping this file as historical record of the alternative design considered. Do not implement the notebook flows — they would conflict with the catalog UI for the same `exceptions` rows.

---

## Problem

Watchdog flags every resource that violates a policy. In a real platform, not every violation is remediable immediately — a legacy dataset might violate a retention policy but be in active use for a regulatory audit, a job might run on an outdated runtime because its library pinned it, a service principal might temporarily need broader access while a migration runs. Without a formal exception workflow, teams have two bad options: ignore violations (pollutes the dashboard with noise and trains people to dismiss alerts) or remediate everything on Watchdog's timeline (creates operational friction that reduces adoption).

Exceptions make governance sustainable. They turn "violation you're ignoring" into "known risk with an owner, a justification, and an expiry."

## What this adds

Two notebook-based workflows for approving and revoking exceptions, integrated into the scan pipeline so exception status flows automatically into violation records and dashboard views.

### Approve exception (`approve_exception.py`)

A Databricks notebook that walks an approver through:

1. **Select violation** — look up by `violation_id`, or filter by resource + policy + owner.
2. **Review violation** — shows policy text, severity, resource classification, when first detected.
3. **Classify exception type** — `planned_remediation` (fix is scheduled), `by_design` (policy doesn't apply to this resource), `risk_accepted` (known risk, owner sign-off), `migration_window` (temporary during system change).
4. **Set expiry** — required for all except `by_design`. Maximum expiry enforced by policy type: `migration_window` caps at 30 days, `planned_remediation` caps at 90 days, `risk_accepted` requires data steward approval and caps at 180 days.
5. **Enter justification** — free text, stored in `exceptions.justification`. Required field.
6. **Confirm and write** — inserts to `platform.watchdog.exceptions` and updates `violations.status = 'exception'`.

### Revoke exception (`revoke_exception.py`)

Revokes an active exception before its expiry. Updates `violations.status` back to `open` and appends to `audit_log` with revocation reason. Supports bulk revocation by resource (e.g., "revoke all exceptions on this dataset after migration completes").

### Scan pipeline integration

The daily scan pipeline checks active exceptions before writing to `violations`. For each `(resource_id, policy_id)` pair:
- If an active, non-expired exception exists → `status = 'exception'`, violation is suppressed from the actionable queue
- If an exception exists but has expired → `status = 'open'`, violation reappears, notification fires
- If no exception → normal `open`/`resolved` lifecycle

Expiry handling is automatic — no manual step to "close" an exception when it expires. The next scan after expiry re-evaluates the resource and updates status accordingly.

### Ad-hoc task notebooks

| Notebook | Purpose |
|----------|---------|
| `bulk_revoke_by_resource.py` | Revoke all exceptions on a resource (e.g., after migration completes) |
| `list_expiring_exceptions.py` | Show exceptions expiring within N days — useful before compliance reviews |
| `exception_audit_report.py` | Pull full exception history for a resource or policy — for external audit requests |

## Data model

### `exceptions` table (in `platform.watchdog`)

| Column | Type | Notes |
|--------|------|-------|
| `exception_id` | string | UUID |
| `violation_id` | string | FK to `violations.violation_id` |
| `resource_id` | string | Denormalized for query convenience |
| `policy_id` | string | Denormalized |
| `exception_type` | string | `planned_remediation`, `by_design`, `risk_accepted`, `migration_window` |
| `justification` | string | Required, free text |
| `approved_by` | string | `current_user()` at approval time |
| `approved_at` | timestamp | |
| `expires_at` | timestamp | Null for `by_design`; required otherwise |
| `revoked_at` | timestamp | Null if still active |
| `revoked_by` | string | Null if still active |
| `revocation_reason` | string | Null if still active |

### Audit log entries

Every exception lifecycle event (create, revoke, expiry re-open) writes a structured entry to `platform.watchdog.audit_log`:

```json
{
  "event_type": "exception_approved",
  "exception_id": "...",
  "violation_id": "...",
  "resource_id": "...",
  "policy_id": "...",
  "approved_by": "jsmith@customer.com",
  "exception_type": "planned_remediation",
  "expires_at": "2026-07-15T00:00:00Z",
  "timestamp": "2026-04-16T09:22:00Z"
}
```

## Access control

| Action | Who can do it |
|--------|--------------|
| Approve `planned_remediation`, `migration_window` | Resource owner or domain steward |
| Approve `risk_accepted` | Data stewards only |
| Approve `by_design` | Data stewards only (permanent — requires highest bar) |
| Revoke any exception | Original approver, data stewards, platform admins |
| View exceptions | Any user with `SELECT` on `platform.watchdog` |

Access enforced via notebook widget that checks `current_user()` against Entra group membership. Not a hard technical gate (notebook runner can bypass), but provides audit accountability.

## File structure

```
bundles/watchdog-bundle/
└── notebooks/
    ├── approve_exception.py         — approval workflow (interactive)
    ├── revoke_exception.py          — revocation workflow (interactive)
    ├── bulk_revoke_by_resource.py   — revoke all exceptions on a resource
    ├── list_expiring_exceptions.py  — exceptions expiring within N days
    └── exception_audit_report.py    — full exception history for audit requests
```

## Activation sequence

1. Deploy `p-watchdog` — exceptions table is created by the core bundle's `setup_schema` task.
2. Deploy notebooks to the `platform/watchdog/notebooks` workspace path via `databricks workspace import`.
3. Run `list_expiring_exceptions.py` to confirm it reads the exceptions table without errors.
4. Create a test violation in alpha, run `approve_exception.py` against it, confirm:
   - Record appears in `platform.watchdog.exceptions`
   - `violations.status` updates to `exception`
   - Next scan keeps status as `exception`
5. Manually expire the test exception (`UPDATE ... SET expires_at = current_timestamp() - interval 1 hour`) and re-run the scan — confirm status reverts to `open`.
6. Establish governance policy on who can approve `risk_accepted` exceptions — add to operator runbook.

## Code-ready defaults

| Decision | Default to code with | When to revisit |
|----------|---------------------|----------------|
| `migration_window` max expiry | 30 days | Hard-coded constant `MAX_EXPIRY_MIGRATION_WINDOW = 30` — easy to change |
| `planned_remediation` max expiry | 90 days | `MAX_EXPIRY_PLANNED_REMEDIATION = 90` |
| `risk_accepted` max expiry | 180 days | `MAX_EXPIRY_RISK_ACCEPTED = 180` |
| `by_design` re-review | Annual renewal — expires after 365 days, must be re-approved | Conservative default; change to permanent if compliance team approves |
| `risk_accepted` notification | Stub the call to the ACS pipeline with a `# TODO` — don't block notebook on it | Wire up after `p-notifications` is deployed |

Expiry caps are constants at the top of `approve_exception.py`. Changing them requires no structural code change. V4C ships with these defaults; the customer compliance team reviews and adjusts values in a follow-on PR.

## TODOs (post-coding)

- [ ] Confirm max expiry periods with the customer compliance: update constants in `approve_exception.py`
- [ ] Decide `by_design` re-review cadence: annual renewal (default) vs. permanent — update expiry logic
- [ ] Wire up `risk_accepted` notification to ACS pipeline after `p-notifications` is deployed
- [ ] Add `exception_status` panel to the compliance dashboard (`p-watchdog-dashboards`)
