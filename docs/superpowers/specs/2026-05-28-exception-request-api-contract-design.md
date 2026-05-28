# Exception Request API Contract

**Status:** Proposed (codifies existing implementation in `ontos-adapter`)
**Date:** 2026-05-28
**Source:** Imported from an external industry proposal (`p-customer-catalog`) describing a catalog UI that owns the exception lifecycle via a FastAPI-fronted Delta write path.

---

## Problem

The exception lifecycle (approve, revoke, list, summarize) is implemented in `ontos-adapter/src/watchdog_governance/` — a FastAPI router plus a `GovernanceProvider` protocol with a concrete Watchdog-backed provider. But the **API contract** isn't documented as a portable spec. Anyone building an alternate catalog UI (their own React app, a chatbot surface, a notebook front-end) has to read `ontos-adapter` source to discover:

- What endpoints exist
- The request/response shapes
- The auth model (where the approver identity comes from)
- The audit fields that need to flow through
- What back-pressure / error semantics callers should expect

This is fine for a reference implementation; it doesn't scale to "any catalog UI". The exception notebooks shipped earlier (`engine/notebooks/approve_exception.py`, `revoke_exception.py`) were the original surface — they were removed once the catalog/FastAPI surface took over (2026-05-28). The notebook deletion left no central contract document behind.

## Goal

Codify the existing contract — endpoints, request/response shapes, auth, audit — as a portable API spec, so that:

1. Other catalog UIs can implement against it without copying `ontos-adapter` code.
2. Other backend providers can implement the `GovernanceProvider` protocol against alternate stores (test fixtures, non-Delta backends, multi-tenant proxies).
3. Changes to the contract have a single source of truth that drift can be detected against.

This is a contract / interface spec, not a new design. The reference implementation already exists.

## Non-Goals

- **UI/UX guidance for the approver workflow.** Each catalog UI owns that.
- **Approval business logic.** Who can approve what at what severity, what justification length is required, what max-expiry should be — these are policy decisions, configurable in the provider, not part of the wire contract.
- **Synchronous re-scan after approval.** Out of scope; the scan pipeline picks up exception state on its next run.
- **New endpoints.** Document what exists today. Additions go through their own spec.

---

## Endpoints

All endpoints live under `/exceptions` (or wherever the host app mounts the router).

### `GET /exceptions`

Query params:

| Param | Type | Default | Notes |
|---|---|---|---|
| `active` | bool | `true` | Only return exceptions whose `expires_at` is in the future and `revoked_at` is null |
| `expiring_soon` | bool | `false` | Restrict to exceptions expiring in the next 7 days |
| `resource_id` | string | null | Filter to a specific resource |

Response: array of exception records (shape below).

### `GET /exceptions/summary`

No params. Returns aggregate counts:

```json
{
  "active": 47,
  "expiring_in_7_days": 6,
  "expired_unrevoked": 0,
  "by_severity": { "critical": 2, "high": 11, "medium": 19, "low": 15 }
}
```

### `GET /exceptions/resource/{resource_id}`

Returns the array of exceptions (active or not) that pertain to a single resource.

### `POST /exceptions`

Status: `201 Created`. Request body:

```json
{
  "resource_id": "metastore_id.catalog.schema.table",
  "policy_ids": ["POL-S009", "POL-MED-005"],
  "justification": "Required free text; minimum length enforced by provider.",
  "expires_days": 30
}
```

- `resource_id` — 4-part watchdog resource identifier.
- `policy_ids` — array; one POST creates one record per policy with the same justification and expiry.
- `justification` — required, free text. Provider enforces minimum length and absence-of-junk heuristics.
- `expires_days` — integer; provider enforces max-expiry per policy severity (e.g., critical caps at 30, high at 90).

Auth: approver identity is resolved by the host app's auth middleware (see Auth below). The route handler reads it from a `get_current_user` dependency and passes it as `approved_by` to the provider — the client does **not** send approver identity.

Response: array of created exception records.

### `DELETE /exceptions/{exception_id}`

Revokes a single active exception. Identity of the revoker resolved from the auth dependency; recorded as `revoked_by` in the audit trail. Returns the updated exception record.

Errors:
- `404` — exception_id does not exist
- `409` — exception is already revoked or expired

### `POST /exceptions/bulk-revoke-expired`

Sweeps expired-but-unrevoked exceptions and revokes them. Useful for nightly hygiene jobs. Returns count revoked.

## Exception Record Shape

```json
{
  "exception_id": "uuid",
  "resource_id": "metastore_id.catalog.schema.table",
  "policy_id": "POL-S009",
  "justification": "free text",
  "approved_by": "alice@example.com",
  "approved_at": "2026-05-28T14:00:00Z",
  "expires_at": "2026-06-27T14:00:00Z",
  "revoked_by": null,
  "revoked_at": null,
  "severity": "high"
}
```

Stored in `{watchdog_catalog}.watchdog.exceptions`. Schema is part of the watchdog deployment; not negotiable from the UI side.

## Auth

The router does not authenticate. It expects the host app to populate `Depends(get_current_user)` from upstream auth — `x-forwarded-user`, OAuth bearer token, OBO token, whatever. The reference implementation uses the `x-forwarded-user` header set by Databricks Apps. Any host that can populate a user identifier can host the router.

**Audit fields the host app is responsible for guaranteeing:**

- `approved_by` and `revoked_by` are **never** taken from the client. They come from the trusted server-side auth context only.
- The host app's auth layer must reject unauthenticated requests before the dependency runs.

## GovernanceProvider Protocol

Backing the router is a Python `Protocol` (see `ontos-adapter/src/watchdog_governance/provider.py`) with the following methods relevant to exceptions:

| Method | Purpose |
|---|---|
| `list_exceptions(ExceptionFilters)` | Backs `GET /exceptions` |
| `exceptions_summary()` | Backs `GET /exceptions/summary` |
| `exceptions_for_resource(resource_id)` | Backs `GET /exceptions/resource/{resource_id}` |
| `approve_exceptions(...)` | Backs `POST /exceptions` |
| `revoke_exception(exception_id, *, revoked_by)` | Backs `DELETE /exceptions/{exception_id}` |
| `bulk_revoke_expired(*, revoked_by)` | Backs `POST /exceptions/bulk-revoke-expired` |

A provider can be implemented against:

- The reference Watchdog Delta provider (`providers/watchdog.py`).
- A test fixture (in-memory dict) for UI development without a live workspace.
- A future remote/proxy implementation that wraps another deployment.

## Non-Watchdog Backends

The protocol is intentionally minimal so an alternate backend can implement it without inheriting Delta-specific assumptions. The only data-model commitments callers depend on:

- An exception is uniquely identified by `(resource_id, policy_id, approved_at)` — `exception_id` is a stable surrogate.
- `expires_at` is the single field driving `active` vs expired.
- Bulk-revoke is allowed to set `revoked_at = expires_at` (not `now()`) when sweeping expired-unrevoked rows. This keeps the audit trail honest about when the exception actually lapsed.

## Dependencies

- `ontos-adapter` reference implementation — already exists.
- `{watchdog_catalog}.watchdog.exceptions` Delta table — provisioned by the watchdog deployment.
- Host app provides auth + dependency injection for `get_current_user`.

## Risks

| Risk | Mitigation |
|---|---|
| Catalog UIs trust client-sent `approved_by` field | Contract explicitly forbids it — server resolves identity. Reference implementation does not accept the field in the request schema. |
| Provider implementations drift on max-expiry / justification rules | Provider is the enforcement boundary; document the rules in the provider doc, not the contract. The contract guarantees `409` on policy-violating writes. |
| Wire schema changes break existing UIs | Schema additions are non-breaking when fields are optional and new endpoints are additive. Breaking changes require a contract-version bump. |
| Bulk-revoke recorded at wrong timestamp loses audit value | Codified above: bulk-revoke sets `revoked_at = expires_at`, not `now()`. |

## Order of Operations

This spec mainly documents what exists. The work is:

1. Write `docs/guide/reference/exception-api.md` capturing the contract above as user-facing reference.
2. Annotate the protocol in `ontos-adapter/src/watchdog_governance/provider.py` with version + contract pointer.
3. Add a contract drift test: a thin schema-check that fixtures the request/response shapes and fails CI if `ontos-adapter` routers diverge.
4. Cross-reference: in `engine/notebooks/` directory README, note that the notebook flow was removed in favor of this contract.

## Estimated Effort

| Phase | Effort |
|---|---|
| Reference doc | 0.5 days |
| Provider annotation + version bump | 0.25 days |
| Contract drift test | 0.5 days |
| **Total** | **~1.25 days** |
