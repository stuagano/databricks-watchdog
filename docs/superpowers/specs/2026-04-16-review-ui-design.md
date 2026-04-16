# Review UI Design Spec

**Date:** 2026-04-16
**Status:** Approved
**Approach:** Full Ontos Integration (Approach 1)

---

## Overview

A remediation review UI built into the ontos-adapter, giving a central governance team a split-panel inbox to review AI-generated remediation proposals and a funnel-centric dashboard to track the remediation program.

## Users

A central governance team that reviews all proposals across the org. Single shared queue — no per-domain routing or role-based filtering.

## Design Decisions

- **Split-panel inbox** over centered card or queue-first — the team needs to see what's ahead and jump around
- **Diff-forward detail** over SQL-forward or context-forward — reviewers validate "what will change?" not "what's the SQL?"
- **Severity-first, confidence-tiebreaker ordering** — critical + low-confidence proposals bubble to the top
- **Funnel-centric dashboard** over KPI cards — maps directly to `v_remediation_funnel` and answers "how's the program doing?"
- **Optimistic UI updates** — proposals leave the pending list immediately on action; rollback on API failure

---

## Backend: API Router

New file: `ontos-adapter/src/watchdog_governance/routers/remediation.py`

Follows the same pattern as `routers/exceptions.py`. Registered in `router.py`.

### Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/governance/remediation/funnel` | GET | Funnel summary — reads `v_remediation_funnel` |
| `/api/governance/remediation/agents` | GET | Agent effectiveness — reads `v_agent_effectiveness` |
| `/api/governance/remediation/proposals` | GET | List proposals. Query params: `status` (default: `pending_review`), `limit`, `offset`. Sorted by severity desc, confidence asc |
| `/api/governance/remediation/proposals/{id}` | GET | Single proposal with full context: violation details, pre-state, proposed SQL, agent info, review history |
| `/api/governance/remediation/proposals/{id}/review` | POST | Submit review decision |
| `/api/governance/remediation/reviewer-load` | GET | Reviewer load stats — reads `v_reviewer_load` |

### Review Action Request Body

```json
{
  "decision": "approved" | "rejected" | "reassigned",
  "reasoning": "string",
  "reassigned_to": "string (required if decision=reassigned)"
}
```

### Server-Side Joins

The `/proposals` endpoint joins `remediation_proposals` with `violations` and `policies` to return enriched rows:

- `proposal_id`, `status`, `proposed_sql`, `confidence`, `context_json`
- `violation_id`, `resource_id`, `resource_name`, `resource_type`
- `policy_id`, `policy_name`, `severity`, `domain`
- `agent_id`, `agent_version`
- `pre_state` (current tags/state before proposed change)
- `created_at`

The `/proposals/{id}` detail endpoint additionally includes:

- Full `context_json` with agent reasoning and citations
- `review_history[]` — prior review records for this proposal (reassignment trail)

### Integration with Engine

The review endpoint calls the existing pure functions from `engine/src/watchdog/remediation/review.py`:

- `approve_proposal()` → writes updated proposal + review record to Delta
- `reject_proposal()` → writes updated proposal + review record to Delta
- `reassign_proposal()` → writes review record to Delta (status stays `pending_review`)

---

## Frontend: Components

### RemediationDashboard.tsx

Route: `/governance/remediation`

Three sections:

1. **Funnel visualization** — five stacked bars narrowing from violations → proposed → pending → applied → verified. Each bar is clickable (navigates to inbox filtered by that status). Data from `/api/governance/remediation/funnel`.

2. **Agent effectiveness cards** — one card per registered agent showing: proposal count, verification rate, average confidence. Data from `/api/governance/remediation/agents`.

3. **Reviewer load table** — pending/approved/rejected counts per reviewer. "Review now" button navigates to inbox. Data from `/api/governance/remediation/reviewer-load`.

### RemediationInbox.tsx

Route: `/governance/remediation/inbox`

Split-panel layout:

**Left panel (30% width):**
- Scrollable list of proposals
- Each row: resource name (truncated), severity badge, confidence pill, agent name
- Active item highlighted with accent border
- Filter tabs at top: Pending | Approved | Applied | All
- Sorted by severity desc, then confidence asc within each severity tier

**Right panel (70% width):**
- Header: resource name, policy name, severity badge, agent + version, confidence score
- **Diff section** (primary content): before/after side-by-side. Current state on left (red for missing/old values), proposed state on right (green for new values). Styled like a code diff.
- SQL preview: monospace block with the proposed `ALTER TABLE` statement
- Context: collapsible section with agent reasoning from `context_json`
- Review history: timeline of prior reviews (for reassigned proposals)
- **Action bar** (sticky bottom): three buttons:
  - Reject — opens dialog with required reasoning textarea
  - Reassign — opens dialog with user input field + optional reasoning
  - Approve — opens dialog with optional comment

### ProposalDiff.tsx (shared sub-component)

Reusable before/after diff renderer. Props: `preState: Record<string, string>`, `proposedState: Record<string, string>`. Renders two-column comparison with additions in green, removals in red. Used in RemediationInbox detail panel.

---

## Data Flow & State Management

**State per view:** Each view manages its own state via `useState` hooks. No global store needed (consistent with existing views).

**Inbox state:**
- `proposals[]` — the queue list (left panel), fetched from `/proposals?status=<tab>`
- `selectedProposal` — full detail for active item, fetched from `/proposals/{id}`
- `activeTab` — current filter tab (pending | approved | applied | all)

**Optimistic updates:** On approve/reject, the proposal immediately removes from the pending list. On API failure, it snaps back with an error toast via `useToast`.

**Refresh strategy:**
- Dashboard: fetch on mount + manual refresh button
- Inbox: fetch on mount + after each review action. No polling.

---

## Routes, Feature Flags & i18n

### Routes

Added to `routes.watchdog.tsx`:

```typescript
{ path: 'remediation',       element: <RemediationDashboard /> },
{ path: 'remediation/inbox', element: <RemediationInbox /> },
```

### Feature Flag

Added to `features.watchdog.ts`:

```typescript
{
  id: 'watchdog-remediation',
  name: 'Remediation Review',
  path: '/governance/remediation',
  description: 'Review and approve AI-generated remediation proposals',
  icon: 'Wrench',
  group: 'govern',
  maturity: 'beta',
}
```

### i18n

New keys added to `watchdog.json` for: funnel stage labels, review action labels, confidence labels, status badges, confirmation dialog text, empty state messages.

---

## Testing

### Backend

Unit tests in `tests/unit/test_remediation_router.py`:

- Funnel endpoint returns correct stage counts
- Proposals endpoint returns enriched rows with joins
- Proposals endpoint respects status filter and sort order
- Proposal detail includes review history
- Review action endpoint validates decision enum
- Review action calls correct pure function (approve/reject/reassign)
- Review action rejects proposals not in `pending_review` status
- Reviewer load endpoint returns per-reviewer stats

### Frontend

Component tests following existing patterns:

- Inbox renders proposals sorted by severity then confidence
- Clicking a proposal in left panel loads detail in right panel
- Filter tabs switch the displayed proposals
- Approve triggers API call and removes proposal from pending list
- Reject requires reasoning before submission
- Reassign requires `reassigned_to` field
- Optimistic update rolls back on API failure with error toast
- Dashboard funnel renders correct counts from API
- Funnel bars navigate to inbox with correct filter on click

---

## Files to Create/Modify

### New Files
- `ontos-adapter/src/watchdog_governance/routers/remediation.py` — API router
- `ontos-adapter/frontend/src/views/RemediationDashboard.tsx` — Dashboard view
- `ontos-adapter/frontend/src/views/RemediationInbox.tsx` — Inbox view
- `ontos-adapter/frontend/src/components/ProposalDiff.tsx` — Shared diff component
- `tests/unit/test_remediation_router.py` — Backend tests

### Modified Files
- `ontos-adapter/src/watchdog_governance/router.py` — Register remediation router
- `ontos-adapter/frontend/src/routes.watchdog.tsx` — Add remediation routes
- `ontos-adapter/frontend/src/config/features.watchdog.ts` — Add feature flag
- `ontos-adapter/frontend/src/i18n/en/watchdog.json` — Add i18n keys
- `ontos-adapter/src/watchdog_governance/models.py` — Add Pydantic models for proposals/reviews
