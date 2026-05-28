# p-watchdog-docs — Operator and Consumer Documentation

**Date:** 2026-04-14
**Status:** ✅ Superseded — implemented in the standalone watchdog repo (2026-05-13)
**Branch:** `proposals/stuart-handoff/p-watchdog-dashboards`
**Dependencies:** `p-watchdog` deployed and running (docs describe a live system, not a hypothetical one)

> **Superseded — see `~/Documents/Projects/databricks-watchdog/docs/` (standalone repo `CustomerDataPlatform/watchdog`).**
>
> Watchdog documentation moved to the standalone repo after extraction. Substantial coverage of the operator + consumer audiences exists there:
>
> **Operator-facing**
> - `docs/architecture-guide.md` ↔ proposed `operator/architecture.md`
> - `docs/deployment-playbook.md` ↔ proposed `operator/deployment-guide.md`
> - `docs/guide/troubleshooting.md` + `docs/guide/how-to/*` ↔ proposed `runbook-scan.md`, `runbook-policies.md`, `runbook-domains.md`, `runbook-onboarding.md`, `runbook-incidents.md`
>
> **Consumer-facing**
> - `docs/guide/concepts/{violations,policies,ontology,architecture}.md` ↔ proposed `consumer/understanding-violations.md`, `dashboard-guide.md`
> - `docs/guide/how-to/{write-policies,configure-notifications,agent-governance}.md` ↔ proposed `consumer/resolving-violations.md`, `agent-developer-guide.md`
> - `docs/guide/reference/{policy-schema,mcp-tools,guardrails-tools,rule-types,tables,ontology-classes,cli}.md` — reference material the original proposal didn't enumerate but rounds out the surface
>
> **Gaps that remain** (could still be useful as targeted follow-ups in the standalone repo, not here):
> - `runbook-teams.md` — notification routing add/update/test procedure (covered partially by `how-to/configure-notifications.md`)
> - `runbook-exceptions.md` — approve/revoke flow (the catalog UI overtook this from the notebook design)
> - Consumer `faq.md` — common questions with answers
>
> Keep this file as historical record of the doc plan. Do not duplicate the doc tree into `customer-infra/docs/` — that would fork content against the "no longer syncing into customer-infra" decision. customer-specific doc lives at `~/Documents/Projects/databricks-watchdog/customer/customer/` (today: ontologies + policies; future: deployment notes if needed).

## Problem

When Stuart hands off the Watchdog deployment, the the customer V4C team inherits a system they didn't build. Without runbooks, the first operational event — a scan failure, a false-positive violation, a team member onboarding — becomes a debugging session. Without consumer docs, data owners receiving violation notifications don't know what they're looking at or what to do.

Documentation here is an operational dependency, not a nice-to-have. It determines whether Watchdog gets adopted or ignored.

## Two audiences, two doc sets

### Operator docs (`docs/operator/`)

For platform engineers and data platform admins who deploy, configure, and maintain Watchdog.

| Document | Contents |
|----------|---------|
| `architecture.md` | How Watchdog works: data model, scan pipeline, policy engine, resource crawlers, notification flow. The mental model operators need before touching anything. |
| `deployment-guide.md` | Step-by-step deployment order across all components: Terraform (Watchdog SP + grants), then DAB bundle (alpha → beta → live). Prerequisites, environment variable checklist, rollback steps. |
| `runbook-scan.md` | What to do when a scan fails, runs long, or produces unexpected results. Includes: check job run logs, query `audit_log` for errors, force a manual re-scan, interpret partial results. |
| `runbook-policies.md` | How to add, modify, disable, and retire policies. When to use YAML vs. MCP tools. How to test a policy in alpha before promoting to live. How to roll back a policy change. |
| `runbook-teams.md` | How to add a new team to the notification routing table, update an existing team's email, and confirm routing is working. |
| `runbook-domains.md` | How to register a new governance domain, assign an owner, and add domain-specific policies. |
| `runbook-exceptions.md` | How to approve an exception (via notebook or MCP tool), set expiry, revoke early. When to escalate vs. approve. |
| `runbook-incidents.md` | Playbook for governance incidents: mass false-positive, compromised SP, policy configuration error affecting production. How to quarantine, remediate, and post-mortem. |
| `runbook-onboarding.md` | How to onboard a new workspace: add it to the resource crawler scope, confirm it appears in `resource_inventory`, assign a domain owner, run a first scan. |

### Consumer docs (`docs/consumer/`)

For data owners, analysts, and developers who receive violation notifications and interact with Watchdog results.

| Document | Contents |
|----------|---------|
| `understanding-violations.md` | What a violation means, what it doesn't mean, what happens if you ignore it, how severity is determined. Includes example violations with plain-English explanations. |
| `resolving-violations.md` | How to fix the five most common violation types (missing cost_center tag, no DQM monitor, missing data steward, runtime below threshold, no alert on critical job). Step-by-step for each. |
| `requesting-exceptions.md` | When exceptions are appropriate vs. not. How to submit an exception request (MCP tool or email to data stewards). What happens after you submit. How long exceptions last. |
| `dashboard-guide.md` | How to read the compliance dashboard. What each panel shows. How to filter to your domain or your resources. How to export for a compliance review meeting. |
| `agent-developer-guide.md` | How to use Watchdog MCP tools in Claude Code or Cursor. Tool reference for `validate_query`, `safe_columns`, `suggest_safe_tables`. How to interpret blocked results and find alternatives. |
| `faq.md` | Common questions: "Why is my table flagged when it's been fine for months?", "Who approved this exception?", "Can I query this table for my ML model?", "How do I add my team to receive notifications?" |

## What makes docs stay current

Docs rot when they're written once and never updated. Three practices prevent this:

1. **Runbooks are verified at handoff.** Before Stuart hands off to V4C, each runbook is executed against a live alpha deployment to confirm it works. Any step that fails gets fixed before merge.

2. **Policy and schema changes trigger doc review.** The PR template for `policies/*.yml` and schema changes includes a checkbox: "I have reviewed `docs/operator/` for accuracy." Not enforced by CI, but visible in PR review.

3. **Consumer docs are written from violation emails.** The `understanding-violations.md` and `resolving-violations.md` documents are written by running the scanner against a test catalog with known violations and documenting what the output says. Grounded in the actual system, not an idealized description of it.

## File structure

```
docs/
├── operator/
│   ├── architecture.md
│   ├── deployment-guide.md
│   ├── runbook-scan.md
│   ├── runbook-policies.md
│   ├── runbook-teams.md
│   ├── runbook-domains.md
│   ├── runbook-exceptions.md
│   ├── runbook-incidents.md
│   └── runbook-onboarding.md
└── consumer/
    ├── understanding-violations.md
    ├── resolving-violations.md
    ├── requesting-exceptions.md
    ├── dashboard-guide.md
    ├── agent-developer-guide.md
    └── faq.md
```

## Activation sequence

1. Deploy `p-watchdog` to alpha and run at least one full scan — docs must describe real behavior, not anticipated behavior.
2. Write `architecture.md` first — it becomes the reference all other docs link back to.
3. Write `deployment-guide.md` by executing a fresh deployment and documenting each step as you go.
4. Write runbooks by intentionally triggering each scenario (kill a scan job, create a false-positive policy, add a team) and capturing the resolution steps.
5. Write consumer docs by generating real violation notifications and documenting how a non-technical recipient should respond.
6. Have at least one V4C team member who didn't write the docs follow each runbook cold — fix anywhere they get stuck.

## TODOs

- [ ] Identify the V4C team member who will own Watchdog operations — they should review all runbooks before merge
- [ ] Confirm escalation path for governance incidents: who gets paged, what's the SLA for critical violations
- [ ] Decide where docs live: in this repo (`customer-infra/docs/`) or in a separate internal knowledge base — if the latter, determine access controls
- [ ] Write `agent-developer-guide.md` after `p-ai-devkit` (Watchdog Guardrails) is deployed — it needs real MCP tool output to document against
