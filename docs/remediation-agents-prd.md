# PRD: Agentic Remediation Framework for Unity Catalog Governance

**Working title:** Watchdog Remediation Agents (a.k.a. "Watchdog Autopilot")
**Status:** Core remediation pipeline is implemented (dispatcher, proposal review queue, applier with dry-run, verifier, 4 reference agents). This PRD informed the design; see `engine/src/watchdog/remediation/` for the implementation.
**Owner:** TBD
**Last updated:** 2026-04-13
**Reference:** [7-Eleven's AI documentation migration on Databricks](https://www.databricks.com/blog/automating-data-documentation-ai-how-7-eleven-bridged-metadata-gap)

> **Platform update (April 2026):** The DocAgent proposed in this PRD for auto-generating table/column documentation is now **superseded** by Databricks' native [AI-Generated Documentation](https://www.databricks.com/blog/announcing-public-preview-ai-generated-documentation-databricks-unity-catalog) feature (Public Preview). The platform auto-generates table and column descriptions via LLM in Catalog Explorer. The remaining agents (StewardAgent, CostCenterAgent, GrantMigrationAgent, ClassificationAgent) are still unique to Watchdog and not covered by the platform. Additionally, [AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/overview-serving-endpoints) now provides native PII detection, rate limiting, and payload logging — the remediation framework should leverage these rather than rebuilding them.

---

## 0. TL;DR

7-Eleven proved that an agentic LLM pipeline (Llama 4 Maverick on Mosaic AI Model Serving) can close one governance gap — missing table/column documentation — in days instead of months. That pattern is generalizable to every other class of Unity Catalog metadata gap. Watchdog has the catalog of gaps (46 policies across 5 domains) but today only *reports* them; it cannot *fix* them.

This PRD proposes a Remediation Agent framework that sits on top of Watchdog's violation store, dispatches the right LLM-backed agent per violation type, applies fixes under human review, and uses the next Watchdog scan as the verification oracle. The result is a closed loop: **detect → remediate → verify → measure**.

---

## 1. Problem

Unity Catalog enforces governance at runtime, and Watchdog measures compliance posture — but the gap between "violation detected" and "violation fixed" is still manual. Customers receive a punch list and are left to translate each violation into SQL, tag edits, grant migrations, or documentation by hand.

7-Eleven demonstrated a better pattern for one specific gap (documentation migration from Confluence into UC comments), but:

1. The pattern has not been generalized to other metadata gaps
2. There is no shared framework that lets you plug in a new agent for each new violation class
3. There is no audit-grade link between the policy that flagged a violation, the LLM that proposed a fix, the reviewer who approved it, and the scan that verified it
4. There is no reusable review queue, verification loop, or evidence trail

Enterprises need a framework where:

1. A violation is detected (by Watchdog or any policy engine)
2. An appropriate remediation agent is dispatched
3. The agent gathers context, proposes a fix, applies it with human review
4. The fix is verified by the next scan
5. Compliance posture measurably improves — with an auditable artifact every regulator can trace

---

## 2. Goals

- **G1.** Generalize the 7-Eleven documentation pipeline into a reusable **Remediation Agent** abstraction
- **G2.** Ship **3 to 5 reference agents** covering the highest-volume Watchdog violation types
- **G3.** Integrate tightly with Watchdog's violation catalog — agents are dispatched *from* violations and verified *by* the next scan
- **G4.** Provide **human-in-the-loop review** with batch approval, diff preview, and rollback
- **G5.** Produce an **auditable compliance artifact**: every change links violation to evidence to LLM output to reviewer to SQL to verification
- **G6.** Make the compliance trend line move — measurably — inside a single quarter at the pilot customer

## 3. Non-goals

- Not building a new policy engine (Watchdog's YAML rules stay)
- Not replacing Unity Catalog enforcement (ABAC, tag policies, row filters continue to run)
- Not a general-purpose text-to-SQL tool
- Not fully autonomous in v1 — every proposal requires human review
- Not a replacement for Databricks AI-generated comments (this is a superset: cross-source, cross-violation, verified)

---

## 4. Users and Personas

| Persona | Need |
|---|---|
| **Data Steward** | Review and approve batches of AI-generated fixes for their domain |
| **Platform Admin** | Configure which agents run, set guardrails, monitor cost |
| **CDO / Compliance Lead** | See compliance percentage move up; prove remediation to auditors |
| **Regulator / Auditor** | Trace any metadata change back to a policy, a rule, a reviewer, and a timestamp |
| **Agent Author** | Build a new remediation agent against a stable SDK without touching Watchdog internals |

---

## 5. Core Concepts

### 5.1 Remediation Agent

A pluggable unit that closes one class of violation. Contract:

```python
class RemediationAgent:
    handles: list[str]  # policy IDs, e.g. ["POL-Q001", "POL-GEN-003"]
    version: str
    model: str           # e.g. "databricks-llama-4-maverick"

    def gather_context(self, violation: Violation) -> Context: ...
    def propose_fix(self, context: Context) -> Proposal: ...
    def apply(self, proposal: Proposal) -> ApplyResult: ...
    def verify(self, apply_result: ApplyResult) -> VerificationStatus: ...
```

The four-step contract mirrors 7-Eleven's pipeline: discover context, rank and match, generate SQL, execute and verify.

```
                          Agent Lifecycle (per violation)
  ┌──────────────────────────────────────────────────────────────────────┐
  │                                                                      │
  │   ┌─────────────────┐    ┌──────────────┐    ┌──────────────────┐   │
  │   │ gather_context() │    │ propose_fix()│    │     apply()      │   │
  │   │                  │    │              │    │                  │   │
  │   │ • violation      │───▶│ • LLM call   │───▶│ • ALTER TABLE    │   │
  │   │ • schema + tags  │    │ • RAG lookup │    │ • UPDATE tags    │   │
  │   │ • sample rows    │    │ • confidence │    │ • GRANT/REVOKE   │   │
  │   │ • lineage        │    │ • SQL gen    │    │ • pre/post snap  │   │
  │   │ • external docs  │    │ • citations  │    │ • audit log      │   │
  │   └─────────────────┘    └──────────────┘    └────────┬─────────┘   │
  │                                                        │             │
  │                                    ┌───────────────────▼──────────┐  │
  │                                    │         verify()             │  │
  │                                    │                              │  │
  │                                    │ • next Watchdog scan runs    │  │
  │                                    │ • JOIN applied × violations  │  │
  │                                    │ • resolved? → verified ✓     │  │
  │                                    │ • still open? → failed ✗     │  │
  │                                    │ • MLflow eval metrics        │  │
  │                                    └──────────────────────────────┘  │
  └──────────────────────────────────────────────────────────────────────┘
```

### 5.2 Dispatcher

Reads Watchdog's `violations` table, routes each open violation to the agent that declares `handles` for it, and respects per-agent concurrency, cost, and rate limits.

```
                         Dispatcher Routing
  ┌──────────────────────────────────────────────────────────────┐
  │  violations table  (status = open, remediable = true)        │
  │                                                              │
  │  ┌─────────────┬──────────────┬──────────────┬────────────┐  │
  │  │ POL-Q001    │ POL-S001     │ POL-A002     │ POL-C002   │  │
  │  │ POL-GEN-003 │              │              │            │  │
  │  │ Undocumented│ PII no       │ Direct user  │ No cost    │  │
  │  │ Asset       │ steward      │ grants       │ center     │  │
  │  └──────┬──────┴──────┬───────┴──────┬───────┴─────┬──────┘  │
  └─────────│─────────────│──────────────│─────────────│─────────┘
            │             │              │             │
            ▼             ▼              ▼             ▼
  ┌─────────────┐ ┌────────────┐ ┌────────────────┐ ┌──────────────┐
  │  DocAgent   │ │ Steward    │ │ GrantMigration │ │ CostCenter   │
  │             │ │ Agent      │ │ Agent          │ │ Agent        │
  │ Llama 4    │ │ LLM +      │ │ Deterministic  │ │ LLM +        │
  │ Maverick + │ │ AAD/SCIM   │ │ + LLM group    │ │ FinOps       │
  │ Confluence │ │ directory  │ │ suggestion     │ │ mapping      │
  │ /git RAG   │ │            │ │                │ │              │
  └──────┬──────┘ └─────┬──────┘ └───────┬────────┘ └──────┬───────┘
         │              │                │                 │
         ▼              ▼                ▼                 ▼
  ┌──────────────────────────────────────────────────────────────┐
  │              remediation_proposals (Delta)                   │
  │  proposal_id │ violation_id │ agent │ sql │ confidence │ ... │
  └──────────────────────────────────────────────────────────────┘

  Dispatch rules:
  • One agent per violation (first match on handles[])
  • Per-agent concurrency limit (default: 10 concurrent)
  • Per-agent token budget ceiling per scan
  • Idempotent: (violation_id, agent_id, agent_version) → skip if exists
  • Resource lock: only one agent may propose for a resource at a time
```

### 5.3 Review Queue

Delta table of `Proposals` with status transitions. Stewards review in a Databricks App or Lakeview page with diff preview and source citations.

```
                    Review Queue State Machine

                         ┌───────────┐
                         │  pending   │
                         │  _review   │
                         └─────┬─────┘
                               │
                    ┌──────────┼──────────┐
                    │          │          │
                    ▼          │          ▼
             ┌───────────┐    │    ┌───────────┐
             │ rejected  │    │    │ reassigned│──┐
             │           │    │    │           │  │
             │ reason    │    │    │ new owner │  │
             │ logged    │    │    └───────────┘  │
             └───────────┘    │          ▲        │
                              │          └────────┘
                              ▼           (back to pending_review
                       ┌───────────┐      under new steward)
                       │ approved  │
                       └─────┬─────┘
                             │
                             ▼
                       ┌───────────┐
                       │ applied   │
                       │           │
                       │ SQL ran   │
                       │ pre/post  │
                       │ captured  │
                       └─────┬─────┘
                             │
                    ┌────────┴────────┐
                    │  next Watchdog  │
                    │  scan runs      │
                    └────────┬────────┘
                             │
                   ┌─────────┼─────────┐
                   │                   │
                   ▼                   ▼
           ┌─────────────┐    ┌────────────────┐
           │  verified   │    │ verification   │
           │      ✓      │    │ _failed   ✗    │
           │             │    │                │
           │ violation   │    │ re-queued for  │
           │ resolved in │    │ investigation  │
           │ next scan   │    │                │
           └─────────────┘    └───────┬────────┘
                                      │
                                      ▼
                               ┌─────────────┐
                               │  rolled_back│
                               │             │
                               │ pre-state   │
                               │ restored    │
                               └─────────────┘
```

### 5.4 Verification Loop

After a proposal is applied, the next Watchdog scan is the verification oracle. If the violation flips to `resolved`, the proposal is marked `verified`. If not, it is flagged `verification_failed` and re-queued for investigation. This is the key architectural decision: **we reuse the measurement layer as the correctness check, so there is no separate test harness for agent output.**

### 5.5 Evidence Trail

Every proposal persists: source violation, retrieved context (with citations), LLM prompt and response, confidence score, reviewer, applied SQL, pre/post state, verification scan ID. Append-only, auditor-ready.

---

## 6. Architecture

```
                        End-to-End Closed Loop

  ┌─────────────────────────────────────────────────────────────────────┐
  │  DETECT                                                             │
  │                                                                     │
  │  ┌──────────────┐    ┌──────────────────────────────────────────┐   │
  │  │  Watchdog     │───▶│  violations (Delta)                     │   │
  │  │  Daily Scan   │    │  status=open, remediable=true           │   │
  │  │              │    │  46 policies × 5 domains                │   │
  │  └──────────────┘    └──────────────────┬───────────────────────┘   │
  └─────────────────────────────────────────│───────────────────────────┘
                                            │
  ┌─────────────────────────────────────────│───────────────────────────┐
  │  REMEDIATE                              ▼                           │
  │                                                                     │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │  Dispatcher (Workflow Task)                                  │   │
  │  │  • routes by policy_id → agent.handles[]                    │   │
  │  │  • enforces concurrency, cost, rate limits                  │   │
  │  │  • idempotent: skip if (violation, agent, version) exists   │   │
  │  └───┬──────────┬──────────┬───────────┬──────────┬────────────┘   │
  │      │          │          │           │          │                 │
  │      ▼          ▼          ▼           ▼          ▼                 │
  │  ┌────────┐ ┌────────┐ ┌─────────┐ ┌────────┐ ┌──────────────┐    │
  │  │DocAgent│ │Steward │ │CostCtr  │ │Grant   │ │Classification│    │
  │  │        │ │Agent   │ │Agent    │ │Migr.   │ │Agent         │    │
  │  │POL-Q001│ │POL-S001│ │POL-C002 │ │POL-A002│ │UntaggedAsset │    │
  │  └───┬────┘ └───┬────┘ └────┬────┘ └───┬────┘ └──────┬───────┘    │
  │      │          │           │          │             │              │
  │      └──────────┴───────────┴──────────┴─────────────┘              │
  │                             │                                       │
  │                             ▼                                       │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │  remediation_proposals (Delta)                               │   │
  │  │  sql, confidence, citations, llm_prompt_hash, context        │   │
  │  └──────────────────────────────┬───────────────────────────────┘   │
  └─────────────────────────────────│───────────────────────────────────┘
                                    │
  ┌─────────────────────────────────│───────────────────────────────────┐
  │  REVIEW                         ▼                                   │
  │                                                                     │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │  Databricks App / Lakeview Review UI                        │   │
  │  │                                                              │   │
  │  │  • diff preview: before/after state + source citations       │   │
  │  │  • per-steward queue (routed by resource owner)              │   │
  │  │  • bulk approve / reject / reassign                          │   │
  │  │  • confidence score + LLM rationale visible                  │   │
  │  └──────────────────────────────┬───────────────────────────────┘   │
  │                approved         │          rejected                  │
  │                  ┌──────────────┴──────────────┐                    │
  │                  ▼                             ▼                    │
  │          ┌──────────────┐              ┌─────────────┐             │
  │          │  Applier     │              │  logged,    │             │
  │          │  (Workflow)  │              │  closed     │             │
  │          │              │              └─────────────┘             │
  │          │ • least-priv │                                          │
  │          │   SP per     │                                          │
  │          │   agent      │                                          │
  │          │ • pre/post   │                                          │
  │          │   snapshot   │                                          │
  │          │ • dry-run    │                                          │
  │          │   supported  │                                          │
  │          └──────┬───────┘                                          │
  └─────────────────│──────────────────────────────────────────────────┘
                    │
  ┌─────────────────│──────────────────────────────────────────────────┐
  │  VERIFY         ▼                                                   │
  │                                                                     │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │  Next Watchdog Scan (verification oracle)                    │   │
  │  │                                                              │   │
  │  │  JOIN remediation_applied × violations                       │   │
  │  │  ON resource + policy                                        │   │
  │  │                                                              │   │
  │  │  ┌─────────────────────┐    ┌─────────────────────────────┐  │   │
  │  │  │ violation resolved  │    │ violation still open        │  │   │
  │  │  │ → status: verified  │    │ → status: verification_     │  │   │
  │  │  │ → trend line moves  │    │   failed                    │  │   │
  │  │  │   ✓                 │    │ → re-queue for investigation│  │   │
  │  │  └─────────────────────┘    └─────────────────────────────┘  │   │
  │  │                                                              │   │
  │  │  MLflow eval: precision, recall, time-to-resolve, cost/fix   │   │
  │  └──────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────┐
  │  MEASURE                                                            │
  │                                                                     │
  │  v_remediation_funnel    violations → proposed → approved →         │
  │                          applied → verified                         │
  │  v_remediation_trend     compliance delta: remediation vs organic   │
  │  v_agent_effectiveness   per-agent precision, throughput, cost      │
  │  v_reviewer_load         open queue depth per steward               │
  └─────────────────────────────────────────────────────────────────────┘
```

**Architectural reuse:** the dispatcher, applier, and verifier are all Databricks Workflow tasks. The review UI is a Databricks App. Agents run under a service principal with least-privilege grants. No new infrastructure — everything rides on the Watchdog engine bundle.

---

## 7. Functional Requirements

### 7.1 Agent SDK

- Python base class `RemediationAgent` with lifecycle hooks
- Declarative registration decorator: `@agent(handles=["POL-Q001"], model="databricks-llama-4-maverick")`
- Built-in helpers: Unity Catalog SDK client, Mosaic AI Model Serving client, MLflow tracking, vector search client (for RAG)
- Standard `Context` object: violation, resource metadata, schema, sample rows (configurable), tags, lineage, related docs, retrieved external content
- Prompt caching by content hash for reproducibility and cost control

### 7.2 Dispatcher Service

- Databricks Workflow task triggered after each Watchdog scan completes
- Reads `violations WHERE status = 'open' AND remediable = true`
- Routes to registered agents based on `handles` declarations
- Enforces per-agent concurrency, cost, and rate limits
- Writes Proposals to `remediation_proposals` table
- Idempotent: re-running on the same (violation, agent, version) does not duplicate proposals

### 7.3 Review UI

- Databricks App with list view, filterable by domain, severity, agent, steward, confidence
- Diff preview: before/after state, source citations from RAG retrieval, LLM rationale, confidence score
- Bulk operations: approve all high-confidence in a batch, reject all from agent X, reassign to another steward
- Per-user assignment based on resource owner (read from Watchdog's `v_resource_compliance` view)
- Keyboard-driven review for throughput

### 7.4 Applier

- Pulls `approved` proposals, executes SQL against Unity Catalog under a least-privilege SP
- Retries on transient failures, never on logic errors
- Writes to `remediation_applied` with pre/post snapshots
- Supports dry-run mode (generates the SQL, records it, does not execute)
- Rollback command: revert a specific applied proposal by re-applying the pre-state

### 7.5 Verifier

- Hooks into the next Watchdog scan after any `applied` proposals exist
- Joins `remediation_applied` x `violations` on resource and policy
- Flips status to `verified` if the violation is resolved in the new scan, `verification_failed` otherwise
- Triggers an MLflow evaluation run per agent: precision, recall, time-to-resolve, cost per fix

### 7.6 Governance of the Governance

- All agents run under a dedicated service principal with **least privilege per agent** (e.g., DocAgent can only `ALTER TABLE ... SET COMMENT`, not `DROP` or `GRANT`)
- The Guardrails MCP enforces that proposed SQL matches each agent's allowed operation set (extending existing read-only enforcement to scoped write enforcement)
- Every action logged to an append-only audit table
- LLM calls cached; same input produces the same proposal (reproducibility for auditors)
- No proposals auto-applied in v1 — human review is mandatory

---

## 8. Data Model Additions to Watchdog

| Table | Purpose |
|---|---|
| `remediation_agents` | Registry: agent_id, handles, version, model, config, active |
| `remediation_proposals` | Proposed fixes with full evidence trail |
| `remediation_applied` | Executed fixes with pre/post state |
| `remediation_reviews` | Review decisions with reviewer and reasoning |
| `remediation_metrics` | Per-agent precision, recall, cost, time-to-resolve |

New column on `violations`: `remediation_status` (values: `none`, `proposed`, `approved`, `applied`, `verified`, `failed`)

```
                    Data Model — Remediation Tables
                    (new tables shaded, existing unshaded)

  EXISTING WATCHDOG                          NEW REMEDIATION LAYER
  ═══════════════                            ═════════════════════

  ┌──────────────────┐                       ┌──────────────────────┐
  │   violations     │──────────────────────▶│ remediation_proposals│
  │                  │  violation_id          │                      │
  │ • violation_id   │                       │ • proposal_id    PK  │
  │ • policy_id      │                       │ • violation_id   FK  │
  │ • resource_name  │     ┌────────────────▶│ • agent_id       FK  │
  │ • status         │     │                 │ • agent_version      │
  │ • severity       │     │                 │ • status             │
  │ • owner          │     │                 │ • proposed_sql       │
  │ + remediation_   │     │                 │ • confidence         │
  │   status (NEW)   │     │                 │ • context_json       │
  └──────────────────┘     │                 │ • llm_prompt_hash    │
                           │                 │ • citations          │
  ┌──────────────────┐     │                 │ • created_at         │
  │ policies         │     │                 └──────────┬───────────┘
  │                  │     │                            │
  │ • policy_id      │     │               ┌────────────┤
  │ • name           │     │               │            │
  │ • severity       │     │               ▼            ▼
  │ • domain         │     │  ┌──────────────────┐  ┌──────────────────┐
  └──────────────────┘     │  │ remediation_     │  │ remediation_     │
                           │  │ reviews          │  │ applied          │
  ┌──────────────────┐     │  │                  │  │                  │
  │ resource_        │     │  │ • review_id  PK  │  │ • apply_id   PK  │
  │ inventory        │     │  │ • proposal_id FK │  │ • proposal_id FK │
  │                  │     │  │ • reviewer       │  │ • executed_sql   │
  │ • resource_name  │     │  │ • decision       │  │ • pre_state      │
  │ • resource_type  │     │  │ • reasoning      │  │ • post_state     │
  │ • tags           │     │  │ • reviewed_at    │  │ • applied_at     │
  │ • owner          │     │  └──────────────────┘  │ • verify_scan_id │
  │ • schema_name    │     │                        │ • verify_status  │
  └──────────────────┘     │                        └──────────────────┘
                           │
  ┌──────────────────┐     │  ┌──────────────────┐
  │ scan_summary     │     │  │ remediation_     │
  │                  │     │  │ agents           │
  │ • scan_id        │     │  │                  │
  │ • scan_time      │─────│─▶│ • agent_id   PK  │
  │ • total_resources│     │  │ • handles[]      │
  │ • violations_new │     │  │ • version        │
  └──────────────────┘     │  │ • model          │
                           │  │ • config_json    │
                           │  │ • active         │
                           └──│ • permissions[]  │
                              └──────────────────┘

                              ┌──────────────────┐
                              │ remediation_     │
                              │ metrics          │
                              │                  │
                              │ • agent_id   FK  │
                              │ • scan_id        │
                              │ • precision      │
                              │ • recall         │
                              │ • avg_cost       │
                              │ • avg_time_to_   │
                              │   resolve        │
                              └──────────────────┘
```

### Compliance views

| View | Purpose |
|---|---|
| `v_remediation_funnel` | Violations -> proposed -> approved -> applied -> verified |
| `v_agent_effectiveness` | Per-agent scorecard: precision, throughput, cost |
| `v_remediation_trend` | Compliance delta attributable to remediation vs. organic |
| `v_reviewer_load` | Open review queue depth per steward |

---

## 9. Reference Agents for v1

| Agent | Handles | LLM Task | Data Sources |
|---|---|---|---|
| **DocAgent** | POL-Q001, UndocumentedAsset | Generate table and column descriptions | Confluence, git, schema, sample rows, lineage |
| **StewardAgent** | POL-S001 (PII needs steward) | Suggest steward from org | AAD / SCIM, lineage, prior stewards on similar tables |
| **CostCenterAgent** | POL-C002 | Attribute cost_center | FinOps mapping, job / owner history, git CODEOWNERS |
| **GrantMigrationAgent** | POL-A002 (direct user grants) | Suggest group equivalent | UC grants, SCIM groups, group membership patterns |
| **ClassificationAgent** | UntaggedAsset -> PiiAsset et al. | Infer data classification | Column names, sample data, table comments, lineage |

**DocAgent is the v1 wedge** — it is the exact 7-Eleven use case, ported into the framework. The other four validate that the abstraction generalizes.

---

## 10. Success Metrics

- **Coverage:** percentage of open violations that have a registered remediation agent
- **Throughput:** violations resolved per week via the framework
- **Precision:** percentage of applied proposals that are not rejected or rolled back (target > 95%)
- **Time-to-resolve:** median hours from violation detected to verified resolved
- **Compliance delta:** movement of domain compliance percentage over rolling 30 days, attributable to remediation
- **Cost per remediation:** dollars in LLM tokens per verified fix
- **Reviewer burden:** percentage of proposals auto-approved via high-confidence rule vs. hand-reviewed

---

## 11. Risks and Open Questions

| Risk | Mitigation |
|---|---|
| LLM hallucinates wrong stewards or classifications | Mandatory human review in v1; confidence thresholds; MLflow evaluation harness per agent |
| Agent applies SQL that breaks downstream consumers | Least-privilege SP per agent; dry-run mode; rollback table; lineage-aware blast radius preview |
| Auditors reject "AI decided this" | Evidence trail: policy -> rule -> LLM prompt -> reviewer -> SQL -> verification. Reproducible via prompt cache. |
| Cost explodes | Per-agent token budgets; cache proposals; batch prompts; per-scan cost ceilings |
| Proposals grow stale between propose and apply | TTL on proposals; re-gather context before apply; invalidate on source resource change |
| Multiple agents fight over the same resource | Dispatcher locks per resource during proposal window |
| Agent version drift causes non-reproducible audit trail | Version pin on `remediation_proposals` row; old versions remain queryable |

**Open questions:**

1. Fully autonomous mode for high-confidence agents (documentation generation) — in scope for v2?
2. Should agents run inside Databricks Workflows (batch) or as a long-running Databricks App (event-driven)?
3. Model serving: per-agent model choice or shared Mosaic AI endpoint?
4. How do we handle multi-metastore remediation (same violation class across three metastores)?
5. Integration with ITSM (ServiceNow, Jira) for approval routing — v1 or v2?
6. Do agents consume Watchdog's MCP tools (nice abstraction, extra hop) or the underlying Delta tables directly (fast, coupled)?
7. How do we expose the framework to customers who want to write their own agents? (SDK package? Template bundle?)

---

## 12. Phased Rollout

### Phase 1 — Foundation

- Agent SDK + Dispatcher + Applier + Verifier + Review UI
- **DocAgent only** (the exact 7-Eleven use case, ported to the framework)
- Manual approval required for every proposal
- **Target:** reproduce the 7-Eleven result on a design-partner workspace, with an audit trail good enough for regulated-industry review (HIPAA, SOX, NIST 800-171, CMMC, ITAR)

### Phase 2 — Coverage

- StewardAgent, CostCenterAgent, ClassificationAgent
- Batch approval workflows
- MLflow evaluation harness per agent
- Confidence scoring + auto-approve for proposals above 0.9 confidence (opt-in per agent)

### Phase 3 — Autonomy

- GrantMigrationAgent (higher risk, needs more guardrails)
- Auto-approve lane for high-confidence proposals (still audit-logged)
- Lineage-aware remediation: fix upstream, cascade to downstream
- ServiceNow / Jira integration for review routing

### Phase 4 — Platform

- Agent marketplace: customers build their own agents against the SDK
- Industry-specific agent packs (healthcare, defense, financial) matching the existing Watchdog policy packs
- Cross-violation agents: one proposal that fixes multiple violations on the same resource atomically

---

## 13. Design Partner Profile

The ideal Phase 1 design partner has:

- **A regulated footprint** — HIPAA, SOX, NIST 800-171, CMMC, ITAR, or similar. Audit trail requirements keep Section 7.6 honest.
- **An existing Watchdog deployment** with a non-trivial open violation count (hundreds to thousands) — enough to exercise the dispatcher and review queue under realistic load
- **A moderately sized estate** — large enough that manual remediation is infeasible, small enough that one or two stewards can review a pilot batch end-to-end
- **An industry policy pack already loaded** (`library/healthcare/`, `library/financial/`, `library/defense/`, `library/general/`) so Phase 1 has real policies to remediate against on day one
- **Willingness to co-develop** — the partner gets a first-of-its-kind capability; we get enough regulatory pressure to keep the evidence trail honest

---

## 14. Relationship to Existing Work

| Existing thing | Relationship |
|---|---|
| [7-Eleven documentation migration](https://www.databricks.com/blog/automating-data-documentation-ai-how-7-eleven-bridged-metadata-gap) | Reference implementation. DocAgent is this, wrapped in the SDK. |
| [Databricks AI-generated comments in UC](https://www.databricks.com/blog/announcing-public-preview-ai-generated-documentation-databricks-unity-catalog) | Superset. AI comments handle the "no external source" case. DocAgent handles the "pull from Confluence / git / wiki" case and generalizes to other violations. |
| Watchdog engine | Provides the violation catalog, the policy model, and (critically) the verification oracle |
| Watchdog MCP | Stays as the query layer. Agents may consume MCP tools or hit Delta directly — open question 6. |
| Watchdog Guardrails MCP | Extends from read-only enforcement to scoped-write enforcement per agent |
| Ontos adapter | Remediation status flows through `v_resource_compliance` — no changes required |
| Industry policy packs | Each pack gains a matching agent pack in later phases (e.g., HIPAA retention-tag agent) |

---

## 15. Next Step

Walk this document through with the Databricks team that ran the 7-Eleven engagement. Specifically ask:

> "If we gave you a `Proposal` object and a verification oracle, could you wrap your existing Llama 4 Maverick + Mosaic AI + Confluence-RAG pipeline as a `DocAgent` against this SDK in two weeks?"

- **If yes:** Phase 1 is real. Scope the design-partner pilot, cut a branch, build the SDK and DocAgent in parallel.
- **If no:** the abstraction is wrong. Learn where before writing code.

---

## Appendix A: Glossary

- **Violation:** an instance of a policy failing for a specific resource in a specific scan
- **Proposal:** an LLM-generated fix for a violation, pending review
- **Apply:** the act of executing an approved proposal's SQL against Unity Catalog
- **Verify:** the next Watchdog scan confirming the violation is resolved
- **Evidence trail:** the auditable chain from policy to verified fix
- **Agent:** a pluggable unit that handles one or more violation classes
- **Dispatcher:** the router that maps violations to agents
- **Verification oracle:** Watchdog itself — the measurement layer doubles as the test harness for agent correctness
