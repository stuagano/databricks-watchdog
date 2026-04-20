# Policy Compile-Down — Schema-as-Code Across Runtime and Scan-Time

**Status:** Draft for review
**Date:** 2026-04-19
**Branch:** `claude/spark-schema-as-code-eknJa`

---

## Problem

Watchdog detects governance gaps at scan time. The platform enforces them at runtime — UC ABAC masks columns, SDP/DLT expectations drop bad rows, tag policies reject bad values, the guardrails MCP blocks unsafe AI calls. Both layers are valuable, and a mature stack runs both.

Today they are **authored separately**. A steward writes a Watchdog policy: *"PII columns must be masked."* Someone else, in a different repo, writes the matching ABAC rule. A data engineer writes the SDP expectation that enforces the same intent at write time. These artifacts drift. Worse, the posture score cannot tell the difference between *"rule exists on paper"* and *"rule exists and is actually enforced at runtime."*

The result: two sources of truth for the same intent, no guarantee they agree, and a posture score that overstates compliance whenever the runtime artifact is missing or out of sync.

## Goals

- One declarative policy in Watchdog's ontology language is the **single source of truth**.
- That policy **compiles down** to runtime enforcement artifacts on the platform substrates that already exist (UC ABAC, UC tag policies, SDP expectations, Guardrails MCP checks).
- The posture score reflects whether the runtime artifact is **deployed and healthy**, not just whether the policy is written.
- Policies that have no runtime equivalent (cross-asset, aggregate, attributional) remain scan-only and are clearly labelled as such.
- Compile-down is **opt-in per policy** — authors decide whether a rule should be enforced at runtime, measured at scan time, or both.

## Non-Goals

- Building a Spark write interceptor or a new runtime enforcement substrate. Watchdog emits artifacts for substrates the platform already provides; it does not replace them.
- Auto-generating SDP pipelines themselves. We emit *expectations* that attach to user-authored pipelines; we do not generate the pipeline code.
- Two-way sync. Compile-down is one-way: policy → artifact. Drift in the deployed artifact is detected and reported, not silently reconciled.
- Replacing UC grants. Runtime enforcement is defense-in-depth, not access control.

---

## Architecture

A **compiler** sits between the policy/ontology files and the platform. For each policy with a `compile_to:` block, the compiler emits one artifact per target, deploys it to the workspace, and records the deployment in a manifest. The scanner then verifies that each declared artifact is present and healthy, and feeds that signal into the posture score.

```
                    engine/policies/*.yml
                            │
                            ▼
                  ┌──────────────────┐
                  │   compiler       │  one policy → N targets
                  └────────┬─────────┘
        ┌───────────┬──────┴──────┬──────────────┐
        ▼           ▼             ▼              ▼
   UC ABAC      UC Tag        SDP          Guardrails
   rule         policy        expectation  MCP check
        │           │             │              │
        └───────────┴──────┬──────┴──────────────┘
                           ▼
                ┌─────────────────────┐
                │ compile manifest    │  what was emitted, where
                └──────────┬──────────┘
                           ▼
                  scanner verifies
                  artifact presence
                  + health, scores
                  posture accordingly
```

### Compile targets

| Watchdog policy intent             | Compiles to              | Enforces at    |
|------------------------------------|--------------------------|----------------|
| `PII columns must be masked`       | UC ABAC rule             | Query time     |
| `Bronze→Silver key must be non-null` | SDP expectation        | Write time     |
| `Prod tables must have steward`    | UC tag policy            | Tag-set time   |
| `Agent must not read unclassified PII` | Guardrails MCP check | Call time *(already done)* |
| Cross-asset / aggregate rules      | *(no runtime target)*    | Scan time only |

---

## Policy schema extension

A policy gets one optional new block. Existing policies are unchanged.

```yaml
- id: POL-PII-001
  name: "PII columns must be masked in production"
  applies_to: PIIColumn
  domain: Security
  severity: critical
  rule:
    ref: column_has_mask
  compile_to:
    - target: uc_abac
      mask_function: main.governance.redact_pii
      apply_when: environment = prod
```

Multiple targets are allowed. The compiler validates each target against a per-target schema. Policies without `compile_to:` remain scan-only — explicit, not implicit.

---

## What does *not* compile

Some policies fundamentally cannot be enforced at runtime — they are properties of the **population**, not of any single event. These stay scan-only and are tagged as such in the posture report so the gap is visible:

- `% of PII tables with a steward` — aggregate
- `Owner X has more than N open violations` — attributional
- `Compliance score is improving month-over-month` — trend
- `If we added rule R, how many assets fail?` — simulation

This is a feature, not a limitation. Runtime gives event-level guarantees; scan gives population-level guarantees. Compile-down clarifies which policies live on which side.

---

## Drift between policy and deployed artifact

Compile-down creates a real ops surface: the deployed artifact can be edited or deleted out-of-band. The compile manifest stores a hash of what was emitted; the scanner re-reads the artifact on each run and compares. Three states:

- **In sync** — counts as enforced; full posture credit.
- **Drifted** — present but modified out-of-band; raises a meta-violation, partial credit.
- **Missing** — declared in `compile_to:` but absent from the workspace; raises a meta-violation, no runtime credit (scan-only credit still applies).

The compiler never silently overwrites a drifted artifact. The steward decides: re-emit, accept the drift into policy, or remove the `compile_to:` block.

---

## Posture scoring change

Today: a policy is `compliant` for an asset if the rule evaluates true. A policy is `non-compliant` if it evaluates false.

With compile-down, each policy/asset pair can be in one of:

| State                      | Posture credit |
|----------------------------|----------------|
| Scan-only policy, rule passes              | full           |
| Compile-down policy, rule passes, artifact in sync | full   |
| Compile-down policy, rule passes, artifact drifted | partial |
| Compile-down policy, rule passes, artifact missing | partial |
| Rule fails                                  | none           |

This is the change that makes the posture score honest: *"the rule is written"* and *"the rule is enforced"* are no longer conflated.

---

## Rollout

1. Schema extension and compiler skeleton (no targets yet) — proves the manifest + drift detection loop.
2. Guardrails MCP target first — Watchdog already owns this substrate, so no platform-edge risk.
3. UC tag policy target — next-simplest, fully declarative.
4. UC ABAC target — needs mask function lifecycle handling.
5. SDP expectation target — needs to attach to user pipelines without owning them; design separately.

Each target ships independently. The compiler is useful from step 2 onward.

---

## Open questions

- **Artifact ownership.** When the compiler emits an ABAC rule, who "owns" it in UC for grant purposes? Likely a dedicated service principal; needs a security review.
- **Pipeline attachment for SDP.** Expectations live inside a pipeline definition. Do we emit them as a Python module the user imports, or via a side-channel that mutates the pipeline spec? The former is cleaner but requires user cooperation.
- **Idempotency on re-emit.** Some platform APIs (ABAC) are idempotent on rule name; others are not. The compiler needs a per-target upsert strategy.
- **Mask function lifecycle.** Compile-down can emit ABAC rules that reference UDF mask functions, but the functions themselves are user-authored. Out of scope for v1; document the dependency.
- **Test substrate.** Compile-down needs an integration environment with a real workspace. Unit tests can cover emission shape; drift detection needs end-to-end.

---

## Why this is the right shape

- **Reuses platform enforcement** — no Spark interceptor, no second runtime to operate.
- **Single source of truth** — one policy file, many enforcement points.
- **Honest posture score** — "enforced" and "written" stop being the same thing.
- **Opt-in per policy** — no forced migration; existing scan-only policies keep working unchanged.
- **Composable with what's already shipped** — Guardrails MCP becomes the first compile target, not a parallel system.
