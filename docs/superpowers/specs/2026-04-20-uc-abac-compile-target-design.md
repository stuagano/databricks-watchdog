# UC ABAC Column Mask Compile Target

**Status:** Approved
**Date:** 2026-04-20

---

## Problem

Watchdog policies declare governance intent ("PII columns must be masked") but the matching UC ABAC column mask rule is authored separately in the workspace. The two drift apart. The posture score cannot distinguish "policy written" from "policy enforced at query time."

The compile-down framework (compiler, manifest, drift detection) is already in place with two targets (Guardrails MCP, UC tag policy). Adding a UC ABAC target closes the loop for the most common runtime enforcement pattern: column masking.

## Goal

A new `UCAbacTarget` compile target that emits a declarative JSON artifact describing a UC column mask rule. The artifact feeds the deployer (future) which turns it into `ALTER TABLE ... SET COLUMN MASK` API calls. The compiler stays pure — no SDK calls, no workspace connectivity.

## Non-Goals

- **Row filters.** Different config shape and policy semantics. Can be a separate `uc_row_filter` target later.
- **Mask function lifecycle.** The compiler references UDFs but does not create, update, or verify them. The mask function is a user-authored dependency.
- **Principal scoping.** v1 uses the UC default: owner sees raw data, everyone else sees masked. Optional `except_principals` can be added later without breaking existing artifacts.
- **Deploy step.** The compiler emits artifacts; a deployer applies them to the workspace. Deploy is out of scope here.

---

## Config Shape

```yaml
compile_to:
  - target: uc_abac
    mask_function: main.governance.redact_pii   # required, 3-part UDF name
    apply_when: environment = prod               # optional, human-readable scope note
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `target` | yes | Must be `uc_abac` |
| `mask_function` | yes | Fully qualified UDF name (`catalog.schema.function`). Validated as three dot-separated identifiers. Not verified against the workspace. |
| `apply_when` | no | Human-readable scope note stored in the artifact. Not parsed or evaluated by the compiler — it is documentation for the deployer and for drift review. |

## Artifact Shape

```json
{
  "policy_id": "POL-PII-001",
  "name": "PII columns must be masked in production",
  "mask_function": "main.governance.redact_pii",
  "apply_when": "environment = prod",
  "applies_to": "PIIColumn",
  "severity": "critical",
  "domain": "Security",
  "description": "PII columns must be masked in production"
}
```

Stored at `uc_abac/{policy_id}.json`. Content is JSON with sorted keys for deterministic hashing, consistent with existing targets.

## Validation Rules

1. `mask_function` must be present. Missing → `ValueError`.
2. `mask_function` must be a three-part dotted identifier matching `^\w+\.\w+\.\w+$`. Malformed (1-part, 2-part, 4-part) → `ValueError`.
3. `apply_when` is optional. Omitted → not present in artifact.

## Integration

- New class `UCAbacTarget` implementing `CompileTarget` protocol.
- Registered in `DEFAULT_REGISTRY` as `"uc_abac"`.
- Existing `compile_policies`, `write_artifacts`, `write_manifest`, and `check_drift` work without changes.
- The existing `test_unknown_target_raises` test that checks for `sdp_expectation` remains valid — `uc_abac` is a new known target.

## Tests

Same pattern as `TestUCTagPolicyTarget`:

| Test | Asserts |
|------|---------|
| Valid mask function emits correct artifact | Content matches expected JSON shape, artifact_id is `uc_abac/{id}.json` |
| `apply_when` omitted | Artifact produced without `apply_when` field |
| Missing `mask_function` | Raises `ValueError` with descriptive message |
| Malformed `mask_function` — 1-part | Raises `ValueError` |
| Malformed `mask_function` — 2-part | Raises `ValueError` |
| Malformed `mask_function` — 4-part | Raises `ValueError` |
| Deterministic hash | Two compiles of same input produce identical content hash |
| End-to-end through registry | YAML → `load_yaml_policies` → `compile_policies` → `write_artifacts` → `check_drift` returns `in_sync` |
