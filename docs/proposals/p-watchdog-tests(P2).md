# p-watchdog-tests — Fixture-Based Test Suite for Watchdog

**Branch:** `proposals/stuart-handoff/p-watchdog-tests`
**Depends on:** `proposals/stuart-handoff/p-watchdog` (bundle code, ontologies, policies)
**Status:** ✅ Superseded — implemented in the standalone watchdog repo (2026-05-13)

> **Superseded — see [`~/Documents/Projects/databricks-watchdog/tests/`](../../../../Projects/databricks-watchdog/tests/) (standalone repo `CustomerDataPlatform/watchdog`).**
>
> Watchdog development moved out of customer-infra after the standalone repo extraction (see memory: *Watchdog standalone repo extraction*). The test suite this proposal calls for is implemented there:
>
> | Proposed file | Standalone-repo location | Status |
> | --- | --- | --- |
> | `tests/unit/test_ontology.py` (25+ cases) | `tests/unit/test_ontology.py` (211 lines) | ✅ |
> | `tests/unit/test_rule_engine.py` (35+ cases) | `tests/unit/test_rule_engine.py` (651 lines) | ✅ |
> | `tests/integration/test_policy_lifecycle.py` | `tests/integration/test_policy_lifecycle.py` (167 lines) | ✅ |
> | `tests/integration/test_violations_merge.py` | `tests/integration/test_violations_merge.py` (238 lines) | ✅ |
> | `notebooks/run_e2e_tests.py` | `tests/e2e/test_crawl_evaluate_pipeline.py` | ✅ |
> | — additional coverage — | 40 unit test files total (crawler, compiler, deployer, drift, exceptions, guardrails, MCP tools, ontos sync, policy packs ×4, remediation, views, etc.) | ✅ |
>
> Keeping this file as historical record of the test design. Do not duplicate the suite in `customer-infra/bundles/watchdog/tests/` — that would fork the test code against the "no longer syncing into customer-infra" decision.

## What this adds

A two-tier fixture-based test suite that validates the watchdog governance pipeline
end-to-end, from ontology classification through policy evaluation to violation
lifecycle management.

## Why fixture-based, not crawl-based

Crawl-based tests depend on the actual state of the target workspace —
they're flaky by definition (resources appear and disappear). Fixture-based tests:
- Inject known resources directly into `resource_inventory`
- Assert on specific `(resource_id, policy_id)` violation pairs
- Are deterministic regardless of what else is in the workspace
- Can simulate remediation (re-inject fixed tags → assert violation resolved)

## Two tiers

### Tier 1: Unit tests

**Location:** `tests/unit/`
**Requires:** Python only — no Spark, no Databricks connection

`test_ontology.py` (25+ cases):
- Base class matching by resource_type
- Derived class classification by tags
- Ancestor chain expansion
- Multi-class resources (PII dosimetry table)
- Edge cases (wrong resource_type, unknown values, `metadata_equals: {resource_type: table}` via the special-case param)

`test_rule_engine.py` (35+ cases):
- All rule types with pass and fail paths
- Non-obvious behaviors: `_eval_if_then` vacuous truth, `_eval_all_of` collects all failures (no short-circuit), `_eval_has_owner` dual-source check, `_eval_metadata_gte` version-aware comparison
- Live primitive refs via `ontologies/rule_primitives.yml`

Run:
```bash
cd bundles/watchdog-bundle && pip install -e src/
pytest tests/unit/ -v
```

### Tier 2: Integration tests

**Location:** `tests/integration/`
**Requires:** databricks-connect (or run inside a Databricks cluster)

`test_policy_lifecycle.py`:
- Seeds 5 fixture resources: one clean, four with specific violations
- Asserts exact `(resource_id, policy_id)` pairs after `evaluate_all()`
- Asserts clean resource has zero open violations
- Spot-checks `v_class_compliance` and `v_resource_compliance` views

`test_violations_merge.py`:
- First failure → `status=open`
- Repeated failure → `first_detected` stable, `last_detected` updated
- Passing scan → `status=resolved`, `resolved_at` set
- Active exception → `status=exception`
- Expired exception → `status=open` (not protected)

Run:
```bash
export WATCHDOG_TEST_CATALOG=<your_catalog>
export DATABRICKS_CONFIG_PROFILE=<your_profile>   # if non-default
pytest tests/integration/ -v -m integration
```

Tests skip automatically if `WATCHDOG_TEST_CATALOG` is not set.

### E2E notebook + job

`notebooks/run_e2e_tests.py` runs the full pipeline in a single Databricks notebook:
setup fixtures → evaluate → assert → remediation scan → assert resolved → cleanup.

The `catalog` widget must be set before running. As a DABs job, catalog is driven
by `${var.catalog}` from the bundle's existing variable:

```bash
databricks bundle run watchdog_e2e_test
```

## Fixture resources

| Resource | Type | Expected violations |
|----------|------|---------------------|
| `gold_clean` | table | **none** — fully tagged gold table |
| `pii_no_steward` | table | POL-S001 critical — PII without steward/retention |
| `untagged` | table | POL-C001, POL-S003, POL-C003 — bare table |
| `bare` cluster | cluster | POL-C002, POL-C006 — no cost_center, no autotermination |
| `old_runtime` job | job | POL-S005 — spark 10.4.x below 15.4 threshold |

## Test isolation

Integration tests create a `<catalog>.watchdog_test_<uuid>` schema per session
and drop it on teardown. Per-test subsets use a nested `watchdog_merge_test_<uuid>`
schema. No production data is touched.

## Files added

```
bundles/watchdog-bundle/
├── tests/
│   ├── README.md
│   ├── unit/
│   │   ├── conftest.py            # sys.path setup, ontology_dir + policies_dir fixtures
│   │   ├── test_ontology.py       # 25+ OntologyEngine cases
│   │   └── test_rule_engine.py    # 35+ RuleEngine cases
│   └── integration/
│       ├── conftest.py            # Spark, test schema lifecycle, seed_inventory, policy_engine
│       ├── test_policy_lifecycle.py   # End-to-end: fixture → classify → evaluate → assert
│       └── test_violations_merge.py  # MERGE lifecycle and exception handling
├── notebooks/
│   └── run_e2e_tests.py           # Databricks notebook E2E test with assertions
└── resources/
    └── watchdog_e2e_test_job.yml  # DABs job (catalog from ${var.catalog})
```
