# Watchdog Test Suite

Two-tier fixture-based tests for the watchdog bundle.

## Tiers

### Unit tests (no Spark)

Pure Python — fast, runnable in CI without a Databricks connection.

Tests `OntologyEngine` and `RuleEngine` in isolation using the live
`ontologies/resource_classes.yml` and `rule_primitives.yml`.

```bash
cd bundles/watchdog-bundle
pip install -e src/
pytest tests/unit/ -v
```

Coverage:
- 25+ ontology classification cases: base classes, derived classes, ancestor chains,
  multi-class resources, edge cases (wrong resource_type, unknown values)
- 35+ rule evaluation cases: all rule types, composite operators, primitive refs,
  non-obvious behaviors (dual-source owner, version comparison, vacuous truth)

### Integration tests (needs Spark)

Requires a live Spark session via `databricks-connect` or running inside a
Databricks cluster. Creates an isolated `<catalog>.watchdog_test_<uid>` schema,
seeds known fixture resources, runs the evaluate pipeline, asserts violations.

```bash
export WATCHDOG_TEST_CATALOG=<your_catalog>
export DATABRICKS_CONFIG_PROFILE=<your_profile>   # if non-default
pytest tests/integration/ -v -m integration
```

Coverage:
- Policy lifecycle: each fixture resource triggers the expected (resource_id, policy_id)
  violations and clean resources produce zero violations
- MERGE lifecycle: open → resolved on passing scan; first_detected stable on repeat failure
- Exception handling: active exception → status=exception; expired → still open
- Compliance views: v_class_compliance and v_resource_compliance reflect evaluation output

## Fixture resources

| Resource | Type | Tags | Expected violations |
|----------|------|------|---------------------|
| `e2e/table/gold_clean` | table | data_layer=gold, data_classification=internal, owner, business_unit, environment | **none** |
| `e2e/table/pii_no_steward` | table | data_classification=pii, owner, business_unit | POL-S001 (critical) |
| `e2e/table/untagged` | table | *none* | POL-C001 (no owner), POL-S003 (no classification), POL-C003 (no BU) |
| `e2e/cluster/bare` | cluster | environment=dev, owner | POL-C002 (no cost_center), POL-C006 (no autotermination) |
| `e2e/job/old_runtime` | job | environment=prod, owner, BU, cost_center | POL-S005 (spark 10.4 < 15.4) |

## E2E notebook

`notebooks/run_e2e_tests.py` runs the full pipeline end-to-end in a Databricks
notebook. Run via DABs (catalog driven by `var.catalog` in `databricks.yml`):

```bash
databricks bundle run watchdog_e2e_test
```

Or open the notebook interactively — widget `cleanup=false` keeps the test
schema for manual inspection after the run.

## Depends on

These tests require `stuart/p-watchdog` merged. The bundle code
(`src/watchdog/`), ontology files, and policy YAML must all be present.
