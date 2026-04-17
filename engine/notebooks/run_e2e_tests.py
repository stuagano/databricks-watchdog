# Databricks notebook source
# MAGIC %md
# MAGIC # Watchdog — End-to-End Test Harness
# MAGIC
# MAGIC Exercises the full pipeline in an isolated `<catalog>.watchdog_test_<uid>` schema:
# MAGIC
# MAGIC 1. Create fixture resources (tables + cluster-like metadata rows).
# MAGIC 2. Run `crawl` to populate `resource_inventory`.
# MAGIC 3. Run `evaluate` to produce `violations`.
# MAGIC 4. Assert that each fixture resource triggers the expected violations
# MAGIC    and clean resources produce zero violations.
# MAGIC 5. Optionally keep the test schema for manual inspection.
# MAGIC
# MAGIC Run via the `watchdog_e2e_test` job in `databricks.yml`, or open the
# MAGIC notebook interactively and set widgets.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Target catalog (no prod data)")
dbutils.widgets.text("test_uid", "", "Test UID (auto-generated when empty)")
dbutils.widgets.dropdown("cleanup", "true", ["true", "false"], "Drop schema after run?")

catalog = dbutils.widgets.get("catalog")
test_uid = dbutils.widgets.get("test_uid")
cleanup = dbutils.widgets.get("cleanup") == "true"

import uuid
from datetime import datetime, timezone

if not test_uid:
    test_uid = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

schema = f"watchdog_test_{test_uid}"
qualified = f"{catalog}.{schema}"

print(f"Test schema: {qualified}")
print(f"Cleanup after run: {cleanup}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the isolated test schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified}")
spark.sql(f"USE {qualified}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seed fixture resources
# MAGIC
# MAGIC Each fixture is designed to trigger specific policies (see tests/README.md).

# COMMAND ----------

fixtures = {
    "gold_clean": {
        "tags": {"data_layer": "gold", "data_classification": "internal",
                 "business_unit": "platform", "environment": "prod",
                 "data_steward": "alice@co.com"},
        "owner": "alice@co.com",
        "expected_violations": [],
    },
    "pii_no_steward": {
        "tags": {"data_classification": "pii", "business_unit": "platform"},
        "owner": "alice@co.com",
        "expected_violations": ["POL-SEC-003"],
    },
    "untagged": {
        "tags": {},
        "owner": "",
        "expected_violations": ["POL-C001", "POL-C003"],
    },
}

for name, spec in fixtures.items():
    table = f"{qualified}.{name}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (id BIGINT, payload STRING) USING DELTA")
    for k, v in spec["tags"].items():
        spark.sql(f"ALTER TABLE {table} SET TAGS ('{k}' = '{v}')")
    if spec["owner"]:
        spark.sql(f"ALTER TABLE {table} OWNER TO `{spec['owner']}`")

print(f"Seeded {len(fixtures)} fixture tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run the crawl → evaluate pipeline

# COMMAND ----------

import sys
sys.path.insert(0, "../src")

from databricks.sdk import WorkspaceClient

from watchdog.crawler import ResourceCrawler
from watchdog.policy_loader import sync_policies_to_delta
from watchdog.views import ensure_semantic_views

w = WorkspaceClient()
crawler = ResourceCrawler(spark, w, catalog, schema)
crawler.crawl_all(resource_types={"table"})
print("Crawl complete.")

sync_policies_to_delta(spark, catalog, schema)

from watchdog.ontology import OntologyEngine
from watchdog.rule_engine import RuleEngine
from watchdog.policy_engine import PolicyEngine
from watchdog.policy_loader import load_yaml_policies

engine = PolicyEngine(
    spark, w, catalog, schema,
    ontology=OntologyEngine(),
    rule_engine=RuleEngine(),
    policies=load_yaml_policies(),
)
results = engine.evaluate_all()
ensure_semantic_views(spark, catalog, schema)

print(f"Evaluate complete: {results.new_violations} violations produced.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Assert expected violations

# COMMAND ----------

failures: list[str] = []
for name, spec in fixtures.items():
    resource_name = f"{qualified}.{name}"
    actual_rows = spark.sql(f"""
        SELECT DISTINCT policy_id
        FROM {catalog}.{schema}.violations
        WHERE resource_name = '{resource_name}' AND status = 'open'
    """).collect()
    actual = {r.policy_id for r in actual_rows}
    expected = set(spec["expected_violations"])

    missing = expected - actual
    extra = actual - expected
    if missing:
        failures.append(f"{name}: missing policies {sorted(missing)}")
    if extra and not expected:
        failures.append(f"{name}: clean fixture produced violations {sorted(extra)}")
    status = "OK" if not missing and not (extra and not expected) else "FAIL"
    print(f"  {status}: {name} → expected={sorted(expected)} actual={sorted(actual)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clean up (optional)

# COMMAND ----------

if cleanup:
    spark.sql(f"DROP SCHEMA IF EXISTS {qualified} CASCADE")
    print(f"Dropped {qualified}")
else:
    print(f"Kept {qualified} for inspection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

if failures:
    print("E2E TEST FAILURES:")
    for f in failures:
        print(f"  - {f}")
    dbutils.notebook.exit(f"FAIL: {len(failures)} fixture assertions failed")
else:
    print(f"E2E PASSED: {len(fixtures)} fixtures verified.")
    dbutils.notebook.exit("OK")
