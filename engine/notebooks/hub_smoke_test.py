# Databricks notebook source
# MAGIC %md
# MAGIC # Hub Smoke Test
# MAGIC
# MAGIC Validates that all Hub-facing compliance views are alive, queryable,
# MAGIC and conform to the schema defined in `hub_contract.yml`.
# MAGIC
# MAGIC **Run after deployment** to verify views work against real data.
# MAGIC
# MAGIC **Prerequisites:** At least one Watchdog scan must have completed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set these to match your deployment
CATALOG = dbutils.widgets.get("catalog") if "dbutils" in dir() else "platform"
SCHEMA = dbutils.widgets.get("schema") if "dbutils" in dir() else "watchdog"

try:
    dbutils.widgets.text("catalog", "platform", "Catalog")
    dbutils.widgets.text("schema", "watchdog", "Schema")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
except Exception:
    pass

print(f"Testing views in: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Contract

# COMMAND ----------

import yaml
import os

# Contract is bundled in the deployment alongside engine code
contract_paths = [
    "/Workspace/Repos/databricks-watchdog/engine/hub_contract.yml",
    os.path.join(os.path.dirname(os.path.abspath("")), "hub_contract.yml"),
    "hub_contract.yml",
]

contract = None
for path in contract_paths:
    try:
        with open(path) as f:
            contract = yaml.safe_load(f)
        print(f"Loaded contract from: {path}")
        break
    except FileNotFoundError:
        continue

if contract is None:
    raise FileNotFoundError(
        "hub_contract.yml not found. Searched: " + ", ".join(contract_paths)
    )

print(f"Contract version: {contract['version']}")
print(f"Views to validate: {list(contract['views'].keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Views

# COMMAND ----------

results = []

for view_name, view_def in contract["views"].items():
    fqn = f"{CATALOG}.{SCHEMA}.{view_name}"
    status = "PASS"
    notes = []
    row_count = 0
    schema_match = True

    try:
        # Query the view
        df = spark.sql(f"SELECT * FROM {fqn} LIMIT 10")
        row_count = df.count()

        # Check column names
        actual_columns = [f.name.lower() for f in df.schema.fields]
        expected_columns = [c["name"].lower() for c in view_def["columns"]]

        missing = set(expected_columns) - set(actual_columns)
        extra = set(actual_columns) - set(expected_columns)

        if missing:
            schema_match = False
            status = "FAIL"
            notes.append(f"Missing columns: {', '.join(sorted(missing))}")

        if extra:
            notes.append(f"Extra columns (not in contract): {', '.join(sorted(extra))}")

        if row_count == 0:
            if status == "PASS":
                status = "WARN"
            notes.append("No rows returned — has a scan been run?")

    except Exception as e:
        status = "FAIL"
        schema_match = False
        error_msg = str(e)[:200]
        notes.append(f"Query failed: {error_msg}")

    results.append({
        "view": view_name,
        "status": status,
        "rows": row_count,
        "schema_match": "✓" if schema_match else "✗",
        "notes": "; ".join(notes) if notes else "",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

import pandas as pd

results_df = pd.DataFrame(results)
print("\n" + "=" * 90)
print("HUB SMOKE TEST RESULTS")
print("=" * 90)
print(results_df.to_string(index=False))
print("=" * 90)

passed = sum(1 for r in results if r["status"] == "PASS")
warned = sum(1 for r in results if r["status"] == "WARN")
failed = sum(1 for r in results if r["status"] == "FAIL")

print(f"\nSummary: {passed} PASS, {warned} WARN, {failed} FAIL out of {len(results)} views")

if failed > 0:
    print("\n⚠ FAILURES DETECTED — review notes above for details.")
else:
    print("\n✓ All views are queryable and schema-conformant.")

# Display as Databricks table for notebook UI
if "displayHTML" in dir():
    display(spark.createDataFrame(results))
