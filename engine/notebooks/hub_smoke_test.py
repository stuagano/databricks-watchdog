# Databricks notebook source

# MAGIC %md
# MAGIC # Watchdog Hub Smoke Test
# MAGIC
# MAGIC Validates that all Hub-facing compliance views are alive and schema-conformant.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - At least one full Watchdog scan must have completed successfully
# MAGIC - The views must be deployed to the target catalog/schema
# MAGIC - Run as a user (or service principal) with SELECT on the watchdog schema
# MAGIC
# MAGIC **What this notebook checks**
# MAGIC - Each view defined in `hub_contract.yml` is queryable
# MAGIC - Actual column names match the contract (reports missing and extra columns)
# MAGIC - Row counts are non-zero (WARN if 0 rows — view exists but may be empty)
# MAGIC
# MAGIC **Status legend**
# MAGIC - `PASS` — schema matches and rows > 0
# MAGIC - `WARN` — schema matches but 0 rows (may be expected on fresh install)
# MAGIC - `FAIL` — missing columns or query error

# COMMAND ----------

CATALOG = "platform"
SCHEMA = "watchdog"
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

CONTRACT_SEARCH_PATHS = [
    "/Workspace/Repos/databricks-watchdog/engine/hub_contract.yml",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "hub_contract.yml")
    if "__file__" in dir()
    else None,
    "hub_contract.yml",
]

contract = None
contract_path_used = None

for path in CONTRACT_SEARCH_PATHS:
    if path is None:
        continue
    try:
        with open(path, "r") as f:
            contract = yaml.safe_load(f)
        contract_path_used = path
        break
    except FileNotFoundError:
        continue
    except Exception as e:
        print(f"Error reading {path}: {e}")
        continue

if contract is None:
    raise FileNotFoundError(
        "hub_contract.yml not found. Tried:\n"
        + "\n".join(str(p) for p in CONTRACT_SEARCH_PATHS if p)
    )

print(f"Loaded contract from: {contract_path_used}")
print(f"Contract version: {contract.get('version', 'unversioned')}")

# Convert views list to dict keyed by name
views_by_name = {v["name"]: v for v in contract["views"]}
print(f"Views in contract: {len(views_by_name)}")
for name in views_by_name:
    print(f"  - {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Views

# COMMAND ----------

results = []

for view_name, view_spec in views_by_name.items():
    full_name = f"{CATALOG}.{SCHEMA}.{view_name}"
    contract_cols = {col["name"] for col in view_spec.get("columns", [])}

    result = {
        "view": view_name,
        "status": None,
        "rows": None,
        "schema_match": None,
        "missing_cols": [],
        "extra_cols": [],
        "notes": "",
    }

    try:
        df = spark.sql(f"SELECT * FROM {full_name} LIMIT 10")
        actual_cols = set(df.columns)

        missing = sorted(contract_cols - actual_cols)
        extra = sorted(actual_cols - contract_cols)

        result["missing_cols"] = missing
        result["extra_cols"] = extra
        result["schema_match"] = len(missing) == 0

        # Get row count (full scan capped to avoid large tables blocking the test)
        count_df = spark.sql(f"SELECT COUNT(*) AS n FROM {full_name}")
        row_count = count_df.collect()[0]["n"]
        result["rows"] = row_count

        if missing:
            result["status"] = "FAIL"
            result["notes"] = f"Missing columns: {missing}"
        elif row_count == 0:
            result["status"] = "WARN"
            result["notes"] = "View exists and schema matches, but contains 0 rows"
        else:
            result["status"] = "PASS"
            if extra:
                result["notes"] = f"Extra columns (not in contract): {extra}"

    except Exception as e:
        result["status"] = "FAIL"
        result["schema_match"] = False
        result["rows"] = None
        result["notes"] = f"Query error: {e}"

    results.append(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

STATUS_ICONS = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL"}
SCHEMA_ICONS = {True: "✓", False: "✗", None: "?"}

# Header
col_widths = [38, 6, 10, 13, 55]
header = (
    f"{'View':<{col_widths[0]}}"
    f"{'Status':<{col_widths[1]}}"
    f"{'Rows':>{col_widths[2]}}"
    f"{'Schema Match':>{col_widths[3]}}"
    f"  {'Notes'}"
)
separator = "-" * (sum(col_widths) + 2)

print(separator)
print(header)
print(separator)

for r in results:
    row_str = str(r["rows"]) if r["rows"] is not None else "n/a"
    schema_icon = SCHEMA_ICONS[r["schema_match"]]
    print(
        f"{r['view']:<{col_widths[0]}}"
        f"{r['status']:<{col_widths[1]}}"
        f"{row_str:>{col_widths[2]}}"
        f"{schema_icon:>{col_widths[3]}}"
        f"  {r['notes']}"
    )

print(separator)

# Summary counts
n_pass = sum(1 for r in results if r["status"] == "PASS")
n_warn = sum(1 for r in results if r["status"] == "WARN")
n_fail = sum(1 for r in results if r["status"] == "FAIL")
print(f"\nSummary: {n_pass} PASS  {n_warn} WARN  {n_fail} FAIL  (of {len(results)} views)")

if n_fail > 0:
    print("\nFAILED views:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"  {r['view']}: {r['notes']}")

# Display as DataFrame if running in Databricks
try:
    from pyspark.sql import Row
    rows = [
        Row(
            view=r["view"],
            status=r["status"],
            rows=str(r["rows"]) if r["rows"] is not None else "n/a",
            schema_match=SCHEMA_ICONS[r["schema_match"]],
            notes=r["notes"],
        )
        for r in results
    ]
    result_df = spark.createDataFrame(rows)
    display(result_df)
except Exception:
    pass
