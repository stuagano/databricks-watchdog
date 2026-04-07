# Databricks notebook source
# MAGIC %md
# MAGIC # Watchdog — Approve Policy Exception
# MAGIC
# MAGIC Grant a time-limited exception for a resource-policy pair.
# MAGIC The next scan will mark matching violations as `exception` instead of `open`.
# MAGIC
# MAGIC **Run interactively** or trigger from the AI/BI dashboard "Request Exception" link.

# COMMAND ----------

dbutils.widgets.text("resource_id", "", "Resource ID")
dbutils.widgets.text("policy_id", "", "Policy ID")
dbutils.widgets.text("justification", "", "Justification (required)")
dbutils.widgets.dropdown("expires_days", "90", ["30", "60", "90", "180", "365", "permanent"], "Expires In (days)")

# COMMAND ----------

resource_id = dbutils.widgets.get("resource_id")
policy_id = dbutils.widgets.get("policy_id")
justification = dbutils.widgets.get("justification")
expires_days = dbutils.widgets.get("expires_days")

assert resource_id, "resource_id is required"
assert policy_id, "policy_id is required"
assert justification and len(justification) >= 10, "justification must be at least 10 characters"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resource & Violation Context

# COMMAND ----------

catalog = spark.conf.get("spark.databricks.watchdog.catalog", "platform")
schema = spark.conf.get("spark.databricks.watchdog.schema", "watchdog")

# Show the violation being excepted
display(spark.sql(f"""
    SELECT resource_id, resource_name, resource_type, policy_id,
           severity, domain, detail, owner, first_detected, last_detected, status
    FROM {catalog}.{schema}.violations
    WHERE resource_id = '{resource_id}' AND policy_id = '{policy_id}'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approve Exception

# COMMAND ----------

import uuid

exception_id = str(uuid.uuid4())
approved_by = spark.sql("SELECT current_user()").first()[0]

if expires_days == "permanent":
    expires_expr = "NULL"
else:
    expires_expr = f"current_timestamp() + INTERVAL {expires_days} DAY"

spark.sql(f"""
    INSERT INTO {catalog}.{schema}.exceptions
    (exception_id, resource_id, policy_id, approved_by, justification, approved_at, expires_at, active)
    VALUES (
        '{exception_id}',
        '{resource_id}',
        '{policy_id}',
        '{approved_by}',
        '{justification}',
        current_timestamp(),
        {expires_expr},
        true
    )
""")

print(f"Exception approved: {exception_id}")
print(f"  Resource:      {resource_id}")
print(f"  Policy:        {policy_id}")
print(f"  Approved by:   {approved_by}")
print(f"  Expires:       {expires_days} days" if expires_days != "permanent" else "  Expires:       never")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confirmation
# MAGIC The violation will be marked `exception` on the next scan run.

# COMMAND ----------

display(spark.sql(f"""
    SELECT exception_id, resource_id, policy_id, approved_by,
           justification, approved_at, expires_at, active
    FROM {catalog}.{schema}.exceptions
    WHERE exception_id = '{exception_id}'
"""))
