# Databricks notebook source
# MAGIC %md
# MAGIC # Watchdog — Revoke Policy Exception
# MAGIC
# MAGIC Deactivate an existing exception. The violation will revert to `open` on the next scan.

# COMMAND ----------

dbutils.widgets.text("exception_id", "", "Exception ID")
dbutils.widgets.text("reason", "", "Revocation reason (optional)")

# COMMAND ----------

exception_id = dbutils.widgets.get("exception_id")
reason = dbutils.widgets.get("reason")

assert exception_id, "exception_id is required"

# COMMAND ----------

catalog = spark.conf.get("spark.databricks.watchdog.catalog", "platform")
schema = spark.conf.get("spark.databricks.watchdog.schema", "watchdog")

# Show the exception being revoked
display(spark.sql(f"""
    SELECT exception_id, resource_id, policy_id, approved_by,
           justification, approved_at, expires_at, active
    FROM {catalog}.{schema}.exceptions
    WHERE exception_id = '{exception_id}'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revoke

# COMMAND ----------

rows_affected = spark.sql(f"""
    UPDATE {catalog}.{schema}.exceptions
    SET active = false
    WHERE exception_id = '{exception_id}' AND active = true
""").first()[0]

if rows_affected > 0:
    print(f"Exception {exception_id} revoked.")
    if reason:
        print(f"  Reason: {reason}")
else:
    print(f"No active exception found with ID {exception_id}.")

# COMMAND ----------

display(spark.sql(f"""
    SELECT exception_id, resource_id, policy_id, approved_by,
           justification, approved_at, expires_at, active
    FROM {catalog}.{schema}.exceptions
    WHERE exception_id = '{exception_id}'
"""))
