"""Policies Table — persists active policy definitions to Delta.

The policies table is consumed by v_tag_policy_coverage (CROSS JOIN) and
by any Hub dashboard that needs to display policy metadata alongside
violation data.
"""

from datetime import datetime, timezone

from pyspark.sql import SparkSession
import pyspark.sql.types as T


POLICIES_SCHEMA = T.StructType([
    T.StructField("policy_id", T.StringType(), False),
    T.StructField("policy_name", T.StringType(), False),
    T.StructField("applies_to", T.StringType(), True),
    T.StructField("domain", T.StringType(), True),
    T.StructField("severity", T.StringType(), True),
    T.StructField("description", T.StringType(), True),
    T.StructField("remediation", T.StringType(), True),
    T.StructField("active", T.BooleanType(), True),
    T.StructField("updated_at", T.TimestampType(), True),
])


def ensure_policies_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the policies table if it doesn't exist."""
    table = f"{catalog}.{schema}.policies"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            policy_id STRING NOT NULL,
            policy_name STRING NOT NULL,
            applies_to STRING,
            domain STRING,
            severity STRING,
            description STRING,
            remediation STRING,
            active BOOLEAN,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)


def write_policies(spark: SparkSession, catalog: str, schema: str,
                   policies: list) -> int:
    """Overwrite the policies table with current active policy definitions.

    Args:
        policies: List of PolicyDefinition dataclasses from the policy engine.
            Each must have: policy_id, name, applies_to, domain, severity,
            description, remediation, active.

    Returns:
        Number of policies written.
    """
    if not policies:
        return 0

    ensure_policies_table(spark, catalog, schema)
    table = f"{catalog}.{schema}.policies"
    now = datetime.now(timezone.utc)

    rows = [
        (p.policy_id, p.name, p.applies_to, p.domain, p.severity,
         p.description, p.remediation, p.active, now)
        for p in policies
    ]

    df = spark.createDataFrame(rows, schema=POLICIES_SCHEMA)
    df.write.mode("overwrite").saveAsTable(table)
    return len(rows)
