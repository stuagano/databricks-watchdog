"""Remediation Delta tables — agents registry and proposals.

Three tables for the foundation layer:
  - remediation_agents: registry of available agents
  - remediation_proposals: proposed fixes with evidence trail
"""

from pyspark.sql import SparkSession


def ensure_remediation_agents_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the remediation_agents registry table."""
    table = f"{catalog}.{schema}.remediation_agents"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            agent_id STRING NOT NULL,
            handles STRING NOT NULL,
            version STRING NOT NULL,
            model STRING,
            config_json STRING,
            permissions STRING,
            active BOOLEAN DEFAULT true,
            registered_at TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported'
        )
    """)


def ensure_remediation_proposals_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the remediation_proposals table."""
    table = f"{catalog}.{schema}.remediation_proposals"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            proposal_id STRING NOT NULL,
            violation_id STRING NOT NULL,
            agent_id STRING NOT NULL,
            agent_version STRING NOT NULL,
            status STRING NOT NULL DEFAULT 'pending_review',
            proposed_sql STRING,
            confidence DOUBLE,
            context_json STRING,
            llm_prompt_hash STRING,
            citations STRING,
            created_at TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported'
        )
    """)


def register_agent(spark: SparkSession, catalog: str, schema: str,
                    agent) -> None:
    """Register an agent in the remediation_agents table.

    Args:
        agent: Object satisfying the RemediationAgent protocol.
    """
    import json
    from datetime import datetime, timezone

    ensure_remediation_agents_table(spark, catalog, schema)
    table = f"{catalog}.{schema}.remediation_agents"

    import pyspark.sql.types as T

    schema_def = T.StructType([
        T.StructField("agent_id", T.StringType(), False),
        T.StructField("handles", T.StringType(), False),
        T.StructField("version", T.StringType(), False),
        T.StructField("model", T.StringType(), True),
        T.StructField("config_json", T.StringType(), True),
        T.StructField("permissions", T.StringType(), True),
        T.StructField("active", T.BooleanType(), True),
        T.StructField("registered_at", T.TimestampType(), False),
    ])

    row = [(
        agent.agent_id,
        ",".join(agent.handles),
        agent.version,
        agent.model,
        None,
        None,
        True,
        datetime.now(timezone.utc),
    )]

    df = spark.createDataFrame(row, schema=schema_def)
    df.write.mode("append").saveAsTable(table)
