"""DDL helpers for guardrails-managed Delta tables.

Called on server startup to ensure required tables exist.
"""

from databricks.sdk import WorkspaceClient


def ensure_agent_audit_log_table(w: WorkspaceClient, schema: str) -> None:
    """Create agent_audit_log table if it doesn't exist.

    Records agent actions (data_access, data_export, etc.) for
    compliance review. Append-only, CDF-enabled.

    Args:
        schema: Fully-qualified schema name (catalog.schema).
    """
    w.statement_execution.execute_statement(
        warehouse_id=None,  # uses serverless by default
        statement=f"""
            CREATE TABLE IF NOT EXISTS {schema}.agent_audit_log (
                log_id        STRING NOT NULL,
                agent_id      STRING NOT NULL,
                action        STRING NOT NULL,
                target        STRING NOT NULL,
                details       MAP<STRING, STRING>,
                classification STRING,
                user          STRING,
                session_id    STRING,
                logged_at     TIMESTAMP NOT NULL
            )
            USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """,
        wait_timeout="30s",
    )
