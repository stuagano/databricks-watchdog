"""Remediation Compliance Views — dashboards for the remediation pipeline.

Four views measuring the remediation funnel, trends, agent effectiveness,
and reviewer workload. All are regular views (not materialized).
"""

from pyspark.sql import SparkSession


def ensure_remediation_views(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create or replace all remediation compliance views."""
    _ensure_remediation_funnel_view(spark, catalog, schema)
    _ensure_remediation_trend_view(spark, catalog, schema)
    _ensure_agent_effectiveness_view(spark, catalog, schema)
    _ensure_reviewer_load_view(spark, catalog, schema)


def _ensure_remediation_funnel_view(spark: SparkSession, catalog: str,
                                     schema: str) -> None:
    """v_remediation_funnel: counts at each pipeline stage."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_remediation_funnel AS
        SELECT
            COUNT(DISTINCT v.violation_id) AS total_violations,
            COUNT(DISTINCT CASE WHEN v.remediation_status IS NOT NULL
                AND v.remediation_status != 'none' THEN v.violation_id END)
                AS with_remediation,
            COUNT(DISTINCT CASE WHEN p.status = 'pending_review' THEN p.proposal_id END)
                AS pending_review,
            COUNT(DISTINCT CASE WHEN p.status = 'approved' THEN p.proposal_id END)
                AS approved,
            COUNT(DISTINCT CASE WHEN p.status = 'applied' THEN p.proposal_id END)
                AS applied,
            COUNT(DISTINCT CASE WHEN p.status = 'verified' THEN p.proposal_id END)
                AS verified,
            COUNT(DISTINCT CASE WHEN p.status = 'verification_failed' THEN p.proposal_id END)
                AS verification_failed,
            COUNT(DISTINCT CASE WHEN p.status = 'rejected' THEN p.proposal_id END)
                AS rejected
        FROM {catalog}.{schema}.violations v
        LEFT JOIN {catalog}.{schema}.remediation_proposals p
            ON v.violation_id = p.violation_id
        WHERE v.status = 'open'
    """)


def _ensure_remediation_trend_view(spark: SparkSession, catalog: str,
                                    schema: str) -> None:
    """v_remediation_trend: compliance delta from remediation vs organic."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_remediation_trend AS
        SELECT
            DATE(p.created_at) AS proposal_date,
            COUNT(DISTINCT CASE WHEN p.status = 'verified' THEN p.proposal_id END)
                AS remediation_resolved,
            COUNT(DISTINCT CASE WHEN p.status = 'verification_failed' THEN p.proposal_id END)
                AS remediation_failed,
            COUNT(DISTINCT CASE WHEN p.status IN ('pending_review', 'approved', 'applied')
                THEN p.proposal_id END)
                AS remediation_in_progress
        FROM {catalog}.{schema}.remediation_proposals p
        GROUP BY DATE(p.created_at)
        ORDER BY proposal_date DESC
    """)


def _ensure_agent_effectiveness_view(spark: SparkSession, catalog: str,
                                      schema: str) -> None:
    """v_agent_effectiveness: per-agent scorecard."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_agent_effectiveness AS
        SELECT
            p.agent_id,
            p.agent_version,
            COUNT(*) AS total_proposals,
            COUNT(CASE WHEN p.status = 'verified' THEN 1 END) AS verified,
            COUNT(CASE WHEN p.status = 'verification_failed' THEN 1 END) AS failed,
            COUNT(CASE WHEN p.status = 'rejected' THEN 1 END) AS rejected,
            ROUND(
                COUNT(CASE WHEN p.status = 'verified' THEN 1 END) * 100.0
                / NULLIF(COUNT(CASE WHEN p.status IN ('verified', 'verification_failed') THEN 1 END), 0),
                1
            ) AS precision_pct,
            ROUND(AVG(p.confidence), 3) AS avg_confidence
        FROM {catalog}.{schema}.remediation_proposals p
        GROUP BY p.agent_id, p.agent_version
        ORDER BY total_proposals DESC
    """)


def _ensure_reviewer_load_view(spark: SparkSession, catalog: str,
                                schema: str) -> None:
    """v_reviewer_load: open queue depth per reviewer."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_reviewer_load AS
        SELECT
            r.reviewer,
            COUNT(DISTINCT CASE WHEN p.status = 'pending_review' THEN p.proposal_id END)
                AS pending_reviews,
            COUNT(DISTINCT CASE WHEN r.decision = 'approved' THEN r.review_id END)
                AS total_approved,
            COUNT(DISTINCT CASE WHEN r.decision = 'rejected' THEN r.review_id END)
                AS total_rejected,
            COUNT(DISTINCT CASE WHEN r.decision = 'reassigned' THEN r.review_id END)
                AS total_reassigned,
            COUNT(DISTINCT r.review_id) AS total_reviews
        FROM {catalog}.{schema}.remediation_reviews r
        LEFT JOIN {catalog}.{schema}.remediation_proposals p
            ON r.proposal_id = p.proposal_id
        GROUP BY r.reviewer
        ORDER BY pending_reviews DESC
    """)
