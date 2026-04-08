"""Semantic Views — ontology-shaped compliance views for Ontos and Genie.

Six views are created in the watchdog schema after each evaluate run:

  v_resource_compliance
    One row per (resource_id, class_name). Starting point for navigating
    from a specific resource to its compliance posture within each class
    it belongs to. Ontos Catalog Commander can join to this to annotate
    UC assets with their violation counts.

  v_class_compliance
    One row per ontology class. Aggregated violation counts and compliance
    rate per class — answers "how are GoldTables doing?", "how are
    PiiAssets doing?". The primary surface for Ontos dashboard grouping.

  v_domain_compliance
    One row per compliance domain (CostGovernance, SecurityGovernance, etc.).
    Quick executive posture view by governance domain.

  v_tag_policy_coverage
    One row per (resource_id, policy_id). Shows per-resource tag policy
    compliance state — which policies are satisfied, violated, or not
    evaluated for each resource.

  v_data_classification_summary
    One row per catalog. Aggregated classification posture — % classified,
    % with steward, % with sensitive data, and ontology classification
    coverage.

  v_dq_monitoring_coverage
    One row per table. Shows which tables have DQM, LHM, both, or neither,
    along with anomaly counts and ontology class assignments.

All six are regular views (not materialized) so they always reflect the
current state of the underlying tables.
"""

from pyspark.sql import SparkSession


def ensure_semantic_views(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create or replace the six semantic compliance views.

    Called after evaluate_policies so the views are always fresh after
    each scan. Idempotent — safe to call on every run.
    """
    _ensure_resource_compliance_view(spark, catalog, schema)
    _ensure_class_compliance_view(spark, catalog, schema)
    _ensure_domain_compliance_view(spark, catalog, schema)
    _ensure_tag_policy_coverage_view(spark, catalog, schema)
    _ensure_data_classification_summary_view(spark, catalog, schema)
    _ensure_dq_monitoring_coverage_view(spark, catalog, schema)


def _ensure_resource_compliance_view(spark: SparkSession, catalog: str,
                                      schema: str) -> None:
    """v_resource_compliance: per-resource posture within each assigned class.

    One row per (resource_id, class_name). A resource assigned to both
    GoldTable and PiiTable appears in both rows so class-level roll-ups
    count it correctly.

    Ontos use: join to UC assets on resource_id to show class-aware
    violation badges in Catalog Commander.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_resource_compliance AS
        SELECT
            rc.resource_id,
            rc.resource_name,
            rc.resource_type,
            rc.owner,
            rc.class_name,
            rc.class_ancestors,
            rc.root_class,
            COUNT(CASE WHEN v.status = 'open' THEN 1 END)
                AS open_violations,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN 1 END)
                AS critical_open,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'high' THEN 1 END)
                AS high_open,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'medium' THEN 1 END)
                AS medium_open,
            COUNT(CASE WHEN v.status = 'exception' THEN 1 END)
                AS excepted_violations,
            MIN(CASE WHEN v.status = 'open' THEN v.first_detected END)
                AS oldest_open_violation,
            MAX(v.last_detected)
                AS last_violation_at,
            CASE
                WHEN COUNT(CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN 1 END) > 0
                    THEN 'critical'
                WHEN COUNT(CASE WHEN v.status = 'open' AND v.severity = 'high' THEN 1 END) > 0
                    THEN 'high'
                WHEN COUNT(CASE WHEN v.status = 'open' THEN 1 END) > 0
                    THEN 'open'
                ELSE 'clean'
            END AS compliance_status
        FROM (
            SELECT DISTINCT resource_id, resource_name, resource_type, owner,
                            class_name, class_ancestors, root_class
            FROM {catalog}.{schema}.resource_classifications
        ) rc
        LEFT JOIN {catalog}.{schema}.violations v
            ON rc.resource_id = v.resource_id
        GROUP BY rc.resource_id, rc.resource_name, rc.resource_type, rc.owner,
                 rc.class_name, rc.class_ancestors, rc.root_class
    """)


def _ensure_class_compliance_view(spark: SparkSession, catalog: str,
                                   schema: str) -> None:
    """v_class_compliance: aggregated compliance posture per ontology class.

    One row per class_name. Rolls violations up through the class hierarchy
    so DataAsset counts include all tables, volumes, catalogs, and schemas.

    Ontos use: primary surface for the compliance dashboard — group by
    root_class for executive roll-ups, by class_name for drill-down.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_class_compliance AS
        SELECT
            rc.class_name,
            rc.root_class,
            COUNT(DISTINCT rc.resource_id)
                AS total_resources,
            COUNT(DISTINCT CASE WHEN v.status = 'open' THEN rc.resource_id END)
                AS resources_with_open_violations,
            ROUND(
                100.0 * (
                    COUNT(DISTINCT rc.resource_id) -
                    COUNT(DISTINCT CASE WHEN v.status = 'open' THEN rc.resource_id END)
                ) / NULLIF(COUNT(DISTINCT rc.resource_id), 0),
                1
            ) AS compliance_pct,
            COUNT(CASE WHEN v.status = 'open' THEN 1 END)
                AS open_violations,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN 1 END)
                AS critical_open,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'high' THEN 1 END)
                AS high_open,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'medium' THEN 1 END)
                AS medium_open,
            COUNT(CASE WHEN v.status = 'exception' THEN 1 END)
                AS excepted_violations,
            COUNT(CASE WHEN v.status = 'resolved' THEN 1 END)
                AS resolved_violations
        FROM (
            SELECT DISTINCT resource_id, class_name, root_class
            FROM {catalog}.{schema}.resource_classifications
        ) rc
        LEFT JOIN {catalog}.{schema}.violations v
            ON rc.resource_id = v.resource_id
        GROUP BY rc.class_name, rc.root_class
        ORDER BY open_violations DESC
    """)


def _ensure_domain_compliance_view(spark: SparkSession, catalog: str,
                                    schema: str) -> None:
    """v_domain_compliance: aggregated compliance posture per governance domain.

    One row per domain (CostGovernance, SecurityGovernance, DataQuality,
    OperationalGovernance, RegulatoryCompliance). Enriched with class
    breakdown so you can see which ontology classes are driving violations
    in each domain.

    Ontos use: executive compliance summary by governance pillar.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_domain_compliance AS
        SELECT
            v.domain,
            COUNT(DISTINCT v.resource_id)
                AS resources_affected,
            COUNT(CASE WHEN v.status = 'open' THEN 1 END)
                AS open_violations,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN 1 END)
                AS critical_open,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'high' THEN 1 END)
                AS high_open,
            COUNT(CASE WHEN v.status = 'exception' THEN 1 END)
                AS excepted_violations,
            COUNT(CASE WHEN v.status = 'resolved' THEN 1 END)
                AS resolved_violations,
            COLLECT_SET(CASE WHEN v.status = 'open' THEN v.resource_classes END)
                AS classes_with_open_violations
        FROM {catalog}.{schema}.violations v
        WHERE v.domain IS NOT NULL
        GROUP BY v.domain
        ORDER BY open_violations DESC
    """)


def _ensure_tag_policy_coverage_view(spark: SparkSession, catalog: str,
                                      schema: str) -> None:
    """v_tag_policy_coverage: per-resource tag policy compliance state.

    One row per (resource_id, policy_id). Answers "which tag-based policies
    are satisfied, violated, or not evaluated for each resource?" by crossing
    the latest resource inventory with active policies in the SecurityGovernance
    and DataClassification domains.

    Includes exception status so dashboards can distinguish between violations
    that are actively open vs those with approved waivers.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_tag_policy_coverage AS
        SELECT
            ri.resource_id,
            ri.resource_type,
            ri.resource_name,
            ri.owner,
            p.policy_id,
            p.policy_name,
            p.severity,
            CASE
                WHEN sr.result = 'pass' THEN 'satisfied'
                WHEN sr.result = 'fail' THEN 'violated'
                ELSE 'not_evaluated'
            END AS coverage_status,
            v.status AS violation_status,
            v.first_detected,
            v.last_detected,
            e.active AS has_exception,
            e.expires_at AS exception_expires
        FROM {catalog}.{schema}.resource_inventory ri
        CROSS JOIN {catalog}.{schema}.policies p
        LEFT JOIN {catalog}.{schema}.scan_results sr
            ON ri.resource_id = sr.resource_id
            AND p.policy_id = sr.policy_id
            AND sr.scan_id = (SELECT MAX(scan_id) FROM {catalog}.{schema}.scan_results)
        LEFT JOIN {catalog}.{schema}.violations v
            ON ri.resource_id = v.resource_id
            AND p.policy_id = v.policy_id
            AND v.status IN ('open', 'exception')
        LEFT JOIN {catalog}.{schema}.exceptions e
            ON ri.resource_id = e.resource_id
            AND p.policy_id = e.policy_id
            AND e.active = true
        WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {catalog}.{schema}.resource_inventory)
            AND p.active = true
            AND p.domain IN ('SecurityGovernance', 'DataClassification')
    """)


def _ensure_data_classification_summary_view(spark: SparkSession, catalog: str,
                                              schema: str) -> None:
    """v_data_classification_summary: aggregated classification posture by catalog.

    One row per catalog (ri.domain). Shows % of tables that are classified,
    have a data steward, contain sensitive data, and are covered by ontology
    classification. Useful for executive dashboards tracking data governance
    maturity across the lakehouse.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_data_classification_summary AS
        SELECT
            ri.domain AS catalog_name,
            COUNT(DISTINCT ri.resource_id) AS total_tables,
            COUNT(DISTINCT CASE WHEN ri.tags['data_classification'] IS NOT NULL THEN ri.resource_id END)
                AS classified_tables,
            COUNT(DISTINCT CASE WHEN ri.tags['data_steward'] IS NOT NULL THEN ri.resource_id END)
                AS tables_with_steward,
            COUNT(DISTINCT CASE WHEN ri.tags['data_classification'] IN ('pii', 'confidential', 'restricted') THEN ri.resource_id END)
                AS sensitive_tables,
            COUNT(DISTINCT CASE WHEN rc.class_name IN ('PiiAsset', 'ConfidentialAsset') THEN ri.resource_id END)
                AS ontology_classified,
            ROUND(
                COUNT(DISTINCT CASE WHEN ri.tags['data_classification'] IS NOT NULL THEN ri.resource_id END) * 100.0
                / NULLIF(COUNT(DISTINCT ri.resource_id), 0), 1
            ) AS classification_pct,
            ROUND(
                COUNT(DISTINCT CASE WHEN ri.tags['data_steward'] IS NOT NULL THEN ri.resource_id END) * 100.0
                / NULLIF(COUNT(DISTINCT CASE WHEN ri.tags['data_classification'] IS NOT NULL THEN ri.resource_id END), 0), 1
            ) AS stewardship_pct
        FROM {catalog}.{schema}.resource_inventory ri
        LEFT JOIN {catalog}.{schema}.resource_classifications rc
            ON ri.resource_id = rc.resource_id
            AND rc.scan_id = ri.scan_id
        WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {catalog}.{schema}.resource_inventory)
            AND ri.resource_type = 'table'
        GROUP BY ri.domain
    """)


def _ensure_dq_monitoring_coverage_view(spark: SparkSession, catalog: str,
                                         schema: str) -> None:
    """v_dq_monitoring_coverage: DQ monitoring status per table.

    One row per table. Shows which tables have DQM (Data Quality Monitoring),
    LHM (Lakehouse Monitoring), both, or neither. Includes anomaly counts
    and ontology class for dashboard filtering.

    Tags dqm_enabled/lhm_enabled are enriched by the crawler's DQ system
    table crawlers during each scan.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_dq_monitoring_coverage AS
        SELECT
            ri.resource_id,
            ri.resource_type,
            ri.resource_name,
            ri.owner,
            ri.domain AS catalog_name,
            COALESCE(ri.tags['dqm_enabled'], 'false') AS dqm_enabled,
            COALESCE(ri.tags['lhm_enabled'], 'false') AS lhm_enabled,
            CASE
                WHEN ri.tags['dqm_enabled'] = 'true' AND ri.tags['lhm_enabled'] = 'true' THEN 'both'
                WHEN ri.tags['dqm_enabled'] = 'true' THEN 'dqm_only'
                WHEN ri.tags['lhm_enabled'] = 'true' THEN 'lhm_only'
                ELSE 'none'
            END AS monitoring_status,
            ri.tags['dqm_anomalies'] AS dqm_anomalies,
            ri.tags['dqm_metrics_checked'] AS dqm_metrics_checked,
            rc.class_name AS ontology_class
        FROM {catalog}.{schema}.resource_inventory ri
        LEFT JOIN {catalog}.{schema}.resource_classifications rc
            ON ri.resource_id = rc.resource_id
            AND rc.scan_id = ri.scan_id
        WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {catalog}.{schema}.resource_inventory)
            AND ri.resource_type = 'table'
    """)
