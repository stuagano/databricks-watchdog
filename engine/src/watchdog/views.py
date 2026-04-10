"""Semantic Views — ontology-shaped compliance views for Ontos and Genie.

Twelve views are created in the watchdog schema after each evaluate run:

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

  v_compliance_trend
    One row per scan. Reads from scan_summary with LAG() deltas so
    dashboards can show posture direction over 30/60/90 day windows.

  v_agent_inventory
    One row per agent. Governance status, source, owner, violation counts.

  v_agent_execution_compliance
    One row per agent_execution. Usage metrics, violation status, risk flags.

  v_agent_risk_heatmap
    One row per agent. Cross-tabulates data sensitivity × access frequency
    for risk scoring.

All twelve are regular views (not materialized) so they always reflect the
current state of the underlying tables.
"""

from pyspark.sql import SparkSession


def ensure_semantic_views(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create or replace all semantic compliance views.

    Called after evaluate_policies so the views are always fresh after
    each scan. Idempotent — safe to call on every run.
    """
    _ensure_resource_compliance_view(spark, catalog, schema)
    _ensure_class_compliance_view(spark, catalog, schema)
    _ensure_domain_compliance_view(spark, catalog, schema)
    _ensure_tag_policy_coverage_view(spark, catalog, schema)
    _ensure_data_classification_summary_view(spark, catalog, schema)
    _ensure_dq_monitoring_coverage_view(spark, catalog, schema)
    _ensure_cross_metastore_compliance_view(spark, catalog, schema)
    _ensure_cross_metastore_inventory_view(spark, catalog, schema)
    _ensure_compliance_trend_view(spark, catalog, schema)
    _ensure_agent_inventory_view(spark, catalog, schema)
    _ensure_agent_execution_compliance_view(spark, catalog, schema)
    _ensure_agent_risk_heatmap_view(spark, catalog, schema)


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


def _ensure_cross_metastore_compliance_view(spark: SparkSession, catalog: str,
                                             schema: str) -> None:
    """v_cross_metastore_compliance: compliance posture per metastore.

    One row per metastore_id. Shows total resources, violation counts, and
    compliance percentage. Useful for comparing governance posture across
    metastores in multi-metastore deployments.

    Each metastore's latest scan is used independently via a correlated
    subquery on metastore_id.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_cross_metastore_compliance AS
        SELECT
            ri.metastore_id,
            COUNT(DISTINCT ri.resource_id) as total_resources,
            COUNT(DISTINCT CASE WHEN v.status = 'open' THEN v.resource_id END) as resources_with_violations,
            COUNT(DISTINCT CASE WHEN v.status = 'open' THEN v.violation_id END) as open_violations,
            COUNT(DISTINCT CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN v.violation_id END) as critical,
            COUNT(DISTINCT CASE WHEN v.status = 'open' AND v.severity = 'high' THEN v.violation_id END) as high,
            ROUND(
                (COUNT(DISTINCT ri.resource_id) - COUNT(DISTINCT CASE WHEN v.status = 'open' THEN v.resource_id END)) * 100.0
                / NULLIF(COUNT(DISTINCT ri.resource_id), 0), 1
            ) as compliance_pct
        FROM {catalog}.{schema}.resource_inventory ri
        LEFT JOIN {catalog}.{schema}.violations v ON ri.resource_id = v.resource_id
        WHERE ri.scan_id = (
            SELECT MAX(scan_id)
            FROM {catalog}.{schema}.resource_inventory
            WHERE metastore_id = ri.metastore_id
        )
        GROUP BY ri.metastore_id
    """)


def _ensure_cross_metastore_inventory_view(spark: SparkSession, catalog: str,
                                            schema: str) -> None:
    """v_cross_metastore_inventory: resource counts per metastore and type.

    One row per (metastore_id, resource_type). Shows resource count and
    distinct owner count. Useful for understanding resource distribution
    across metastores.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_cross_metastore_inventory AS
        SELECT
            metastore_id,
            resource_type,
            COUNT(*) as resource_count,
            COUNT(DISTINCT owner) as distinct_owners
        FROM {catalog}.{schema}.resource_inventory
        WHERE scan_id = (
            SELECT MAX(scan_id)
            FROM {catalog}.{schema}.resource_inventory
            WHERE metastore_id = resource_inventory.metastore_id
        )
        GROUP BY metastore_id, resource_type
        ORDER BY metastore_id, resource_count DESC
    """)


def _ensure_compliance_trend_view(spark: SparkSession, catalog: str,
                                   schema: str) -> None:
    """v_compliance_trend: compliance posture over time from scan_summary.

    One row per scan. Enriched with deltas from the previous scan so
    dashboards can show direction (improving/declining/stable) and trend
    lines over 30/60/90 day windows.

    Uses LAG() window function to compute scan-over-scan changes.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_compliance_trend AS
        SELECT
            scan_id,
            scanned_at,
            metastore_id,
            total_resources,
            total_policies_evaluated,
            total_classifications,
            open_violations,
            resolved_violations,
            exception_violations,
            new_violations,
            newly_resolved,
            critical_open,
            high_open,
            medium_open,
            low_open,
            compliance_pct,
            -- Deltas from previous scan
            open_violations - LAG(open_violations, 1, open_violations)
                OVER (PARTITION BY metastore_id ORDER BY scanned_at)
                AS open_violations_delta,
            compliance_pct - LAG(compliance_pct, 1, compliance_pct)
                OVER (PARTITION BY metastore_id ORDER BY scanned_at)
                AS compliance_pct_delta,
            total_resources - LAG(total_resources, 1, total_resources)
                OVER (PARTITION BY metastore_id ORDER BY scanned_at)
                AS resources_delta,
            critical_open - LAG(critical_open, 1, critical_open)
                OVER (PARTITION BY metastore_id ORDER BY scanned_at)
                AS critical_delta,
            -- Direction indicator
            CASE
                WHEN compliance_pct > LAG(compliance_pct, 1, compliance_pct)
                    OVER (PARTITION BY metastore_id ORDER BY scanned_at)
                    THEN 'improving'
                WHEN compliance_pct < LAG(compliance_pct, 1, compliance_pct)
                    OVER (PARTITION BY metastore_id ORDER BY scanned_at)
                    THEN 'declining'
                ELSE 'stable'
            END AS trend_direction,
            -- Rolling averages (approximate via last N scans)
            AVG(compliance_pct)
                OVER (PARTITION BY metastore_id ORDER BY scanned_at
                      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
                AS compliance_pct_7scan_avg,
            AVG(open_violations)
                OVER (PARTITION BY metastore_id ORDER BY scanned_at
                      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
                AS open_violations_30scan_avg
        FROM {catalog}.{schema}.scan_summary
        ORDER BY scanned_at DESC
    """)


def _ensure_agent_inventory_view(spark: SparkSession, catalog: str,
                                  schema: str) -> None:
    """v_agent_inventory: agent governance posture.

    One row per agent. Shows source (Apps vs serving endpoint), owner,
    governance metadata, violation counts, and compliance status. Agents
    without owner or audit logging are flagged as ungoverned.

    Dashboard use: agent inventory table and governed/ungoverned KPIs.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_agent_inventory AS
        SELECT
            ri.resource_id,
            ri.resource_name,
            ri.owner,
            ri.metadata['app_name'] AS app_name,
            ri.metadata['endpoint_name'] AS endpoint_name,
            CASE
                WHEN ri.metadata['app_name'] IS NOT NULL THEN 'databricks_app'
                WHEN ri.metadata['endpoint_name'] IS NOT NULL THEN 'serving_endpoint'
                ELSE 'unknown'
            END AS agent_source,
            ri.tags['agent_owner'] AS agent_owner,
            ri.tags['audit_logging_enabled'] AS audit_logging,
            ri.tags['environment'] AS environment,
            ri.tags['accessed_pii'] AS accessed_pii,
            ri.tags['used_external_tool'] AS used_external_tool,
            ri.tags['exported_data'] AS exported_data,
            CASE
                WHEN ri.tags['agent_owner'] IS NOT NULL
                     AND ri.tags['audit_logging_enabled'] = 'true'
                    THEN 'governed'
                WHEN ri.tags['agent_owner'] IS NOT NULL
                    THEN 'partially_governed'
                ELSE 'ungoverned'
            END AS governance_status,
            COUNT(CASE WHEN v.status = 'open' THEN 1 END)
                AS open_violations,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN 1 END)
                AS critical_violations,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'high' THEN 1 END)
                AS high_violations,
            COUNT(CASE WHEN v.status = 'exception' THEN 1 END)
                AS excepted_violations,
            COLLECT_SET(CASE WHEN v.status = 'open' THEN v.policy_id END)
                AS violated_policies,
            COLLECT_SET(rc.class_name)
                AS ontology_classes
        FROM {catalog}.{schema}.resource_inventory ri
        LEFT JOIN {catalog}.{schema}.violations v
            ON ri.resource_id = v.resource_id
        LEFT JOIN {catalog}.{schema}.resource_classifications rc
            ON ri.resource_id = rc.resource_id
            AND rc.scan_id = ri.scan_id
        WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {catalog}.{schema}.resource_inventory)
            AND ri.resource_type = 'agent'
        GROUP BY ri.resource_id, ri.resource_name, ri.owner,
                 ri.metadata['app_name'], ri.metadata['endpoint_name'],
                 ri.tags['agent_owner'], ri.tags['audit_logging_enabled'],
                 ri.tags['environment'], ri.tags['accessed_pii'],
                 ri.tags['used_external_tool'], ri.tags['exported_data']
    """)


def _ensure_agent_execution_compliance_view(spark: SparkSession, catalog: str,
                                             schema: str) -> None:
    """v_agent_execution_compliance: per-execution compliance posture.

    One row per agent_execution. Shows usage metrics (request count, tokens),
    violation status, and risk flags. Joins to the parent agent for context.

    Dashboard use: execution detail table, PII access patterns, top consumers.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_agent_execution_compliance AS
        SELECT
            ex.resource_id AS execution_id,
            ex.resource_name AS execution_name,
            ex.metadata['endpoint_name'] AS endpoint_name,
            ex.metadata['requester'] AS requester,
            CAST(ex.metadata['request_count'] AS BIGINT) AS request_count,
            CAST(ex.metadata['total_input_tokens'] AS BIGINT) AS total_input_tokens,
            CAST(ex.metadata['total_output_tokens'] AS BIGINT) AS total_output_tokens,
            CAST(ex.metadata['error_count'] AS BIGINT) AS error_count,
            ex.tags['accessed_pii'] AS accessed_pii,
            ex.tags['used_external_tool'] AS used_external_tool,
            ex.tags['exported_data'] AS exported_data,
            ex.tags['high_volume'] AS high_volume,
            ex.tags['has_errors'] AS has_errors,
            ex.owner,
            COUNT(CASE WHEN v.status = 'open' THEN 1 END)
                AS open_violations,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN 1 END)
                AS critical_violations,
            COLLECT_SET(CASE WHEN v.status = 'open' THEN v.policy_id END)
                AS violated_policies,
            CASE
                WHEN COUNT(CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN 1 END) > 0
                    THEN 'critical'
                WHEN COUNT(CASE WHEN v.status = 'open' AND v.severity = 'high' THEN 1 END) > 0
                    THEN 'high'
                WHEN COUNT(CASE WHEN v.status = 'open' THEN 1 END) > 0
                    THEN 'violation'
                ELSE 'compliant'
            END AS compliance_status
        FROM {catalog}.{schema}.resource_inventory ex
        LEFT JOIN {catalog}.{schema}.violations v
            ON ex.resource_id = v.resource_id
        WHERE ex.scan_id = (SELECT MAX(scan_id) FROM {catalog}.{schema}.resource_inventory)
            AND ex.resource_type = 'agent_execution'
        GROUP BY ex.resource_id, ex.resource_name, ex.owner,
                 ex.metadata['endpoint_name'], ex.metadata['requester'],
                 ex.metadata['request_count'], ex.metadata['total_input_tokens'],
                 ex.metadata['total_output_tokens'], ex.metadata['error_count'],
                 ex.tags['accessed_pii'], ex.tags['used_external_tool'],
                 ex.tags['exported_data'], ex.tags['high_volume'],
                 ex.tags['has_errors']
    """)


def _ensure_agent_risk_heatmap_view(spark: SparkSession, catalog: str,
                                     schema: str) -> None:
    """v_agent_risk_heatmap: agent risk scoring by data sensitivity × activity.

    One row per agent. Combines execution volume (from agent_execution records)
    with data sensitivity flags to produce a risk tier (critical/high/medium/low).

    Dashboard use: risk heatmap, top-risk agents, risk distribution chart.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_agent_risk_heatmap AS
        WITH agent_activity AS (
            SELECT
                ex.metadata['endpoint_name'] AS endpoint_name,
                SUM(CAST(ex.metadata['request_count'] AS BIGINT)) AS total_requests,
                SUM(CAST(ex.metadata['total_input_tokens'] AS BIGINT)) AS total_input_tokens,
                SUM(CAST(ex.metadata['total_output_tokens'] AS BIGINT)) AS total_output_tokens,
                COUNT(DISTINCT ex.metadata['requester']) AS unique_requesters,
                MAX(CASE WHEN ex.tags['accessed_pii'] = 'true' THEN 1 ELSE 0 END) AS any_pii_access,
                MAX(CASE WHEN ex.tags['used_external_tool'] = 'true' THEN 1 ELSE 0 END) AS any_external_access,
                MAX(CASE WHEN ex.tags['exported_data'] = 'true' THEN 1 ELSE 0 END) AS any_data_export,
                SUM(CAST(ex.metadata['error_count'] AS BIGINT)) AS total_errors
            FROM {catalog}.{schema}.resource_inventory ex
            WHERE ex.scan_id = (SELECT MAX(scan_id) FROM {catalog}.{schema}.resource_inventory)
                AND ex.resource_type = 'agent_execution'
            GROUP BY ex.metadata['endpoint_name']
        )
        SELECT
            ag.resource_id,
            ag.resource_name,
            ag.owner,
            CASE
                WHEN ag.metadata['app_name'] IS NOT NULL THEN 'databricks_app'
                ELSE 'serving_endpoint'
            END AS agent_source,
            ag.tags['agent_owner'] AS agent_owner,
            COALESCE(aa.total_requests, 0) AS total_requests,
            COALESCE(aa.total_input_tokens, 0) AS total_input_tokens,
            COALESCE(aa.unique_requesters, 0) AS unique_requesters,
            COALESCE(aa.any_pii_access, 0) AS pii_access,
            COALESCE(aa.any_external_access, 0) AS external_access,
            COALESCE(aa.any_data_export, 0) AS data_export,
            COALESCE(aa.total_errors, 0) AS total_errors,
            -- Sensitivity score: 0-3 based on flags
            COALESCE(aa.any_pii_access, 0)
                + COALESCE(aa.any_external_access, 0)
                + COALESCE(aa.any_data_export, 0)
                AS sensitivity_score,
            -- Volume tier based on request count
            CASE
                WHEN COALESCE(aa.total_requests, 0) >= 1000000 THEN 'very_high'
                WHEN COALESCE(aa.total_requests, 0) >= 100000 THEN 'high'
                WHEN COALESCE(aa.total_requests, 0) >= 10000 THEN 'medium'
                ELSE 'low'
            END AS volume_tier,
            -- Risk tier: sensitivity × volume
            CASE
                WHEN (COALESCE(aa.any_pii_access, 0) + COALESCE(aa.any_data_export, 0)) > 0
                     AND COALESCE(aa.total_requests, 0) >= 100000
                    THEN 'critical'
                WHEN (COALESCE(aa.any_pii_access, 0) + COALESCE(aa.any_external_access, 0)
                      + COALESCE(aa.any_data_export, 0)) > 0
                    THEN 'high'
                WHEN COALESCE(aa.total_requests, 0) >= 100000
                    THEN 'medium'
                ELSE 'low'
            END AS risk_tier,
            COUNT(CASE WHEN v.status = 'open' THEN 1 END) AS open_violations,
            COUNT(CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN 1 END)
                AS critical_violations
        FROM {catalog}.{schema}.resource_inventory ag
        LEFT JOIN agent_activity aa
            ON COALESCE(ag.metadata['endpoint_name'], ag.metadata['app_name']) = aa.endpoint_name
        LEFT JOIN {catalog}.{schema}.violations v
            ON ag.resource_id = v.resource_id
        WHERE ag.scan_id = (SELECT MAX(scan_id) FROM {catalog}.{schema}.resource_inventory)
            AND ag.resource_type = 'agent'
        GROUP BY ag.resource_id, ag.resource_name, ag.owner,
                 ag.metadata['app_name'], ag.metadata['endpoint_name'],
                 ag.tags['agent_owner'],
                 aa.total_requests, aa.total_input_tokens,
                 aa.unique_requesters, aa.any_pii_access,
                 aa.any_external_access, aa.any_data_export, aa.total_errors
    """)
