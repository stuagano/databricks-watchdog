"""Semantic Views — ontology-shaped compliance views for Ontos and Genie.

Three views are created in the watchdog schema after each evaluate run:

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

All three are regular views (not materialized) so they always reflect the
current state of the violations and resource_classifications tables.
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
    _ensure_cross_metastore_compliance_view(spark, catalog, schema)
    _ensure_cross_metastore_inventory_view(spark, catalog, schema)


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
