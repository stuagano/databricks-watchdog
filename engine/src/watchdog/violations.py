"""Violations Manager — deduplicates scan results into the violations table.

The scan_results table gets a new row for every (resource, policy) evaluation
on every scan. The violations table is the deduplicated view:
  - One row per (resource_id, policy_id) combination
  - Tracks first_detected, last_detected, status (open/resolved/exception)
  - MERGE logic: new failures → insert; existing failures → update last_detected;
    previously failing resources that now pass → mark resolved.

Exceptions (approved policy waivers) are respected: if a violation has an
active exception, it stays in 'exception' status even if still failing.

Ontos Integration:
  - violations table includes domain and resource_classes columns for
    Ontos compliance dashboard grouping and semantic linking
  - resource_classifications table provides per-scan class assignments
    that Ontos can join to its semantic models
"""

from datetime import datetime, timezone

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


def ensure_violations_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the violations table if it doesn't exist.

    Includes domain and resource_classes columns for Ontos dashboard integration.
    """
    table = f"{catalog}.{schema}.violations"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            violation_id STRING NOT NULL,
            resource_id STRING NOT NULL,
            resource_type STRING,
            resource_name STRING,
            policy_id STRING NOT NULL,
            severity STRING,
            domain STRING,
            detail STRING,
            remediation STRING,
            owner STRING,
            resource_classes STRING,
            metastore_id STRING,
            first_detected TIMESTAMP NOT NULL,
            last_detected TIMESTAMP NOT NULL,
            resolved_at TIMESTAMP,
            status STRING NOT NULL DEFAULT 'open',
            notified_at TIMESTAMP,
            remediation_status STRING
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.enableDeletionVectors' = 'true',
            'delta.feature.allowColumnDefaults' = 'supported'
        )
    """)


def ensure_exceptions_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the exceptions table if it doesn't exist."""
    table = f"{catalog}.{schema}.exceptions"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            exception_id STRING NOT NULL,
            resource_id STRING NOT NULL,
            policy_id STRING NOT NULL,
            approved_by STRING NOT NULL,
            justification STRING NOT NULL,
            approved_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP,
            active BOOLEAN DEFAULT true,
            metastore_id STRING
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported'
        )
    """)


def ensure_classifications_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the resource_classifications table for Ontos semantic linking.

    Stores the ontology class assignments from each scan. Ontos can join this
    to its semantic models to link Watchdog classifications to business concepts.
    """
    table = f"{catalog}.{schema}.resource_classifications"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            scan_id STRING NOT NULL,
            resource_id STRING NOT NULL,
            resource_type STRING,
            resource_name STRING,
            owner STRING,
            class_name STRING NOT NULL,
            class_ancestors STRING,
            root_class STRING,
            metastore_id STRING,
            classified_at TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.appendOnly' = 'true'
        )
    """)


def write_classifications(spark: SparkSession, catalog: str, schema: str,
                          scan_id: str, classifications: list[tuple],
                          metastore_id: str | None = None) -> int:
    """Write resource classification results to Delta.

    Args:
        classifications: List of (resource_id, resource_type, resource_name,
                         owner, class_name, class_ancestors, root_class, timestamp) tuples
        metastore_id: Optional metastore identifier for multi-metastore support.

    Returns:
        Number of classification rows written.
    """
    if not classifications:
        return 0

    ensure_classifications_table(spark, catalog, schema)
    table = f"{catalog}.{schema}.resource_classifications"

    rows = [(scan_id, *c, metastore_id) for c in classifications]
    _class_schema = T.StructType([
        T.StructField("scan_id", T.StringType()),
        T.StructField("resource_id", T.StringType()),
        T.StructField("resource_type", T.StringType()),
        T.StructField("resource_name", T.StringType()),
        T.StructField("owner", T.StringType()),
        T.StructField("class_name", T.StringType()),
        T.StructField("class_ancestors", T.StringType()),
        T.StructField("root_class", T.StringType()),
        T.StructField("classified_at", T.TimestampType()),
        T.StructField("metastore_id", T.StringType()),
    ])
    df = spark.createDataFrame(rows, schema=_class_schema)
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(table)
    return len(rows)


def merge_violations(spark: SparkSession, catalog: str, schema: str,
                     scan_id: str) -> dict:
    """Merge the latest scan results into the violations table.

    Logic:
      1. Get all FAIL results from the current scan (with domain + classes)
      2. MERGE into violations:
         - Match on (resource_id, policy_id)
         - Existing + still failing → update last_detected
         - New failure → insert with status='open'
      3. Resolve: any open violation whose (resource_id, policy_id) is NOT
         in the current scan's failures → mark status='resolved'
      4. Respect exceptions: violations with active exceptions keep
         status='exception' regardless of scan results

    Returns a summary dict with counts.
    """
    violations_table = f"{catalog}.{schema}.violations"
    scan_results_table = f"{catalog}.{schema}.scan_results"
    exceptions_table = f"{catalog}.{schema}.exceptions"
    inventory_table = f"{catalog}.{schema}.resource_inventory"

    ensure_violations_table(spark, catalog, schema)
    ensure_exceptions_table(spark, catalog, schema)

    now = datetime.now(timezone.utc)

    # Current scan failures with resource metadata + ontology fields.
    # Deduplicate by (resource_id, policy_id) to avoid MERGE conflicts
    # when the JOIN produces multiple rows per resource.
    current_failures = spark.sql(f"""
        SELECT resource_id, policy_id, details, domain, severity,
               resource_classes, resource_type, resource_name, owner, metastore_id
        FROM (
            SELECT
                sr.resource_id,
                sr.policy_id,
                sr.details,
                sr.domain,
                sr.severity,
                sr.resource_classes,
                ri.resource_type,
                ri.resource_name,
                ri.owner,
                ri.metastore_id,
                ROW_NUMBER() OVER (
                    PARTITION BY sr.resource_id, sr.policy_id
                    ORDER BY sr.evaluated_at DESC
                ) AS rn
            FROM {scan_results_table} sr
            LEFT JOIN {inventory_table} ri
                ON sr.resource_id = ri.resource_id AND ri.scan_id = '{scan_id}'
            WHERE sr.scan_id = '{scan_id}' AND sr.result = 'fail'
        ) WHERE rn = 1
    """)

    current_failures.createOrReplaceTempView("_watchdog_current_failures")

    # Load active exceptions for status override
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW _watchdog_active_exceptions AS
        SELECT resource_id, policy_id
        FROM {exceptions_table}
        WHERE active = true
          AND (expires_at IS NULL OR expires_at > current_timestamp())
    """)

    # MERGE: upsert failures into violations
    spark.sql(f"""
        MERGE INTO {violations_table} AS target
        USING (
            SELECT
                cf.*,
                CASE
                    WHEN ae.resource_id IS NOT NULL THEN 'exception'
                    ELSE 'open'
                END AS computed_status
            FROM _watchdog_current_failures cf
            LEFT JOIN _watchdog_active_exceptions ae
                ON cf.resource_id = ae.resource_id
                AND cf.policy_id = ae.policy_id
        ) AS source
        ON target.resource_id = source.resource_id
            AND target.policy_id = source.policy_id
        WHEN MATCHED THEN UPDATE SET
            last_detected = current_timestamp(),
            detail = source.details,
            severity = source.severity,
            domain = source.domain,
            resource_classes = source.resource_classes,
            owner = source.owner,
            status = CASE
                WHEN source.computed_status = 'exception' THEN 'exception'
                ELSE 'open'
            END,
            resolved_at = NULL
        WHEN NOT MATCHED THEN INSERT (
            violation_id, resource_id, resource_type, resource_name,
            policy_id, severity, domain, detail, remediation, owner,
            resource_classes, metastore_id, first_detected, last_detected, status
        ) VALUES (
            uuid(),
            source.resource_id,
            source.resource_type,
            source.resource_name,
            source.policy_id,
            source.severity,
            source.domain,
            source.details,
            NULL,
            source.owner,
            source.resource_classes,
            source.metastore_id,
            current_timestamp(),
            current_timestamp(),
            source.computed_status
        )
    """)

    # Resolve: mark open violations not in current failures as resolved.
    # Scoped to the current scan's metastore to prevent cross-metastore
    # resolution — scanning metastore A must not resolve metastore B's violations.
    metastore_id_from_scan = spark.sql(
        f"SELECT MAX(metastore_id) AS ms FROM _watchdog_current_failures"
    ).first()
    _ms_id = metastore_id_from_scan.ms if metastore_id_from_scan else None
    _ms_filter = f"AND v.metastore_id = '{_ms_id}'" if _ms_id else ""

    spark.sql(f"""
        MERGE INTO {violations_table} AS target
        USING (
            SELECT v.resource_id, v.policy_id
            FROM {violations_table} v
            LEFT JOIN _watchdog_current_failures cf
                ON v.resource_id = cf.resource_id
                AND v.policy_id = cf.policy_id
            WHERE v.status = 'open' AND cf.resource_id IS NULL
            {_ms_filter}
        ) AS resolved
        ON target.resource_id = resolved.resource_id
            AND target.policy_id = resolved.policy_id
        WHEN MATCHED THEN UPDATE SET
            status = 'resolved',
            resolved_at = current_timestamp()
    """)

    # Collect summary
    summary = spark.sql(f"""
        SELECT
            status,
            count(*) as cnt
        FROM {violations_table}
        GROUP BY status
    """).collect()

    counts = {row.status: row.cnt for row in summary}

    new_violations = spark.sql(f"""
        SELECT count(*) as cnt
        FROM {violations_table}
        WHERE first_detected = last_detected
          AND status IN ('open', 'exception')
    """).first().cnt

    return {
        "open": counts.get("open", 0),
        "resolved": counts.get("resolved", 0),
        "exception": counts.get("exception", 0),
        "new_this_scan": new_violations,
    }


def ensure_scan_summary_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the scan_summary table if it doesn't exist.

    Append-only, one row per scan. Captures posture metrics at scan time
    so trend views can show compliance direction over 30/60/90 day windows.
    """
    table = f"{catalog}.{schema}.scan_summary"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            scan_id STRING NOT NULL,
            scanned_at TIMESTAMP NOT NULL,
            metastore_id STRING,
            total_resources INT,
            total_policies_evaluated INT,
            total_classifications INT,
            open_violations INT,
            resolved_violations INT,
            exception_violations INT,
            new_violations INT,
            newly_resolved INT,
            critical_open INT,
            high_open INT,
            medium_open INT,
            low_open INT,
            compliance_pct DOUBLE
        )
        USING DELTA
        CLUSTER BY (scanned_at)
        TBLPROPERTIES (
            'delta.appendOnly' = 'true'
        )
    """)


def write_scan_summary(spark: SparkSession, catalog: str, schema: str,
                        scan_id: str, scanned_at, metastore_id: str | None,
                        total_resources: int, total_policies_evaluated: int,
                        total_classifications: int,
                        violation_summary: dict) -> None:
    """Write a single summary row for this scan.

    Called after merge_violations so violation_summary has final counts.
    Queries violations table for severity breakdown and compliance_pct.
    """
    ensure_scan_summary_table(spark, catalog, schema)
    violations_table = f"{catalog}.{schema}.violations"

    # Severity breakdown from current violations state
    severity_counts = spark.sql(f"""
        SELECT
            COUNT(CASE WHEN status = 'open' AND severity = 'critical' THEN 1 END) AS critical_open,
            COUNT(CASE WHEN status = 'open' AND severity = 'high' THEN 1 END) AS high_open,
            COUNT(CASE WHEN status = 'open' AND severity = 'medium' THEN 1 END) AS medium_open,
            COUNT(CASE WHEN status = 'open' AND severity = 'low' THEN 1 END) AS low_open
        FROM {violations_table}
    """).first()

    # Compliance %: resources with zero open violations / total resources
    inventory_table = f"{catalog}.{schema}.resource_inventory"
    compliance_row = spark.sql(f"""
        SELECT
            COUNT(DISTINCT ri.resource_id) AS total,
            COUNT(DISTINCT CASE WHEN v.resource_id IS NOT NULL THEN ri.resource_id END) AS with_violations
        FROM {inventory_table} ri
        LEFT JOIN {violations_table} v
            ON ri.resource_id = v.resource_id AND v.status = 'open'
        WHERE ri.scan_id = '{scan_id}'
    """).first()

    total = compliance_row.total or 0
    with_violations = compliance_row.with_violations or 0
    compliance_pct = round((total - with_violations) * 100.0 / total, 1) if total > 0 else 100.0

    # Count newly resolved this scan (resolved_at within last few seconds of scanned_at)
    newly_resolved = violation_summary.get("resolved", 0)

    summary_schema = T.StructType([
        T.StructField("scan_id", T.StringType()),
        T.StructField("scanned_at", T.TimestampType()),
        T.StructField("metastore_id", T.StringType()),
        T.StructField("total_resources", T.IntegerType()),
        T.StructField("total_policies_evaluated", T.IntegerType()),
        T.StructField("total_classifications", T.IntegerType()),
        T.StructField("open_violations", T.IntegerType()),
        T.StructField("resolved_violations", T.IntegerType()),
        T.StructField("exception_violations", T.IntegerType()),
        T.StructField("new_violations", T.IntegerType()),
        T.StructField("newly_resolved", T.IntegerType()),
        T.StructField("critical_open", T.IntegerType()),
        T.StructField("high_open", T.IntegerType()),
        T.StructField("medium_open", T.IntegerType()),
        T.StructField("low_open", T.IntegerType()),
        T.StructField("compliance_pct", T.DoubleType()),
    ])

    row = [(
        scan_id,
        scanned_at,
        metastore_id,
        total_resources,
        total_policies_evaluated,
        total_classifications,
        violation_summary.get("open", 0),
        violation_summary.get("resolved", 0),
        violation_summary.get("exception", 0),
        violation_summary.get("new_this_scan", 0),
        newly_resolved,
        severity_counts.critical_open or 0,
        severity_counts.high_open or 0,
        severity_counts.medium_open or 0,
        severity_counts.low_open or 0,
        compliance_pct,
    )]

    table = f"{catalog}.{schema}.scan_summary"
    df = spark.createDataFrame(row, schema=summary_schema)
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(table)
