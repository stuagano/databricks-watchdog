-- Dashboard v2: Compliance Summary (top-level KPIs)
-- Platform-wide compliance posture at a glance

WITH violation_counts AS (
    SELECT
        metastore_id,
        status,
        severity,
        COUNT(*) AS cnt
    FROM platform.watchdog.violations
    GROUP BY metastore_id, status, severity
),
resource_counts AS (
    SELECT
        metastore_id,
        COUNT(DISTINCT resource_id) AS total_resources
    FROM platform.watchdog.resource_inventory ri
    WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory WHERE metastore_id = ri.metastore_id)
    GROUP BY metastore_id
),
violating_resources AS (
    SELECT
        metastore_id,
        COUNT(DISTINCT resource_id) AS resources_with_violations
    FROM platform.watchdog.violations
    WHERE status = 'open'
    GROUP BY metastore_id
),
exception_counts AS (
    SELECT
        COUNT(*) AS total_exceptions,
        SUM(CASE WHEN expires_at IS NOT NULL
            AND DATEDIFF(expires_at, current_timestamp()) <= 14 THEN 1 ELSE 0 END) AS expiring_soon
    FROM platform.watchdog.exceptions
    WHERE active = true
        AND (expires_at IS NULL OR expires_at > current_timestamp())
)
SELECT
    rc.metastore_id,
    rc.total_resources,
    COALESCE(vr.resources_with_violations, 0) AS resources_with_violations,
    ROUND(100.0 * (rc.total_resources - COALESCE(vr.resources_with_violations, 0)) / rc.total_resources, 1) AS compliance_pct,
    COALESCE(SUM(CASE WHEN vc.status = 'open' AND vc.severity = 'critical' THEN vc.cnt END), 0) AS critical_open,
    COALESCE(SUM(CASE WHEN vc.status = 'open' AND vc.severity = 'high' THEN vc.cnt END), 0) AS high_open,
    COALESCE(SUM(CASE WHEN vc.status = 'open' AND vc.severity = 'medium' THEN vc.cnt END), 0) AS medium_open,
    COALESCE(SUM(CASE WHEN vc.status = 'open' AND vc.severity = 'low' THEN vc.cnt END), 0) AS low_open,
    COALESCE(SUM(CASE WHEN vc.status = 'open' THEN vc.cnt END), 0) AS total_open,
    COALESCE(SUM(CASE WHEN vc.status = 'resolved' THEN vc.cnt END), 0) AS total_resolved,
    COALESCE(SUM(CASE WHEN vc.status = 'exception' THEN vc.cnt END), 0) AS total_excepted,
    ec.total_exceptions AS active_exceptions,
    ec.expiring_soon AS exceptions_expiring_soon
FROM resource_counts rc
LEFT JOIN violation_counts vc ON vc.metastore_id = rc.metastore_id
LEFT JOIN violating_resources vr ON vr.metastore_id = rc.metastore_id
LEFT JOIN exception_counts ec ON 1=1
GROUP BY rc.metastore_id, rc.total_resources, vr.resources_with_violations,
         ec.total_exceptions, ec.expiring_soon
