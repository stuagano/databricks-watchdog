-- Dashboard v2: Compliance Summary (top-level KPIs)
-- Platform-wide compliance posture at a glance

WITH violation_counts AS (
    SELECT
        status,
        severity,
        COUNT(*) AS cnt
    FROM platform.watchdog.violations
    GROUP BY status, severity
),
resource_counts AS (
    SELECT COUNT(DISTINCT resource_id) AS total_resources
    FROM platform.watchdog.resource_inventory
    WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory)
),
violating_resources AS (
    SELECT COUNT(DISTINCT resource_id) AS resources_with_violations
    FROM platform.watchdog.violations
    WHERE status = 'open'
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
    rc.total_resources,
    vr.resources_with_violations,
    ROUND(100.0 * (rc.total_resources - vr.resources_with_violations) / rc.total_resources, 1) AS compliance_pct,
    COALESCE(SUM(CASE WHEN vc.status = 'open' AND vc.severity = 'critical' THEN vc.cnt END), 0) AS critical_open,
    COALESCE(SUM(CASE WHEN vc.status = 'open' AND vc.severity = 'high' THEN vc.cnt END), 0) AS high_open,
    COALESCE(SUM(CASE WHEN vc.status = 'open' AND vc.severity = 'medium' THEN vc.cnt END), 0) AS medium_open,
    COALESCE(SUM(CASE WHEN vc.status = 'open' AND vc.severity = 'low' THEN vc.cnt END), 0) AS low_open,
    COALESCE(SUM(CASE WHEN vc.status = 'open' THEN vc.cnt END), 0) AS total_open,
    COALESCE(SUM(CASE WHEN vc.status = 'resolved' THEN vc.cnt END), 0) AS total_resolved,
    COALESCE(SUM(CASE WHEN vc.status = 'exception' THEN vc.cnt END), 0) AS total_excepted,
    ec.total_exceptions AS active_exceptions,
    ec.expiring_soon AS exceptions_expiring_soon
FROM violation_counts vc
CROSS JOIN resource_counts rc
CROSS JOIN violating_resources vr
CROSS JOIN exception_counts ec
GROUP BY rc.total_resources, vr.resources_with_violations,
         ec.total_exceptions, ec.expiring_soon
