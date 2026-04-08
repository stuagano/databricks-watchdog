-- Dashboard v2: Violations by Owner (role-scoped view)
-- Use with AI/BI dashboard filter: WHERE owner = :current_user
-- or WHERE domain = :selected_domain for domain owner view

SELECT
    metastore_id,
    owner,
    domain,
    severity,
    COUNT(*) AS violation_count,
    COUNT(DISTINCT resource_id) AS resources_affected,
    COUNT(DISTINCT policy_id) AS policies_violated,
    MIN(first_detected) AS earliest_violation,
    MAX(last_detected) AS latest_violation
FROM platform.watchdog.violations
WHERE status = 'open'
GROUP BY metastore_id, owner, domain, severity
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
    END,
    violation_count DESC
