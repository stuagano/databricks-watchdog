-- Dashboard v2: Violation Detail (drilldown from owner or domain view)
-- Use with AI/BI dashboard filter: WHERE owner = :selected_owner

SELECT
    v.resource_id,
    v.resource_name,
    v.resource_type,
    v.policy_id,
    p.policy_name,
    v.severity,
    v.domain,
    v.detail,
    v.remediation,
    v.owner,
    v.resource_classes,
    v.first_detected,
    v.last_detected,
    DATEDIFF(current_timestamp(), v.first_detected) AS days_open,
    v.status,
    CASE
        WHEN e.exception_id IS NOT NULL THEN 'has_exception'
        ELSE 'no_exception'
    END AS exception_status
FROM platform.watchdog.violations v
LEFT JOIN platform.watchdog.policies p
    ON v.policy_id = p.policy_id AND p.active = true
LEFT JOIN platform.watchdog.exceptions e
    ON v.resource_id = e.resource_id
    AND v.policy_id = e.policy_id
    AND e.active = true
    AND (e.expires_at IS NULL OR e.expires_at > current_timestamp())
WHERE v.status IN ('open', 'exception')
ORDER BY
    CASE v.severity
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
    END,
    v.last_detected DESC
