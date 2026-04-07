-- Dashboard v2: Active Exceptions
-- Shows all non-expired, active exceptions with resource context

SELECT
    e.exception_id,
    e.resource_id,
    v.resource_name,
    v.resource_type,
    e.policy_id,
    p.policy_name,
    v.severity,
    v.domain,
    e.approved_by,
    e.justification,
    e.approved_at,
    e.expires_at,
    CASE
        WHEN e.expires_at IS NULL THEN 'permanent'
        ELSE CAST(DATEDIFF(e.expires_at, current_timestamp()) AS STRING) || ' days'
    END AS time_remaining,
    CASE
        WHEN e.expires_at IS NOT NULL
            AND DATEDIFF(e.expires_at, current_timestamp()) <= 14
        THEN 'expiring_soon'
        WHEN e.expires_at IS NULL THEN 'permanent'
        ELSE 'active'
    END AS urgency
FROM platform.watchdog.exceptions e
LEFT JOIN platform.watchdog.violations v
    ON e.resource_id = v.resource_id AND e.policy_id = v.policy_id
LEFT JOIN platform.watchdog.policies p
    ON e.policy_id = p.policy_id AND p.active = true
WHERE e.active = true
    AND (e.expires_at IS NULL OR e.expires_at > current_timestamp())
ORDER BY
    CASE
        WHEN e.expires_at IS NOT NULL
            AND DATEDIFF(e.expires_at, current_timestamp()) <= 14
        THEN 0
        ELSE 1
    END,
    e.approved_at DESC
