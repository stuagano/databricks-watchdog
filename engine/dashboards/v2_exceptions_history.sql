-- Dashboard v2: Exception Audit Trail
-- Full history of all exceptions (active, expired, revoked)

SELECT
    e.metastore_id,
    e.exception_id,
    e.resource_id,
    v.resource_name,
    v.resource_type,
    e.policy_id,
    p.policy_name,
    v.severity,
    v.domain,
    v.owner AS resource_owner,
    e.approved_by,
    e.justification,
    e.approved_at,
    e.expires_at,
    e.active,
    CASE
        WHEN e.active = false THEN 'revoked'
        WHEN e.expires_at IS NOT NULL
            AND e.expires_at <= current_timestamp() THEN 'expired'
        ELSE 'active'
    END AS current_state
FROM platform.watchdog.exceptions e
LEFT JOIN platform.watchdog.violations v
    ON e.resource_id = v.resource_id AND e.policy_id = v.policy_id
LEFT JOIN platform.watchdog.policies p
    ON e.policy_id = p.policy_id
ORDER BY e.approved_at DESC
