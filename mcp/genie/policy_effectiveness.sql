-- Policy Effectiveness
-- Shows how each policy is performing across the estate
SELECT
    p.policy_id,
    p.policy_name,
    p.severity,
    p.domain,
    p.applies_to,
    COUNT(DISTINCT CASE WHEN v.status = 'open' THEN v.violation_id END) as open_violations,
    COUNT(DISTINCT CASE WHEN v.status = 'resolved' THEN v.violation_id END) as resolved,
    COUNT(DISTINCT CASE WHEN v.status = 'exception' THEN v.violation_id END) as exceptions,
    COUNT(DISTINCT v.resource_id) as resources_affected
FROM ${catalog}.${schema}.policies p
LEFT JOIN ${catalog}.${schema}.violations v ON p.policy_id = v.policy_id
WHERE p.active = true
GROUP BY p.policy_id, p.policy_name, p.severity, p.domain, p.applies_to
ORDER BY open_violations DESC
