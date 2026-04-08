-- Compliance Overview
-- Shows overall compliance posture across all governance domains
SELECT
    domain,
    COUNT(DISTINCT resource_id) as resources_affected,
    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_violations,
    SUM(CASE WHEN status = 'open' AND severity = 'critical' THEN 1 ELSE 0 END) as critical,
    SUM(CASE WHEN status = 'open' AND severity = 'high' THEN 1 ELSE 0 END) as high,
    SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) as resolved,
    SUM(CASE WHEN status = 'exception' THEN 1 ELSE 0 END) as exceptions
FROM ${catalog}.${schema}.violations
GROUP BY domain
ORDER BY critical DESC, high DESC
