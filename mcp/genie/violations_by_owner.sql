-- Violations by Owner
-- Shows open violations grouped by resource owner for accountability
SELECT
    owner,
    COUNT(*) as total_violations,
    SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical,
    SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as high,
    SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END) as medium,
    SUM(CASE WHEN severity = 'low' THEN 1 ELSE 0 END) as low,
    MIN(first_detected) as oldest_violation,
    MAX(last_detected) as latest_detection
FROM ${catalog}.${schema}.violations
WHERE status = 'open'
GROUP BY owner
ORDER BY critical DESC, high DESC, total_violations DESC
