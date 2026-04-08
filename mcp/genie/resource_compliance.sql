-- Resource Compliance
-- Per-resource compliance status with ontology class and violation details
SELECT
    ri.resource_id,
    ri.resource_type,
    ri.resource_name,
    ri.owner,
    rc.class_name as ontology_class,
    COUNT(DISTINCT CASE WHEN v.status = 'open' THEN v.violation_id END) as open_violations,
    COUNT(DISTINCT CASE WHEN v.status = 'open' AND v.severity = 'critical' THEN v.violation_id END) as critical_open,
    COUNT(DISTINCT CASE WHEN v.status = 'resolved' THEN v.violation_id END) as resolved,
    COUNT(DISTINCT CASE WHEN v.status = 'exception' THEN v.violation_id END) as exceptions
FROM ${catalog}.${schema}.resource_inventory ri
LEFT JOIN ${catalog}.${schema}.resource_classifications rc
    ON ri.resource_id = rc.resource_id AND ri.scan_id = rc.scan_id
LEFT JOIN ${catalog}.${schema}.violations v
    ON ri.resource_id = v.resource_id
WHERE ri.scan_id = (SELECT MAX(scan_id) FROM ${catalog}.${schema}.resource_inventory)
GROUP BY ri.resource_id, ri.resource_type, ri.resource_name, ri.owner, rc.class_name
