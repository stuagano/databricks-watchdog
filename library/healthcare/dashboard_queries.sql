-- HIPAA Compliance Dashboard Queries
-- Use in Lakeview or Genie Space for healthcare-specific governance views
--
-- Parameterized with ${catalog} and ${schema} for portability.
-- Replace with your actual catalog and schema names before use.

-- Query 1: PHI Asset Inventory
-- All assets classified as PHI with stewardship and retention status
SELECT
    ri.resource_id,
    ri.resource_name,
    ri.owner,
    rc.class_name,
    ri.tags['data_steward'] as phi_steward,
    ri.tags['retention_policy'] as retention_policy,
    ri.tags['encryption_at_rest'] as encrypted,
    ri.tags['access_logging_enabled'] as access_logged,
    COUNT(DISTINCT CASE WHEN v.status = 'open' THEN v.violation_id END) as open_violations
FROM ${catalog}.${schema}.resource_inventory ri
JOIN ${catalog}.${schema}.resource_classifications rc
    ON ri.resource_id = rc.resource_id AND ri.scan_id = rc.scan_id
LEFT JOIN ${catalog}.${schema}.violations v
    ON ri.resource_id = v.resource_id AND v.status = 'open'
WHERE ri.scan_id = (SELECT MAX(scan_id) FROM ${catalog}.${schema}.resource_inventory)
    AND rc.class_name IN ('PhiAsset', 'EphiAsset')
GROUP BY ri.resource_id, ri.resource_name, ri.owner, rc.class_name,
         ri.tags['data_steward'], ri.tags['retention_policy'],
         ri.tags['encryption_at_rest'], ri.tags['access_logging_enabled'];

-- Query 2: HIPAA Compliance Summary
-- Overall HIPAA compliance posture
SELECT
    p.policy_id,
    p.policy_name,
    p.severity,
    COUNT(DISTINCT CASE WHEN v.status = 'open' THEN v.resource_id END) as resources_in_violation,
    COUNT(DISTINCT CASE WHEN v.status = 'resolved' THEN v.resource_id END) as resources_resolved,
    COUNT(DISTINCT CASE WHEN v.status = 'exception' THEN v.resource_id END) as resources_excepted
FROM ${catalog}.${schema}.policies p
LEFT JOIN ${catalog}.${schema}.violations v ON p.policy_id = v.policy_id
WHERE p.policy_id LIKE 'POL-HIPAA-%'
    AND p.active = true
GROUP BY p.policy_id, p.policy_name, p.severity
ORDER BY p.severity, p.policy_id;

-- Query 3: PHI in Non-Production Environments
-- Critical: identifies PHI data that may exist in dev/test
SELECT
    ri.resource_id,
    ri.resource_name,
    ri.owner,
    ri.tags['environment'] as environment,
    ri.tags['data_classification'] as classification
FROM ${catalog}.${schema}.resource_inventory ri
JOIN ${catalog}.${schema}.resource_classifications rc
    ON ri.resource_id = rc.resource_id AND ri.scan_id = rc.scan_id
WHERE ri.scan_id = (SELECT MAX(scan_id) FROM ${catalog}.${schema}.resource_inventory)
    AND rc.class_name IN ('PhiAsset', 'EphiAsset')
    AND ri.tags['environment'] IN ('dev', 'development', 'sandbox', 'test');
