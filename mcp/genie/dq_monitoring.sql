-- Data Quality Monitoring Coverage
-- Shows which tables have DQM, LHM, both, or neither
SELECT
    ri.domain as catalog_name,
    ri.resource_name,
    ri.owner,
    COALESCE(ri.tags['dqm_enabled'], 'false') as dqm_enabled,
    COALESCE(ri.tags['lhm_enabled'], 'false') as lhm_enabled,
    CASE
        WHEN ri.tags['dqm_enabled'] = 'true' AND ri.tags['lhm_enabled'] = 'true' THEN 'both'
        WHEN ri.tags['dqm_enabled'] = 'true' THEN 'dqm_only'
        WHEN ri.tags['lhm_enabled'] = 'true' THEN 'lhm_only'
        ELSE 'none'
    END as monitoring_status,
    ri.tags['dqm_anomalies'] as anomalies
FROM ${catalog}.${schema}.resource_inventory ri
WHERE ri.scan_id = (SELECT MAX(scan_id) FROM ${catalog}.${schema}.resource_inventory)
  AND ri.resource_type = 'table'
ORDER BY monitoring_status, ri.domain
