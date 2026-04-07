-- Dashboard v3: Data Quality Coverage Overview
-- Shows which tables have DQM, LHM, DQX coverage and which have gaps

SELECT
    ri.resource_id AS table_name,
    ri.owner,
    ri.domain AS catalog,
    ri.metadata['schema'] AS schema_name,
    ri.tags['data_layer'] AS data_layer,
    COALESCE(ri.tags['dqm_enabled'], 'false') AS dqm_enabled,
    COALESCE(ri.tags['lhm_enabled'], 'false') AS lhm_enabled,
    COALESCE(ri.tags['dqx_enabled'], 'false') AS dqx_enabled,
    COALESCE(ri.tags['dqm_anomalies'], '0') AS dqm_anomalies,
    CASE
        WHEN ri.tags['dqm_enabled'] = 'true'
            AND ri.tags['lhm_enabled'] = 'true'
            AND ri.tags['dqx_enabled'] = 'true' THEN 'full'
        WHEN ri.tags['dqm_enabled'] = 'true'
            OR ri.tags['lhm_enabled'] = 'true'
            OR ri.tags['dqx_enabled'] = 'true' THEN 'partial'
        ELSE 'none'
    END AS dq_coverage_level,
    CAST(
        (CASE WHEN ri.tags['dqm_enabled'] = 'true' THEN 1 ELSE 0 END) +
        (CASE WHEN ri.tags['lhm_enabled'] = 'true' THEN 1 ELSE 0 END) +
        (CASE WHEN ri.tags['dqx_enabled'] = 'true' THEN 1 ELSE 0 END)
    AS INT) AS tools_enabled
FROM platform.watchdog.resource_inventory ri
WHERE ri.resource_type = 'table'
    AND ri.scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory)
    AND ri.tags['data_layer'] IN ('gold', 'silver')
ORDER BY
    dq_coverage_level ASC,
    ri.domain, ri.metadata['schema'], ri.resource_name
