-- Dashboard v3: DQ Anomalies (recent)
-- Shows DQM and LHM anomalies detected in the last 7 days

SELECT
    ds.table_id,
    ds.source,
    ds.metric,
    ds.status,
    ds.value,
    ds.anomaly,
    ds.checked_at,
    ri.owner,
    ri.tags['data_layer'] AS data_layer,
    ri.domain AS catalog
FROM platform.watchdog.dq_status ds
LEFT JOIN platform.watchdog.resource_inventory ri
    ON ds.table_id = ri.resource_id
    AND ri.resource_type = 'table'
    AND ri.scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory)
WHERE ds.anomaly = true
    AND ds.checked_at >= current_timestamp() - INTERVAL 7 DAY
ORDER BY ds.checked_at DESC
