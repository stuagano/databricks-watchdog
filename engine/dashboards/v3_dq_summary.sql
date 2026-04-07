-- Dashboard v3: DQ Summary KPIs
-- Top-level data quality metrics for platform overview

WITH latest_scan AS (
    SELECT MAX(scan_id) AS scan_id FROM platform.watchdog.resource_inventory
),
gold_tables AS (
    SELECT
        ri.resource_id,
        ri.tags['dqm_enabled'] AS dqm_enabled,
        ri.tags['lhm_enabled'] AS lhm_enabled,
        ri.tags['dqx_enabled'] AS dqx_enabled,
        ri.tags['dqm_anomalies'] AS dqm_anomalies
    FROM platform.watchdog.resource_inventory ri
    CROSS JOIN latest_scan ls
    WHERE ri.scan_id = ls.scan_id
        AND ri.resource_type = 'table'
        AND ri.tags['data_layer'] = 'gold'
),
anomaly_counts AS (
    SELECT COUNT(*) AS recent_anomalies
    FROM platform.watchdog.dq_status
    WHERE anomaly = true
        AND checked_at >= current_timestamp() - INTERVAL 7 DAY
)
SELECT
    COUNT(*) AS total_gold_tables,
    SUM(CASE WHEN dqm_enabled = 'true' THEN 1 ELSE 0 END) AS dqm_covered,
    SUM(CASE WHEN lhm_enabled = 'true' THEN 1 ELSE 0 END) AS lhm_covered,
    SUM(CASE WHEN dqx_enabled = 'true' THEN 1 ELSE 0 END) AS dqx_covered,
    SUM(CASE WHEN dqm_enabled = 'true' OR lhm_enabled = 'true' OR dqx_enabled = 'true'
        THEN 1 ELSE 0 END) AS any_dq_coverage,
    ROUND(100.0 * SUM(CASE WHEN dqm_enabled = 'true' OR lhm_enabled = 'true' OR dqx_enabled = 'true'
        THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS dq_coverage_pct,
    SUM(CASE WHEN CAST(COALESCE(dqm_anomalies, '0') AS INT) > 0 THEN 1 ELSE 0 END) AS tables_with_anomalies,
    ac.recent_anomalies
FROM gold_tables
CROSS JOIN anomaly_counts ac
