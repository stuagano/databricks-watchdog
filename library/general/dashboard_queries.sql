-- General Governance Dashboard Queries
-- Use with Watchdog scan results stored in Unity Catalog.

-- ============================================================================
-- 1. Governance Health Score — overall pass rate by policy domain
-- ============================================================================
SELECT
    domain,
    COUNT(*) AS total_evaluations,
    COUNT(CASE WHEN status = 'PASS' THEN 1 END) AS passed,
    COUNT(CASE WHEN status = 'FAIL' THEN 1 END) AS failed,
    ROUND(
        COUNT(CASE WHEN status = 'PASS' THEN 1 END) * 100.0 / COUNT(*),
        1
    ) AS health_score_pct
FROM watchdog.policy_results
WHERE policy_id LIKE 'POL-GEN-%'
GROUP BY domain
ORDER BY health_score_pct ASC;

-- ============================================================================
-- 2. Tagging Coverage — percentage of data assets with key tags
-- ============================================================================
SELECT
    'data_classification' AS tag_name,
    COUNT(*) AS total_assets,
    COUNT(CASE WHEN tags['data_classification'] IS NOT NULL THEN 1 END) AS tagged,
    ROUND(
        COUNT(CASE WHEN tags['data_classification'] IS NOT NULL THEN 1 END) * 100.0 / COUNT(*),
        1
    ) AS coverage_pct
FROM watchdog.scan_results
WHERE resource_type IN ('table', 'volume')

UNION ALL

SELECT
    'owner' AS tag_name,
    COUNT(*) AS total_assets,
    COUNT(CASE WHEN tags['owner'] IS NOT NULL OR metadata['owner'] IS NOT NULL THEN 1 END) AS tagged,
    ROUND(
        COUNT(CASE WHEN tags['owner'] IS NOT NULL OR metadata['owner'] IS NOT NULL THEN 1 END) * 100.0 / COUNT(*),
        1
    ) AS coverage_pct
FROM watchdog.scan_results

UNION ALL

SELECT
    'environment' AS tag_name,
    COUNT(*) AS total_assets,
    COUNT(CASE WHEN tags['environment'] IS NOT NULL THEN 1 END) AS tagged,
    ROUND(
        COUNT(CASE WHEN tags['environment'] IS NOT NULL THEN 1 END) * 100.0 / COUNT(*),
        1
    ) AS coverage_pct
FROM watchdog.scan_results

UNION ALL

SELECT
    'cost_center' AS tag_name,
    COUNT(*) AS total_assets,
    COUNT(CASE WHEN tags['cost_center'] IS NOT NULL THEN 1 END) AS tagged,
    ROUND(
        COUNT(CASE WHEN tags['cost_center'] IS NOT NULL THEN 1 END) * 100.0 / COUNT(*),
        1
    ) AS coverage_pct
FROM watchdog.scan_results
WHERE resource_type IN ('job', 'cluster', 'warehouse', 'pipeline')

ORDER BY coverage_pct ASC;

-- ============================================================================
-- 3. Stale Asset Report — assets not queried in 90+ days
-- ============================================================================
SELECT
    resource_name,
    resource_type,
    tags['data_classification'] AS classification,
    tags['owner'] AS owner,
    tags['lifecycle_status'] AS lifecycle_status,
    metadata['last_query_date'] AS last_query_date,
    scan_timestamp
FROM watchdog.scan_results
WHERE (tags['stale'] = 'true'
       OR metadata['last_query_date'] IS NULL
       OR metadata['last_query_date'] = '')
  AND resource_type IN ('table', 'volume')
ORDER BY resource_name;

-- ============================================================================
-- 4. Policy Compliance Trend — weekly pass rate over time
-- ============================================================================
SELECT
    DATE_TRUNC('week', scan_timestamp) AS week,
    policy_id,
    policy_name,
    ROUND(
        COUNT(CASE WHEN status = 'PASS' THEN 1 END) * 100.0 / COUNT(*),
        1
    ) AS pass_rate_pct
FROM watchdog.policy_results
WHERE policy_id LIKE 'POL-GEN-%'
  AND scan_timestamp >= DATEADD(MONTH, -3, CURRENT_DATE())
GROUP BY 1, 2, 3
ORDER BY week DESC, policy_id;
