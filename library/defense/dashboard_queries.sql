-- Defense Compliance Dashboard Queries
-- Use with Watchdog scan results stored in Unity Catalog.

-- ============================================================================
-- 1. CUI Coverage Summary — assets with/without proper CUI markings
-- ============================================================================
SELECT
    CASE
        WHEN tags['cui_marking'] IS NOT NULL THEN 'Marked'
        ELSE 'Unmarked'
    END AS cui_status,
    CASE
        WHEN tags['regulatory_domain'] = 'ITAR' THEN 'ITAR'
        WHEN tags['regulatory_domain'] = 'NIST-800-171' THEN 'NIST-800-171'
        WHEN tags['data_classification'] IN ('cui', 'controlled_unclassified') THEN 'CUI'
        ELSE 'Other'
    END AS framework,
    COUNT(*) AS asset_count,
    ROUND(
        COUNT(CASE WHEN tags['cui_marking'] IS NOT NULL THEN 1 END) * 100.0 / COUNT(*),
        1
    ) AS pct_compliant
FROM watchdog.scan_results
WHERE tags['regulatory_domain'] IN ('NIST-800-171', 'ITAR')
   OR tags['data_classification'] IN ('cui', 'controlled_unclassified')
GROUP BY 1, 2
ORDER BY framework, cui_status;

-- ============================================================================
-- 2. CMMC Level 2 Readiness — policy pass rates by CMMC control family
-- ============================================================================
SELECT
    policy_id,
    policy_name,
    severity,
    COUNT(*) AS total_evaluated,
    COUNT(CASE WHEN status = 'PASS' THEN 1 END) AS passed,
    COUNT(CASE WHEN status = 'FAIL' THEN 1 END) AS failed,
    ROUND(
        COUNT(CASE WHEN status = 'PASS' THEN 1 END) * 100.0 / COUNT(*),
        1
    ) AS pass_rate_pct
FROM watchdog.policy_results
WHERE policy_id LIKE 'POL-NIST-%' OR policy_id LIKE 'POL-CMMC-%' OR policy_id LIKE 'POL-ITAR-%'
GROUP BY policy_id, policy_name, severity
ORDER BY
    CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
    policy_id;

-- ============================================================================
-- 3. ITAR Export Control Gaps — ITAR assets missing export classification
-- ============================================================================
SELECT
    resource_name,
    resource_type,
    tags['regulatory_domain'] AS regulatory_domain,
    tags['data_classification'] AS data_classification,
    tags['export_control_classification'] AS export_classification,
    tags['encryption_standard'] AS encryption_standard,
    scan_timestamp
FROM watchdog.scan_results
WHERE tags['regulatory_domain'] = 'ITAR'
  AND (tags['export_control_classification'] IS NULL
       OR tags['export_control_classification'] = '')
ORDER BY scan_timestamp DESC;
