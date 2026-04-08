-- Financial Compliance Dashboard Queries
-- Use with Databricks SQL or Lakeview dashboards.
-- Assumes watchdog scan results are stored in a violations table.

-- ============================================================================
-- 1. SOX Control Status — compliance by control owner
-- ============================================================================
SELECT
    t.sox_control_owner                         AS control_owner,
    COUNT(DISTINCT r.resource_id)               AS total_assets,
    SUM(CASE WHEN v.policy_id IS NULL THEN 1 ELSE 0 END) AS compliant,
    SUM(CASE WHEN v.policy_id IS NOT NULL THEN 1 ELSE 0 END) AS violations,
    ROUND(
        100.0 * SUM(CASE WHEN v.policy_id IS NULL THEN 1 ELSE 0 END)
        / NULLIF(COUNT(DISTINCT r.resource_id), 0), 1
    )                                           AS compliance_pct
FROM watchdog.resources r
JOIN watchdog.resource_tags t
    ON r.resource_id = t.resource_id
LEFT JOIN watchdog.violations v
    ON r.resource_id = v.resource_id
    AND v.policy_id LIKE 'POL-SOX-%'
    AND v.scan_date = CURRENT_DATE()
WHERE t.regulatory_domain = 'SOX'
GROUP BY t.sox_control_owner
ORDER BY violations DESC;


-- ============================================================================
-- 2. PCI-DSS Encryption Coverage — encryption status across cardholder data
-- ============================================================================
SELECT
    r.catalog || '.' || r.schema_name || '.' || r.resource_name AS asset_name,
    COALESCE(t.encryption_at_rest, 'missing')   AS encryption_at_rest,
    COALESCE(t.encryption_in_transit, 'missing') AS encryption_in_transit,
    COALESCE(t.column_masking_enabled, 'missing') AS column_masking,
    CASE
        WHEN t.encryption_at_rest = 'true'
         AND t.encryption_in_transit = 'true'
         AND t.column_masking_enabled = 'true'
        THEN 'COMPLIANT'
        ELSE 'NON-COMPLIANT'
    END                                         AS pci_status
FROM watchdog.resources r
JOIN watchdog.resource_tags t
    ON r.resource_id = t.resource_id
WHERE t.data_classification IN ('pci', 'cardholder', 'pan')
   OR t.regulatory_domain = 'PCI-DSS'
ORDER BY pci_status DESC, asset_name;


-- ============================================================================
-- 3. Overall Financial Compliance by Regulation
-- ============================================================================
SELECT
    CASE
        WHEN v.policy_id LIKE 'POL-SOX-%'  THEN 'SOX'
        WHEN v.policy_id LIKE 'POL-PCI-%'  THEN 'PCI-DSS'
        WHEN v.policy_id LIKE 'POL-GLBA-%' THEN 'GLBA'
        ELSE 'Other'
    END                                         AS regulation,
    v.severity,
    COUNT(*)                                    AS violation_count,
    COUNT(DISTINCT v.resource_id)               AS affected_assets
FROM watchdog.violations v
WHERE v.scan_date = CURRENT_DATE()
  AND v.policy_id LIKE 'POL-SOX-%'
   OR v.policy_id LIKE 'POL-PCI-%'
   OR v.policy_id LIKE 'POL-GLBA-%'
GROUP BY 1, 2
ORDER BY
    CASE regulation
        WHEN 'SOX'     THEN 1
        WHEN 'PCI-DSS' THEN 2
        WHEN 'GLBA'    THEN 3
        ELSE 4
    END,
    CASE v.severity
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        WHEN 'low'      THEN 4
    END;


-- ============================================================================
-- 4. Financial Compliance Trend — daily compliance rate over last 30 days
-- ============================================================================
SELECT
    v.scan_date,
    CASE
        WHEN v.policy_id LIKE 'POL-SOX-%'  THEN 'SOX'
        WHEN v.policy_id LIKE 'POL-PCI-%'  THEN 'PCI-DSS'
        WHEN v.policy_id LIKE 'POL-GLBA-%' THEN 'GLBA'
    END                                         AS regulation,
    COUNT(*)                                    AS violations
FROM watchdog.violations v
WHERE v.scan_date >= DATE_SUB(CURRENT_DATE(), 30)
  AND (v.policy_id LIKE 'POL-SOX-%'
    OR v.policy_id LIKE 'POL-PCI-%'
    OR v.policy_id LIKE 'POL-GLBA-%')
GROUP BY v.scan_date, regulation
ORDER BY v.scan_date, regulation;
