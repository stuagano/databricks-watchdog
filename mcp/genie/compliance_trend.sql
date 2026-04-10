-- Compliance Trend
-- Shows compliance posture over time with scan-over-scan deltas
SELECT
    scanned_at,
    total_resources,
    open_violations,
    compliance_pct,
    open_violations_delta,
    compliance_pct_delta,
    trend_direction,
    critical_open,
    high_open,
    compliance_pct_7scan_avg
FROM ${catalog}.${schema}.v_compliance_trend
ORDER BY scanned_at DESC
