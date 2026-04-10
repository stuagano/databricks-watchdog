-- Agent Compliance Dashboard — Execution Compliance
-- Source view: v_agent_execution_compliance

-- Execution compliance summary KPIs
SELECT
    COUNT(*) AS total_executions,
    COUNT(CASE WHEN compliance_status = 'compliant' THEN 1 END) AS compliant,
    COUNT(CASE WHEN compliance_status IN ('violation', 'high', 'critical') THEN 1 END) AS non_compliant,
    ROUND(
        100.0 * COUNT(CASE WHEN compliance_status = 'compliant' THEN 1 END)
        / NULLIF(COUNT(*), 0), 1
    ) AS compliance_pct
FROM v_agent_execution_compliance;

-- Top consumers by request volume
SELECT
    endpoint_name,
    requester,
    request_count,
    total_input_tokens,
    total_output_tokens,
    compliance_status,
    accessed_pii,
    open_violations
FROM v_agent_execution_compliance
ORDER BY request_count DESC
LIMIT 20;

-- PII access patterns: which endpoints access PII most
SELECT
    endpoint_name,
    COUNT(*) AS execution_count,
    SUM(request_count) AS total_requests,
    SUM(total_input_tokens) AS total_tokens
FROM v_agent_execution_compliance
WHERE accessed_pii = 'true'
GROUP BY endpoint_name
ORDER BY total_requests DESC;

-- Execution compliance by endpoint (bar chart)
SELECT
    endpoint_name,
    COUNT(*) AS total_executions,
    COUNT(CASE WHEN compliance_status = 'compliant' THEN 1 END) AS compliant,
    COUNT(CASE WHEN compliance_status != 'compliant' THEN 1 END) AS non_compliant
FROM v_agent_execution_compliance
GROUP BY endpoint_name
ORDER BY non_compliant DESC
LIMIT 15;

-- Error rate by endpoint
SELECT
    endpoint_name,
    SUM(request_count) AS total_requests,
    SUM(error_count) AS total_errors,
    ROUND(
        100.0 * SUM(error_count) / NULLIF(SUM(request_count), 0), 2
    ) AS error_rate_pct
FROM v_agent_execution_compliance
WHERE error_count > 0
GROUP BY endpoint_name
ORDER BY error_rate_pct DESC;
