-- Agent Execution Compliance
-- Shows per-execution usage metrics, compliance status, and risk flags
SELECT
    endpoint_name,
    requester,
    request_count,
    total_input_tokens,
    total_output_tokens,
    error_count,
    compliance_status,
    CASE WHEN accessed_pii = 'true' THEN 'Yes' ELSE 'No' END AS pii_access,
    open_violations
FROM ${catalog}.${schema}.v_agent_execution_compliance
ORDER BY request_count DESC
