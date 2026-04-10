-- Agent Risk Heatmap
-- Shows agent risk scoring based on data sensitivity and request volume
SELECT
    resource_name AS agent_name,
    agent_source,
    COALESCE(agent_owner, 'unassigned') AS owner,
    risk_tier,
    volume_tier,
    sensitivity_score,
    pii_access,
    external_access,
    data_export,
    total_requests,
    total_input_tokens,
    unique_requesters,
    open_violations
FROM ${catalog}.${schema}.v_agent_risk_heatmap
ORDER BY
    CASE risk_tier WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
    total_requests DESC
