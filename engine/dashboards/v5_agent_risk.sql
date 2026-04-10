-- Agent Compliance Dashboard — Risk Heatmap
-- Source view: v_agent_risk_heatmap

-- Risk heatmap: sensitivity × volume
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
    total_errors,
    open_violations,
    critical_violations
FROM v_agent_risk_heatmap
ORDER BY
    CASE risk_tier
        WHEN 'critical' THEN 1 WHEN 'high' THEN 2
        WHEN 'medium' THEN 3 ELSE 4
    END,
    total_requests DESC;

-- Risk tier distribution (donut chart)
SELECT risk_tier, COUNT(*) AS agent_count
FROM v_agent_risk_heatmap
GROUP BY risk_tier;

-- Sensitivity breakdown (stacked bar)
SELECT
    resource_name AS agent_name,
    pii_access,
    external_access,
    data_export,
    sensitivity_score
FROM v_agent_risk_heatmap
WHERE sensitivity_score > 0
ORDER BY sensitivity_score DESC, total_requests DESC
LIMIT 20;

-- Volume vs violations scatter data
SELECT
    resource_name AS agent_name,
    total_requests,
    open_violations,
    risk_tier
FROM v_agent_risk_heatmap
WHERE total_requests > 0;
