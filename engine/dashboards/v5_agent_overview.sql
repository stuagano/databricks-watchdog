-- Agent Compliance Dashboard — Overview KPIs
-- Source views: v_agent_inventory, v_agent_execution_compliance, v_agent_risk_heatmap

-- KPI: Total agents, governed vs ungoverned
SELECT
    COUNT(*) AS total_agents,
    COUNT(CASE WHEN governance_status = 'governed' THEN 1 END) AS governed,
    COUNT(CASE WHEN governance_status = 'partially_governed' THEN 1 END) AS partially_governed,
    COUNT(CASE WHEN governance_status = 'ungoverned' THEN 1 END) AS ungoverned,
    ROUND(
        100.0 * COUNT(CASE WHEN governance_status = 'governed' THEN 1 END)
        / NULLIF(COUNT(*), 0), 1
    ) AS governance_pct
FROM v_agent_inventory;

-- KPI: Agents by source
SELECT agent_source, COUNT(*) AS agent_count
FROM v_agent_inventory
GROUP BY agent_source
ORDER BY agent_count DESC;

-- KPI: Agent violations by severity
SELECT
    SUM(open_violations) AS total_open,
    SUM(critical_violations) AS critical,
    SUM(high_violations) AS high,
    SUM(excepted_violations) AS excepted
FROM v_agent_inventory;

-- Agents by governance status (pie chart)
SELECT governance_status, COUNT(*) AS agent_count
FROM v_agent_inventory
GROUP BY governance_status;

-- Risk distribution (bar chart)
SELECT risk_tier, COUNT(*) AS agent_count
FROM v_agent_risk_heatmap
GROUP BY risk_tier
ORDER BY
    CASE risk_tier
        WHEN 'critical' THEN 1 WHEN 'high' THEN 2
        WHEN 'medium' THEN 3 ELSE 4
    END;
