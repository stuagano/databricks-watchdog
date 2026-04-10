-- Agent Compliance Dashboard — Agent Inventory Detail
-- Source view: v_agent_inventory

-- Full agent inventory table
SELECT
    resource_name AS agent_name,
    agent_source,
    COALESCE(agent_owner, owner, 'unassigned') AS owner,
    governance_status,
    environment,
    open_violations,
    critical_violations,
    CASE WHEN accessed_pii = 'true' THEN 'Yes' ELSE 'No' END AS pii_access,
    CASE WHEN used_external_tool = 'true' THEN 'Yes' ELSE 'No' END AS external_access,
    CASE WHEN exported_data = 'true' THEN 'Yes' ELSE 'No' END AS data_export,
    violated_policies,
    ontology_classes
FROM v_agent_inventory
ORDER BY
    CASE governance_status
        WHEN 'ungoverned' THEN 1
        WHEN 'partially_governed' THEN 2
        ELSE 3
    END,
    open_violations DESC;

-- Top violating agents (bar chart)
SELECT
    resource_name AS agent_name,
    open_violations,
    critical_violations,
    high_violations
FROM v_agent_inventory
WHERE open_violations > 0
ORDER BY open_violations DESC
LIMIT 20;

-- Violations by policy across agents
SELECT
    policy_id,
    COUNT(*) AS agent_count
FROM v_agent_inventory
LATERAL VIEW explode(violated_policies) vp AS policy_id
GROUP BY policy_id
ORDER BY agent_count DESC;
