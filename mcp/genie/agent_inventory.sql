-- Agent Inventory
-- Shows all discovered AI agents with governance status, data access flags, and violations
SELECT
    resource_name AS agent_name,
    agent_source,
    COALESCE(agent_owner, owner, 'unassigned') AS owner,
    governance_status,
    environment,
    open_violations,
    critical_violations,
    high_violations,
    CASE WHEN accessed_pii = 'true' THEN 'Yes' ELSE 'No' END AS pii_access,
    CASE WHEN used_external_tool = 'true' THEN 'Yes' ELSE 'No' END AS external_access,
    CASE WHEN exported_data = 'true' THEN 'Yes' ELSE 'No' END AS data_export
FROM ${catalog}.${schema}.v_agent_inventory
ORDER BY open_violations DESC
