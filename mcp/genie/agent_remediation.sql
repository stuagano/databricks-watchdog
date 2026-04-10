-- Agent Remediation Priorities
-- Shows prioritized remediation actions ranked by impact and effort
SELECT
    policy_name,
    severity,
    customer_agent_violations AS agents_affected,
    effort_estimate,
    remediation_steps AS what_to_do,
    affected_customer_agents AS which_agents,
    oldest_days_open AS days_open,
    priority_score
FROM ${catalog}.${schema}.v_agent_remediation_priorities
WHERE customer_agent_violations > 0
ORDER BY priority_score DESC
