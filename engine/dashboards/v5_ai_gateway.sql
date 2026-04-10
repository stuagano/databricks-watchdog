-- AI Gateway Cost Governance
-- Token consumption, estimated cost, model routing, and rate limiting insights

-- KPI: Total cost summary
SELECT
    SUM(estimated_dbus) AS total_estimated_dbus,
    SUM(total_tokens) AS total_tokens,
    SUM(request_count) AS total_requests,
    COUNT(DISTINCT endpoint_name) AS active_endpoints,
    COUNT(DISTINCT requester) AS unique_requesters,
    SUM(rate_limited_count) AS total_rate_limited
FROM v_ai_gateway_cost_governance;

-- Cost by model/endpoint (bar chart)
SELECT
    endpoint_name,
    entity_type,
    task_type,
    SUM(estimated_dbus) AS estimated_dbus,
    SUM(total_tokens) AS total_tokens,
    SUM(request_count) AS requests
FROM v_ai_gateway_cost_governance
GROUP BY endpoint_name, entity_type, task_type
ORDER BY estimated_dbus DESC
LIMIT 15;

-- Cost by requester (top consumers)
SELECT
    requester,
    COUNT(DISTINCT endpoint_name) AS endpoints_used,
    SUM(request_count) AS total_requests,
    SUM(total_tokens) AS total_tokens,
    SUM(estimated_dbus) AS estimated_dbus,
    MAX(governance_status) AS governance_status
FROM v_ai_gateway_cost_governance
GROUP BY requester
ORDER BY estimated_dbus DESC
LIMIT 20;

-- Cost by entity type (pie chart)
SELECT
    entity_type,
    SUM(estimated_dbus) AS estimated_dbus,
    SUM(request_count) AS requests
FROM v_ai_gateway_cost_governance
GROUP BY entity_type;

-- Ungoverned high-cost consumers (risk table)
SELECT
    endpoint_name,
    requester,
    governance_status,
    request_count,
    total_tokens,
    estimated_dbus,
    cost_risk_flag
FROM v_ai_gateway_cost_governance
WHERE cost_risk_flag != 'normal'
ORDER BY estimated_dbus DESC;

-- Rate-limited requesters
SELECT
    endpoint_name,
    requester,
    request_count,
    rate_limited_count,
    ROUND(100.0 * rate_limited_count / NULLIF(request_count, 0), 1) AS rate_limit_pct,
    estimated_dbus
FROM v_ai_gateway_cost_governance
WHERE rate_limited_count > 0
ORDER BY rate_limited_count DESC;

-- Model routing: which task types are most used
SELECT
    task_type,
    entity_type,
    COUNT(DISTINCT endpoint_name) AS endpoints,
    SUM(request_count) AS total_requests,
    SUM(total_tokens) AS total_tokens,
    SUM(estimated_dbus) AS estimated_dbus
FROM v_ai_gateway_cost_governance
GROUP BY task_type, entity_type
ORDER BY total_requests DESC;
