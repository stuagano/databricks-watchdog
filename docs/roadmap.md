# Watchdog Roadmap

## Sprint 1 — Core Engine
- [x] Resource crawler (12 types)
- [x] Ontology engine (tag-based classification)
- [x] Policy engine (declarative rules)
- [x] Violation tracking (MERGE + lifecycle)
- [x] Notification service (Delta queue + ACS)
- [x] AI/BI dashboards (8 SQL queries)

## Sprint 2 — MCP Server + Terraform
- [x] MCP server with SSE transport
- [x] On-behalf-of auth (user identity passthrough)
- [x] 6 governance tools (get_violations, get_governance_summary, get_policies, get_scan_history, get_resource_violations, get_exceptions)
- [x] Terraform module (SP, catalog, schema, grants)

## Sprint 3 — Ontos Adapter + Guardrails
- [x] Ontos adapter (GovernanceProvider protocol + FastAPI routers)
- [x] React views for Ontos fork
- [x] AI DevKit guardrails MCP server (9 tools)
- [x] Watchdog client integration for guardrails

## Sprint 4 — AI-Assisted Governance
- [x] `explain_violation` — plain-language violation explainer with remediation steps (P0)
- [x] `what_if_policy` — simulate proposed policies against current inventory (P1)
- [ ] Policy recommendation engine (suggest policies based on resource patterns)
- [ ] Violation trend forecasting

## Sprint 5 — Industry Packs (Future)
- [ ] Healthcare policy pack (HIPAA)
- [ ] Financial policy pack (SOX, PCI-DSS)
- [ ] Defense policy pack (CMMC, ITAR)
