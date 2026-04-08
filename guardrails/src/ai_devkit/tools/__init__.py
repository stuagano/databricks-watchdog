"""Governance tools for the AI Dev Kit — extends the official MCP server.

These tools add a governance layer that the official ai-dev-kit doesn't have:
- validate_ai_query: enforce classification × operation policies
- suggest_safe_tables: discover data within governance limits
- safe_columns: column-level safety for partially restricted tables
- preview_data: sample rows before building pipelines
- estimate_cost: DBU cost estimation before expensive operations

Integration: register these as additional tools in the ai-dev-kit MCP server.
"""
