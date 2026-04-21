# CLAUDE.md — databricks-watchdog

## Project Overview

Watchdog is a compliance posture evaluator for Databricks Unity Catalog and AI agents. See `docs/roadmap.md` for full identity, scope, and what's built.

## Architecture

- **Engine** (`engine/src/watchdog/`): crawlers, ontology, rule engine, policy engine, violations, compiler, deployer, drift detection, remediation pipeline
- **MCP servers**: Watchdog MCP (13 query tools), Guardrails MCP (13 governance tools)
- **Ontos adapter**: GovernanceProvider protocol + remediation review UI
- **Industry packs** (`library/`): healthcare, financial, defense, general

Key files:
- `engine/src/watchdog/entrypoints.py` — all 10 CLI entrypoints
- `engine/src/watchdog/rule_engine.py` — 16 rule types
- `engine/ontologies/resource_classes.yml` — 8 base classes, 20+ derived classes
- `engine/src/watchdog/compiler.py` — compile-down pipeline
- `engine/src/watchdog/remediation/` — remediation pipeline package

## Testing

```bash
# Unit tests
cd engine && python -m pytest tests/ -v

# Specific test file
python -m pytest tests/unit/test_drift.py -v
```

## End-of-Session Documentation Updates

When a session makes code changes that affect documented features, update the corresponding docs before ending. The mapping is:

| Change type | Doc to update |
|---|---|
| New/changed CLI entrypoint | `docs/guide/reference/cli.md` |
| New ontology class | `docs/guide/reference/ontology-classes.md` |
| New rule type | `docs/guide/reference/rule-types.md` |
| New Delta table or column | `docs/guide/reference/tables.md` |
| New policy field | `docs/guide/reference/policy-schema.md` |
| New MCP tool (Watchdog) | `docs/guide/reference/mcp-tools.md` |
| New MCP tool (Guardrails) | `docs/guide/reference/guardrails-tools.md` |
| Feature completed or scope change | `docs/roadmap.md` |
| Architectural change | `docs/architecture-guide.md` |
| New crawler | `docs/guide/how-to/extend-crawlers.md` |
| Drift detection changes | `docs/guide/how-to/drift-detection.md` |

Update the "Last updated" date in any doc you modify. Skip trivial sessions (questions, exploration, no code changes).

## Conventions

- Policies use YAML under `engine/policies/`, one file per governance domain
- Ontology classes defined in `engine/ontologies/resource_classes.yml`
- Rule primitives in `engine/ontologies/rule_primitives.yml`
- Design specs go in `docs/superpowers/specs/`, implementation plans in `docs/superpowers/plans/`
- Commit messages follow conventional commits: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `perf`
