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

When a session makes code changes that affect documented features, update the corresponding docs before ending. This applies to additions, removals, and renames equally.

### What triggers a doc update

| Change type | Doc to update |
|---|---|
| Add/remove/rename CLI entrypoint | `docs/guide/reference/cli.md` |
| Add/remove/rename ontology class | `docs/guide/reference/ontology-classes.md` |
| Add/remove/rename rule type | `docs/guide/reference/rule-types.md` |
| Add/remove/rename Delta table or column | `docs/guide/reference/tables.md` |
| Add/remove/rename policy field | `docs/guide/reference/policy-schema.md` |
| Add/remove/rename MCP tool (Watchdog) | `docs/guide/reference/mcp-tools.md` |
| Add/remove/rename MCP tool (Guardrails) | `docs/guide/reference/guardrails-tools.md` |
| Feature completed, dropped, or scope change | `docs/roadmap.md` |
| Architectural change | `docs/architecture-guide.md` |
| Add/remove crawler | `docs/guide/how-to/extend-crawlers.md` |
| Drift detection changes | `docs/guide/how-to/drift-detection.md` |

### Removal and rename hygiene

When removing or renaming a feature, function, table, or entrypoint:

1. **Delete the doc section** — don't leave stale references behind. A documented feature that no longer exists is worse than an undocumented feature that does.
2. **Search for cross-references** — grep the docs/ directory for the old name. References in roadmap.md, architecture-guide.md, README.md, and how-to guides often mention specific function names, table names, or counts.
3. **Update counts** — if a doc says "13 tools" or "16 rule types" and you removed one, fix the number.
4. **Check the README** — it has hardcoded counts (class count, rule types, tool count, table count) that go stale when features are added or removed.

### Duplicated facts

Design principles and key numbers get copied across multiple docs. When a fact changes, the copies go stale silently. After changing any of the following, grep for the old claim across `docs/` AND `README.md`:

| Fact | Grep pattern | Known locations |
|---|---|---|
| Read-only vs write behavior | `read-only\|never writes\|never creates` | architecture-guide, concepts/architecture, hub-integration-plan, prerequisites, roadmap |
| Ontology class count | `\d+ classes` | architecture-guide, concepts/architecture, deployment-playbook, README, cli.md (example output) |
| Rule type count | `\d+ rule` | architecture-guide, concepts/architecture, rule-types.md |
| View count | `\d+ (compliance\|views)` | architecture-guide, concepts/architecture, cli.md, index.md, first-dashboard, prerequisites, deployment-playbook, roadmap |
| Table count | `\d+ tables` | concepts/architecture, prerequisites, hub-prd-comparison, README (diagram + deployment table), first-dashboard |
| Genie Space table count | `\d+ tables` | architecture-guide (diagram), concepts/architecture, first-dashboard, roadmap, README (diagram + deployment table) |
| CLI entrypoint count | `entrypoint` | cli.md, concepts/architecture |
| MCP tool count | `\d+ tool` | architecture-guide, README, guardrails-tools, mcp-tools |
| "Cannot fix/remediate" claims | `only reports\|cannot fix\|never remediates` | remediation-agents-prd, architecture-guide |

### General rules

- Update the "Last updated" date in any doc you modify.
- Skip trivial sessions (questions, exploration, no code changes).
- When in doubt, grep `docs/` AND `README.md` for the old value — stale copies are the #1 source of doc drift.

## Conventions

- Policies use YAML under `engine/policies/`, one file per governance domain
- Ontology classes defined in `engine/ontologies/resource_classes.yml`
- Rule primitives in `engine/ontologies/rule_primitives.yml`
- Design specs go in `docs/superpowers/specs/`, implementation plans in `docs/superpowers/plans/`
- Commit messages follow conventional commits: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `perf`
