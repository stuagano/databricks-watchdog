"""Quality gates (U7).

Thin wrapper around Watchdog's pure MDM check builders/interpreters
(``watchdog.mdm_checks.build_check_sql`` / ``interpret``), config-driven via
``config/mdm_checks.yml``: dedup, reconciliation, and completeness checks
against ``gold.entities`` (U6's survivorship output).

This module owns exactly one thing Watchdog's pure functions can't: resolving
the config's logical schema keys (``gold`` / ``mdm`` / ...) to the concrete,
catalog-qualified table names Spark actually queries, via :class:`pipeline.
config.Cfg`. All check *logic* (what SQL to run, how to read the result)
stays in Watchdog so the same rules can be surfaced in its violations UI
later -- this module never reimplements that logic, only wires it to a
config file and a Spark session.

:func:`load_checks` and :func:`resolve_check` are pure and unit-tested
without Spark. :func:`run` is the Spark I/O wrapper (loads the YAML, resolves
each check, executes the Watchdog-built SQL, interprets the rows) and is
exercised on the Databricks workspace, not by the local pytest suite.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from watchdog.mdm_checks import build_check_sql, interpret

from pipeline.config import Cfg

DEFAULT_CHECKS_PATH = Path(__file__).parent.parent / "config" / "mdm_checks.yml"


def load_checks(path: Path = DEFAULT_CHECKS_PATH) -> list[dict[str, Any]]:
    """Load the raw check definitions from ``mdm_checks.yml``."""
    with open(path) as f:
        doc = yaml.safe_load(f)
    return doc["checks"]


def resolve_check(check: dict[str, Any], cfg: Cfg) -> dict[str, Any]:
    """Resolve one YAML check's logical ``schema``/``table`` (and, for
    reconcile/completeness, ``source_schema``/``source_table``/
    ``source_filter``) into the concrete shape Watchdog's ``build_check_sql``
    / ``interpret`` expect: a flat dict with ``table`` (and ``source``, when
    applicable) as ready-to-splice SQL table references.

    The source side is always resolved to a parenthesized subquery (``(SELECT
    * FROM <fqn> [WHERE <source_filter>])``) rather than a bare table name, so
    an optional ``source_filter`` composes correctly whether Watchdog uses it
    as a scalar-subquery FROM (``reconcile``) or an aliased derived table
    (``completeness``) -- a bare ``"<fqn> WHERE ..."`` string only works in
    the former position.
    """
    resolved: dict[str, Any] = {
        "id": check["id"],
        "name": check.get("name", check["id"]),
        "kind": check["kind"],
        "severity": check.get("severity", "medium"),
        "table": cfg.tbl(check["schema"], check["table"]),
    }
    if "keys" in check:
        resolved["keys"] = check["keys"]
    if "measure" in check:
        resolved["measure"] = check["measure"]
    if "tolerance_pct" in check:
        resolved["tolerance_pct"] = check["tolerance_pct"]
    if "source_schema" in check:
        source_fqn = cfg.tbl(check["source_schema"], check["source_table"])
        source_filter = check.get("source_filter")
        where = f" WHERE {source_filter}" if source_filter else ""
        resolved["source"] = f"(SELECT * FROM {source_fqn}{where})"
    return resolved


def run(spark: Any, cfg: Cfg, run_id: str) -> list[dict[str, Any]]:
    """Run every configured MDM check against ``gold.entities`` (and its
    reconcile/completeness sources) and return one issue dict per check.

    A check that errors (e.g. a table not yet created) becomes a failed issue
    rather than crashing the run -- one bad/premature check shouldn't sink
    the whole quality gate, matching Watchdog's own ``run()`` semantics.
    """
    issues: list[dict[str, Any]] = []
    for raw_check in load_checks():
        resolved = resolve_check(raw_check, cfg)
        try:
            sql = build_check_sql(resolved)
            rows = [row.asDict() for row in spark.sql(sql).collect()]
            issue = interpret(resolved, rows)
        except Exception as e:
            issue = {
                "id": resolved["id"],
                "name": resolved["name"],
                "table": resolved["table"],
                "kind": resolved["kind"],
                "severity": resolved["severity"],
                "passed": False,
                "detail": f"check errored: {e}",
            }
        issue["run_id"] = run_id
        issues.append(issue)
    return issues
