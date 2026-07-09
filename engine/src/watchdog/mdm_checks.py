"""MDM data-quality checks — cross-table aggregate checks that DQM/DQX/Lakehouse
Monitoring don't do well: golden-record uniqueness (dedup), source reconciliation,
and completeness. Config-driven (mdm_checks.yml); the customer points each check at
their golden + source tables.

These are DATA checks (run SQL against the actual rows), not the metadata/tag rules
the policy engine evaluates — so they live here, separate from policy_engine. Pure
SQL builders + result interpreters are split from spark I/O so they're testable
without a workspace.

v1 reports issues (and exits non-zero on failure). Surfacing them in the Watchdog
violations UI is the follow-on — it must write scan_results within a scan_id the
single merge_violations call covers, or merge would resolve every other violation.
"""
from __future__ import annotations


def _csv(cols: list[str]) -> str:
    return ", ".join(cols)


def build_check_sql(check: dict) -> str:
    """SQL for one MDM check. kind ∈ {dedup, reconcile, completeness}."""
    kind = check["kind"]
    table = check["table"]
    if kind == "dedup":
        keys = _csv(check["keys"])
        return (f"SELECT {keys}, COUNT(*) AS n FROM {table} "
                f"GROUP BY {keys} HAVING COUNT(*) > 1")
    if kind == "reconcile":
        m = check["measure"]  # e.g. COUNT(DISTINCT customer_id) or SUM(amount)
        return (f"SELECT (SELECT {m} FROM {table}) AS golden, "
                f"(SELECT {m} FROM {check['source']}) AS source")
    if kind == "completeness":
        # golden rows whose key has no match in source (orphans). Assumes same
        # key column names on both sides.
        on = " AND ".join(f"g.{k} = s.{k}" for k in check["keys"])
        return (f"SELECT COUNT(*) AS orphans FROM {table} g "
                f"WHERE NOT EXISTS (SELECT 1 FROM {check['source']} s WHERE {on})")
    raise ValueError(f"{check.get('id')}: unknown MDM check kind {kind!r}")


def interpret(check: dict, rows: list[dict]) -> dict:
    """Given the query result rows, decide pass/fail + a human detail."""
    kind = check["kind"]
    if kind == "dedup":
        n = len(rows)
        return _issue(check, n == 0,
                      f"{n} duplicate key-group(s) on ({_csv(check['keys'])})")
    if kind == "reconcile":
        row = rows[0] if rows else {}
        g = float(row.get("golden") or 0)
        s = float(row.get("source") or 0)
        pct = abs(g - s) / max(abs(s), 1.0) * 100.0
        tol = float(check.get("tolerance_pct", 0))
        return _issue(check, pct <= tol,
                      f"golden={g:g} source={s:g} diff={pct:.2f}% (tolerance {tol:g}%)")
    if kind == "completeness":
        orphans = int((rows[0] if rows else {}).get("orphans") or 0)
        return _issue(check, orphans == 0,
                      f"{orphans} golden row(s) with no source match")
    raise ValueError(f"unknown MDM check kind {kind!r}")


def _issue(check: dict, passed: bool, detail: str) -> dict:
    return {
        "id": check["id"],
        "name": check.get("name", check["id"]),
        "table": check["table"],
        "kind": check["kind"],
        "severity": check.get("severity", "medium"),
        "passed": passed,
        "detail": detail,
    }


def run(spark, checks: list[dict]) -> list[dict]:
    """Execute each check; return one issue per check. A check that errors (e.g.
    a missing table) becomes a failed issue, not a crash — one bad config
    shouldn't sink the whole run."""
    issues: list[dict] = []
    for check in checks:
        try:
            rows = [r.asDict() for r in spark.sql(build_check_sql(check)).collect()]
            issues.append(interpret(check, rows))
        except Exception as e:
            issues.append(_issue(check, False, f"check errored: {e}"))
    return issues
