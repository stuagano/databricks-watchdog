"""Unit tests for watchdog.mdm_checks — dedup/reconcile/completeness checks.

build_check_sql/interpret are pure (no Spark), so these test the SQL-building
and pass/fail logic directly against synthetic result rows -- run() itself
(the Spark I/O wrapper) is exercised in examples/mdm-entity-resolution/'s own
integration pass, not here.

Run with: pytest tests/unit/test_mdm_checks.py -v
"""
import pytest
from watchdog.mdm_checks import build_check_sql, interpret

DEDUP_CHECK = {"id": "POL-GOLD-001", "name": "no duplicate golden ids", "table": "gold.entities",
               "kind": "dedup", "keys": ["entity_id"]}
RECONCILE_CHECK = {"id": "POL-GOLD-002", "name": "golden reconciles with source", "table": "gold.entities",
                    "kind": "reconcile", "source": "mdm.entity_crosswalk",
                    "measure": "COUNT(*)", "tolerance_pct": 0.5}
COMPLETENESS_CHECK = {"id": "POL-GOLD-003", "name": "every golden row has a source", "table": "gold.entities",
                       "kind": "completeness", "source": "mdm.entity_crosswalk", "keys": ["entity_id"]}


class TestBuildCheckSql:
    def test_dedup_groups_by_keys(self):
        sql = build_check_sql(DEDUP_CHECK)
        assert "GROUP BY entity_id" in sql
        assert "HAVING COUNT(*) > 1" in sql
        assert "gold.entities" in sql

    def test_reconcile_compares_golden_and_source(self):
        sql = build_check_sql(RECONCILE_CHECK)
        assert "gold.entities" in sql
        assert "mdm.entity_crosswalk" in sql
        assert "COUNT(*)" in sql

    def test_completeness_finds_orphans(self):
        sql = build_check_sql(COMPLETENESS_CHECK)
        assert "NOT EXISTS" in sql
        assert "g.entity_id = s.entity_id" in sql

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError, match="unknown MDM check kind"):
            build_check_sql({"id": "X", "table": "t", "kind": "bogus"})


class TestInterpret:
    def test_dedup_passes_with_no_duplicate_groups(self):
        result = interpret(DEDUP_CHECK, [])
        assert result["passed"] is True
        assert result["id"] == "POL-GOLD-001"

    def test_dedup_fails_with_duplicate_groups(self):
        result = interpret(DEDUP_CHECK, [{"entity_id": "e1", "n": 2}])
        assert result["passed"] is False
        assert "1 duplicate key-group" in result["detail"]

    def test_reconcile_passes_within_tolerance(self):
        result = interpret(RECONCILE_CHECK, [{"golden": 100, "source": 100}])
        assert result["passed"] is True

    def test_reconcile_fails_outside_tolerance(self):
        result = interpret(RECONCILE_CHECK, [{"golden": 90, "source": 100}])
        assert result["passed"] is False
        assert "diff=10.00%" in result["detail"]

    def test_completeness_passes_with_no_orphans(self):
        result = interpret(COMPLETENESS_CHECK, [{"orphans": 0}])
        assert result["passed"] is True

    def test_completeness_fails_with_orphans(self):
        result = interpret(COMPLETENESS_CHECK, [{"orphans": 3}])
        assert result["passed"] is False
        assert "3 golden row(s) with no source match" in result["detail"]

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError, match="unknown MDM check kind"):
            interpret({"id": "X", "kind": "bogus"}, [])
