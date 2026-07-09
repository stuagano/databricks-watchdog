"""ctk demonstration + caps proof: Watchdog's mdm_checks actually catch real
defects, for all three check kinds this pipeline's U7 quality gate uses --
not just report a trivial pass regardless of input.

Backs the `mdm-checks-catch-real-defects` capability (see capabilities.yaml):
a quality gate that always reports "passed" is worse than no quality gate at
all, because it looks like coverage. Each check below is exercised with both
a genuinely broken result set (must fail) and a genuinely clean one (must
pass), using ctk's expect() for the declarative assertions.
"""

from ctk import expect
from watchdog.mdm_checks import interpret

DEDUP_CHECK = {"id": "POL-GOLD-001", "table": "gold.entities", "kind": "dedup", "keys": ["entity_id"]}
RECONCILE_CHECK = {
    "id": "POL-GOLD-002", "table": "gold.entities", "kind": "reconcile",
    "source": "mdm.entity_crosswalk", "measure": "COUNT(*)", "tolerance_pct": 0.5,
}
COMPLETENESS_CHECK = {
    "id": "POL-GOLD-003", "table": "gold.entities", "kind": "completeness",
    "source": "mdm.entity_crosswalk", "keys": ["entity_id"],
}


def test_dedup_check_catches_a_real_duplicate_entity_id():
    broken = interpret(DEDUP_CHECK, [{"entity_id": "e1", "n": 2}])
    clean = interpret(DEDUP_CHECK, [])

    expect(broken["passed"], label="dedup on a real duplicate").equals(False).verify()
    expect(broken["detail"], label="dedup failure detail").matches(r"1 duplicate").verify()
    expect(clean["passed"], label="dedup on no duplicates").equals(True).verify()


def test_reconcile_check_catches_a_real_count_mismatch():
    broken = interpret(RECONCILE_CHECK, [{"golden": 90, "source": 100}])
    clean = interpret(RECONCILE_CHECK, [{"golden": 100, "source": 100}])

    expect(broken["passed"], label="reconcile beyond tolerance").equals(False).verify()
    expect(broken["detail"], label="reconcile failure detail").matches(r"diff=10\.00%").verify()
    expect(clean["passed"], label="reconcile within tolerance").equals(True).verify()


def test_completeness_check_catches_a_real_orphan_row():
    broken = interpret(COMPLETENESS_CHECK, [{"orphans": 3}])
    clean = interpret(COMPLETENESS_CHECK, [{"orphans": 0}])

    expect(broken["passed"], label="completeness with orphans").equals(False).verify()
    expect(broken["detail"], label="completeness failure detail").matches(r"3 golden row").verify()
    expect(clean["passed"], label="completeness with no orphans").equals(True).verify()
