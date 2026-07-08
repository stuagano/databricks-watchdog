"""Batch orchestration entrypoint (U9).

Wires U1 -> U3 -> U4 -> U8 -> U5 -> U6 -> U7 into a single, idempotent
`run_all(run_id)` call: one Spark session, one `run_id` threaded through every
stage, every stage overwriting (or, for U8's steward queue, appending to) its
own table so the whole pipeline can be re-run for the same `run_id` without
manual cleanup.

Stage modules are imported lazily, inside the function, rather than at module
top level. Several of them pull in heavyweight/optional dependencies at their
own module scope (`pipeline.quality` imports Watchdog's `mdm_checks`,
`pipeline.match`/`pipeline.steward` reach for the Databricks SDK) that are
only available in a notebook/job context or under pytest's `conftest.py`
`sys.path` shim -- importing them lazily keeps `pipeline.run_all` itself
importable everywhere (including a bare `python -c "import pipeline.run_all"`)
regardless of which of those are installed.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

# `jobs/pipeline_job.yml` runs this file directly as a `spark_python_task`
# (`python .../pipeline/run_all.py ...`), not via a package install -- Python
# only puts this file's own directory (`pipeline/`) on `sys.path` in that
# mode, so `pipeline` itself (and its sibling modules under `pipeline/`)
# wouldn't be importable without this. Mirrors `conftest.py`'s sys.path shim
# so the same reference-pipeline tree works under pytest *and* as a job.
#
# A Databricks `spark_python_task` execs this file's source without setting
# `__file__` in its globals (it compiles+execs the file's bytes directly), so
# `__file__` raises NameError there even though it's set under pytest/CLI.
# `sys.argv[0]` is the job harness's equivalent (the file it was told to run).
try:
    _THIS_FILE = Path(__file__)
except NameError:
    _THIS_FILE = Path(sys.argv[0])
_REPO_ROOT = _THIS_FILE.resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
# This example lives at <repo_root>/examples/mdm-entity-resolution/, and the
# `watchdog` package lives at <repo_root>/engine/src/watchdog/.
_WATCHDOG_SRC = _REPO_ROOT.parent.parent / "engine" / "src"
if _WATCHDOG_SRC.is_dir() and str(_WATCHDOG_SRC) not in sys.path:
    sys.path.insert(0, str(_WATCHDOG_SRC))

from pipeline.config import Cfg  # noqa: E402 (must follow the sys.path bootstrap above)

DEFAULT_MAPPING_SPEC_PATH = _THIS_FILE.resolve().parent.parent / "config" / "mapping_spec.json"


def run_all(run_id: str, cfg: Cfg | None = None, spark: Any = None) -> list[dict[str, Any]]:
    """Run the full bronze->gold batch pipeline for one `run_id`.

    Order: U1 (generate + write synthetic bronze sources) -> U3 (standardize
    bronze->silver) -> U4 (Vector Search batch matching -> `match.pairs`) ->
    U8 (route `needs_review` pairs to the steward queue, right after U4 since
    it only reads `match.pairs`) -> U5 (persistent id crosswalk) -> U6
    (survivorship merge -> `gold.entities`) -> U7 (quality gates against
    `gold.entities`).

    `cfg`/`spark` default to the reference architecture's catalog and the
    ambient Databricks runtime session (notebook/job context) respectively,
    so this can be invoked as `run_all(run_id)` from a Job task; both are
    accepted as parameters so a caller (or a test) can inject a local Cfg /
    SparkSession instead.

    Returns U7's list of quality-check issue dicts (`{"passed": bool, ...}`
    per configured check) so the caller (a Job task, `verify_e2e.py`, or a
    notebook) can assert every check passed.
    """
    from pipeline import crosswalk, gen_sources, mapping, match, quality, standardize, steward, survivorship

    if cfg is None:
        cfg = Cfg()
    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

    # U1: synthetic multi-source generator -> bronze.{erp,plm,procurement,truth}
    data = gen_sources.generate()
    gen_sources.write(spark, cfg, data)

    # U3: standardize bronze -> silver.source_records, via the declarative
    # source -> canonical mapping spec (config-as-data, never hardcoded).
    spec = mapping.load_spec(str(DEFAULT_MAPPING_SPEC_PATH))
    standardize.run(spark, cfg, spec)

    # U4: Vector Search blocking + batch matcher -> match.pairs
    match.run(cfg, run_id, spark=spark)

    # U8: route this run's needs_review pairs to the steward queue.
    steward.publish(spark, cfg, run_id)

    # U5: persistent id crosswalk -> mdm.entity_crosswalk
    crosswalk.assign(spark, cfg, run_id)

    # U6: seed mdm.survivorship_rules from config/survivorship_rules.json (config-as-
    # data; idempotent overwrite so an edit to the JSON rolls out on the next run),
    # then survivorship merge -> gold.entities
    survivorship.seed_rules(spark, cfg)
    survivorship.run(spark, cfg, run_id)

    # U7: quality gates against gold.entities
    return quality.run(spark, cfg, run_id)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint — the target of the Job's `spark_python_task` (see
    `jobs/pipeline_job.yml`). Runs the full pipeline for `--run-id`, prints
    every U7 quality-check result, and exits non-zero if any check failed so
    the Job run itself surfaces as FAILED rather than silently succeeding
    with bad data downstream.
    """
    parser = argparse.ArgumentParser(description="Run the fuzzy-match ER reference pipeline end to end.")
    parser.add_argument("--run-id", required=True, help="Unique id threaded through every stage's output tables.")
    parser.add_argument("--catalog", default=Cfg().catalog, help="Unity Catalog catalog name (default: %(default)s).")
    args = parser.parse_args(argv)

    cfg = Cfg(catalog=args.catalog)
    issues = run_all(args.run_id, cfg=cfg)

    failed = [issue for issue in issues if not issue.get("passed", False)]
    for issue in issues:
        status = "PASS" if issue.get("passed") else "FAIL"
        print(f"[{status}] {issue.get('id')}: {issue.get('name')} — {issue.get('detail', '')}")

    if failed:
        print(f"\n{len(failed)}/{len(issues)} quality checks FAILED for run_id={args.run_id!r}", file=sys.stderr)
        return 1

    print(f"\nAll {len(issues)} quality checks passed for run_id={args.run_id!r}")
    return 0


if __name__ == "__main__":
    # A Databricks spark_python_task execs this file's compiled source inside
    # a notebook-like harness that reports ANY propagated SystemExit as a
    # FAILED run, regardless of its code -- observed live: a fully successful
    # run (all U7 checks passed, main() returned 0) still surfaced as
    # INTERNAL_ERROR/FAILED with error "SystemExit: 0". Only raise on an
    # actual failure so success doesn't get reported as one.
    _exit_code = main()
    if _exit_code != 0:
        raise SystemExit(_exit_code)
