"""End-to-end acceptance check for the U9 batch orchestration job.

Integration script: submits `pipeline/run_all.py` as a one-time Databricks
Jobs run (`databricks jobs submit` semantics, via the SDK's `WorkspaceClient.
jobs.submit`) TWICE with two different `run_id`s, and asserts the plan's U9
acceptance bar:

1. Both submitted runs report `result_state == SUCCESS` ("job SUCCEEDED").
2. `gold.entities` row count equals the number of distinct *active*
   `entity_id`s in `entity_crosswalk` -- no duplicate or missing golden
   record for any active entity.
3. Re-running the pipeline (run #2, same catalog) produces an identical
   `entity_crosswalk` -- id stability across a re-run, not just cluster
   stability (see `pipeline/crosswalk.py::stable_assign`).
4. Every U7 quality check (`pipeline.quality.run`) passes after run #2.

Needs a live Databricks workspace: a `WorkspaceClient` (job submission) and a
Spark session (table assertions). NOT part of the local pytest suite --
mirrors `scripts/verify_match.py`'s integration-only pattern. Run this
script itself via `databricks jobs submit` on your own workspace profile,
e.g.:

    databricks jobs submit --profile <your-profile> --json '{
      "run_name": "er-ref-verify-e2e",
      "tasks": [{
        "task_key": "verify_e2e",
        "spark_python_task": {"python_file": "examples/mdm-entity-resolution/scripts/verify_e2e.py"},
        "environment_key": "verify_env"
      }],
      "environments": [{
        "environment_key": "verify_env",
        "spec": {
          "client": "1",
          "dependencies": ["databricks-vectorsearch>=0.40", "databricks-sdk>=0.30.0", "pyyaml>=6.0"]
        }
      }]
    }'
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

# Run standalone as its own spark_python_task (see run_all.py's identical
# bootstrap for why __file__ needs a sys.argv[0] fallback under that harness).
try:
    _THIS_FILE = Path(__file__)
except NameError:
    _THIS_FILE = Path(sys.argv[0])
_REPO_ROOT = _THIS_FILE.resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
# verify()'s U7 recheck (pipeline.quality) imports Watchdog's mdm_checks directly --
# make that importable too. `watchdog` lives at <repo_root>/engine/src/watchdog/.
_WATCHDOG_SRC = _REPO_ROOT.parent.parent / "engine" / "src"
if _WATCHDOG_SRC.is_dir() and str(_WATCHDOG_SRC) not in sys.path:
    sys.path.insert(0, str(_WATCHDOG_SRC))

from pipeline.config import Cfg  # noqa: E402 (must follow the sys.path bootstrap above)

# Workspace-relative path to the U9 entrypoint, as synced by the bundle (see
# jobs/pipeline_job.yml). Adjust if this script's deployment root differs.
_PIPELINE_PYTHON_FILE = "examples/mdm-entity-resolution/pipeline/run_all.py"

# Matches jobs/pipeline_job.yml's `er_ref_env` so a submitted ad hoc run gets
# the same dependencies as the scheduled job.
_ENV_DEPENDENCIES = ["databricks-vectorsearch>=0.40", "databricks-sdk>=0.30.0", "pyyaml>=6.0"]

_SUBMIT_TIMEOUT_SECONDS = 3600


def _submit_pipeline_run(ws: Any, run_id: str, catalog: str) -> Any:
    """Submit one `run_all.py` run (`databricks jobs submit` semantics) and
    block until it reaches a terminal state. Returns the terminal `Run`.
    """
    from databricks.sdk.service import compute, jobs

    waiter = ws.jobs.submit(
        run_name=f"er-ref-e2e-{run_id}",
        tasks=[
            jobs.SubmitTask(
                task_key="run_all",
                environment_key="er_ref_env",
                spark_python_task=jobs.SparkPythonTask(
                    python_file=_PIPELINE_PYTHON_FILE,
                    parameters=["--run-id", run_id, "--catalog", catalog],
                ),
            )
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="er_ref_env",
                spec=compute.Environment(client="1", dependencies=_ENV_DEPENDENCIES),
            )
        ],
    )
    return waiter.result(timeout=_SUBMIT_TIMEOUT_SECONDS)


def _assert_run_succeeded(run: Any, run_id: str) -> None:
    from databricks.sdk.service.jobs import RunResultState

    result_state = run.state.result_state if run.state is not None else None
    assert result_state == RunResultState.SUCCESS, (
        f"submitted run for run_id={run_id!r} did not succeed: "
        f"result_state={result_state}, state_message={getattr(run.state, 'state_message', None)}"
    )


def _active_crosswalk(spark: Any, cfg: Cfg) -> dict[str, str]:
    """Snapshot `source_record_id -> entity_id` for every active crosswalk row."""
    rows = (
        spark.table(cfg.tbl("mdm", "entity_crosswalk"))
        .filter("status = 'active'")
        .select("source_record_id", "entity_id")
        .collect()
    )
    return {row["source_record_id"]: row["entity_id"] for row in rows}


def verify(spark: Any, ws: Any, cfg: Cfg | None = None) -> dict[str, Any]:
    """Run the pipeline twice end to end and assert the U9 acceptance bar.

    Returns a metrics dict on success; raises `AssertionError` (or lets a
    submission/SDK error propagate) otherwise.
    """
    cfg = cfg or Cfg()

    run_id_1 = f"e2e-{uuid.uuid4().hex[:8]}"
    run_1 = _submit_pipeline_run(ws, run_id_1, cfg.catalog)
    _assert_run_succeeded(run_1, run_id_1)

    entities_count = spark.table(cfg.tbl("gold", "entities")).count()
    xwalk_after_run_1 = _active_crosswalk(spark, cfg)
    distinct_active_entities = len(set(xwalk_after_run_1.values()))
    assert entities_count == distinct_active_entities, (
        f"gold.entities count ({entities_count}) != distinct active entity_id count "
        f"({distinct_active_entities}) after run_id={run_id_1!r}"
    )

    # Re-run for the same catalog -- id stability means the *same* active
    # crosswalk (source_record_id -> entity_id) must come back out, even
    # though this is a fresh run_id and every stage overwrote its table.
    run_id_2 = f"e2e-{uuid.uuid4().hex[:8]}"
    run_2 = _submit_pipeline_run(ws, run_id_2, cfg.catalog)
    _assert_run_succeeded(run_2, run_id_2)

    xwalk_after_run_2 = _active_crosswalk(spark, cfg)
    assert xwalk_after_run_1 == xwalk_after_run_2, (
        "entity_crosswalk changed across an idempotent re-run (id instability): "
        f"{len(set(xwalk_after_run_1.items()) ^ set(xwalk_after_run_2.items()))} rows differ"
    )

    from pipeline.quality import run as run_quality_checks

    issues = run_quality_checks(spark, cfg, run_id_2)
    failed = [issue for issue in issues if not issue.get("passed", False)]
    assert not failed, f"U7 quality checks failed: {failed}"

    return {
        "run_id_1": run_id_1,
        "run_id_2": run_id_2,
        "entities_count": entities_count,
        "distinct_active_entities": distinct_active_entities,
        "crosswalk_stable": True,
        "quality_checks_passed": len(issues),
    }


if __name__ == "__main__":
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    _spark = SparkSession.builder.getOrCreate()
    _ws = WorkspaceClient()
    print(verify(_spark, _ws, Cfg()))
