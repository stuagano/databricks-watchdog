"""Shared pytest fixtures for the reference pipeline.

The ``spark`` fixture is a session-scoped **local** SparkSession — pure-logic and
schema-conformance tests run against it with no Databricks workspace. Tests that
need Vector Search / Jobs / live Delta are integration tests run on the workspace
and are not driven by this fixture.
"""

import sys
from pathlib import Path

import pytest

# Make ``pipeline`` importable when pytest is invoked from the repo root or elsewhere.
sys.path.insert(0, str(Path(__file__).parent))

# Make Watchdog's pure ``mdm_checks`` builders/interpreters importable (U7 quality
# gates reuse them rather than reimplementing dedup/reconcile/completeness SQL).
# This example lives at <repo_root>/examples/mdm-entity-resolution/, and the
# `watchdog` package lives at <repo_root>/engine/src/watchdog/.
_WATCHDOG_SRC = Path(__file__).parent.parent.parent / "engine" / "src"
if _WATCHDOG_SRC.is_dir():
    sys.path.insert(0, str(_WATCHDOG_SRC))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("er-ref-tests")
        .getOrCreate()
    )
    yield s
    s.stop()
