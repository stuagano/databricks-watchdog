"""Shared pytest fixtures for the reference pipeline.

The ``spark`` fixture is a session-scoped **local** SparkSession — pure-logic and
schema-conformance tests run against it with no Databricks workspace. Tests that
need Vector Search / Jobs / live Delta are integration tests run on the workspace
and are not driven by this fixture.

Also wires in ``ctk`` (see ``ctk/``): a ``workspace``/``run_started_at`` fixture
pair for artifact-freshness checks, and an autouse ``fail_on_error_log`` guard
that fails a test if the code under test logged ERROR/CRITICAL, even if the
test's own asserts all passed -- the runtime counterpart to the swallowed-
exception scanner (``ctk.lint.find_swallowed_exceptions``). This example's own
live-run history is exactly the failure mode both exist to catch: local pytest
was green while the live matcher silently auto-matched unrelated parts.
"""

import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

# Make ``pipeline`` (and the ``ctk`` package alongside it) importable when
# pytest is invoked from the repo root or elsewhere.
sys.path.insert(0, str(Path(__file__).parent))

# Make Watchdog's pure ``mdm_checks`` builders/interpreters importable (U7 quality
# gates reuse them rather than reimplementing dedup/reconcile/completeness SQL).
# This example lives at <repo_root>/examples/mdm-entity-resolution/, and the
# `watchdog` package lives at <repo_root>/engine/src/watchdog/.
_WATCHDOG_SRC = Path(__file__).parent.parent.parent / "engine" / "src"
if _WATCHDOG_SRC.is_dir():
    sys.path.insert(0, str(_WATCHDOG_SRC))

from ctk.logguard import CapturingHandler  # noqa: E402 (must follow the sys.path bootstrap above)


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


@dataclass
class Workspace:
    root: Path

    def path(self, *parts: str) -> str:
        return str(self.root.joinpath(*parts))

    def write(self, name: str, content: str) -> str:
        p = self.root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return str(p)

    def read(self, name: str) -> str:
        return (self.root / name).read_text()


@pytest.fixture
def workspace(tmp_path: Path) -> Workspace:
    """Isolated scratch directory for a test's inputs/outputs (ctk)."""
    return Workspace(root=tmp_path)


@pytest.fixture
def run_started_at() -> float:
    """Epoch seconds at fixture setup, for ``ctk.Artifact(newer_than=...)`` --
    proves an output file was actually (re)written during this test, not left
    over from a previous run."""
    return time.time()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "allow_error_logs: do not fail the test if it logs at ERROR/CRITICAL level",
    )
    config.addinivalue_line("markers", "unit: fast, isolated tests with no real I/O")
    config.addinivalue_line("markers", "integration: tests that hit real dependencies")
    config.addinivalue_line("markers", "slow: long-running tests")


@pytest.fixture(autouse=True)
def fail_on_error_log(request: pytest.FixtureRequest):
    """ctk guard: fail a test if its code logged ERROR/CRITICAL, unless the
    test is explicitly marked ``@pytest.mark.allow_error_logs``."""
    if request.node.get_closest_marker("allow_error_logs"):
        yield
        return

    handler = CapturingHandler()
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        yield
    finally:
        root.removeHandler(handler)

    if handler.records:
        msgs = "\n".join(
            f"  - {r.levelname} {r.name}: {r.getMessage()}" for r in handler.records[:20]
        )
        pytest.fail(
            "code logged ERROR/CRITICAL during this test (likely a swallowed/"
            "handled-but-real failure):\n" + msgs,
            pytrace=False,
        )
