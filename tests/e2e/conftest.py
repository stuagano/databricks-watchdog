"""Fixtures for the Watchdog e2e tier.

E2E tests require a live Databricks workspace. Set ``WATCHDOG_E2E_ENABLED=1``
to opt in; otherwise every test in this directory is auto-skipped so CI stays
green without credentials.
"""
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "engine" / "src"))


def pytest_collection_modifyitems(config, items):
    if os.environ.get("WATCHDOG_E2E_ENABLED") == "1":
        return
    skip_marker = pytest.mark.skip(reason="Set WATCHDOG_E2E_ENABLED=1 to run e2e tests")
    for item in items:
        item.add_marker(skip_marker)


@pytest.fixture(scope="session")
def test_catalog() -> str:
    catalog = os.environ.get("WATCHDOG_TEST_CATALOG")
    if not catalog:
        pytest.skip("WATCHDOG_TEST_CATALOG must be set for e2e tests")
    return catalog


@pytest.fixture(scope="session")
def spark():
    """Return an active SparkSession via databricks-connect."""
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        pytest.skip("databricks-connect not installed; cannot run e2e tests")
    return DatabricksSession.builder.getOrCreate()
