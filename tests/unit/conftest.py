"""Fixtures for watchdog unit tests.

Unit tests are pure Python — no Spark, no Databricks connection needed.
Run from the bundle root:
    cd bundles/watchdog-bundle
    pip install -e src/
    pytest tests/unit/
"""
import sys
from pathlib import Path

# Ensure watchdog package is importable from src/
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pytest

BUNDLE_ROOT = Path(__file__).parent.parent.parent


@pytest.fixture(scope="session")
def ontology_dir() -> str:
    """Path to the live ontologies/ directory."""
    return str(BUNDLE_ROOT / "ontologies")


@pytest.fixture(scope="session")
def policies_dir() -> str:
    """Path to the live policies/ directory."""
    return str(BUNDLE_ROOT / "policies")
