"""Fixtures for watchdog unit tests.

Unit tests are pure Python — no Spark, no Databricks connection needed.
Run from the repo root:
    pip install pyyaml pytest
    PYTHONPATH=engine/src pytest tests/unit/
"""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
ENGINE_ROOT = REPO_ROOT / "engine"

# Ensure watchdog package is importable from engine/src/
sys.path.insert(0, str(ENGINE_ROOT / "src"))

import pytest


@pytest.fixture(scope="session")
def ontology_dir() -> str:
    """Path to the engine ontologies/ directory."""
    return str(ENGINE_ROOT / "ontologies")


@pytest.fixture(scope="session")
def policies_dir() -> str:
    """Path to the engine policies/ directory."""
    return str(ENGINE_ROOT / "policies")
