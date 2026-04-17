"""Fixtures for watchdog unit tests.

Unit tests are pure Python — no Spark, no Databricks connection needed.
Run from the repo root:
    pip install pyyaml pytest
    PYTHONPATH=engine/src pytest tests/unit/

A minimal ``pyspark``/``pyspark.sql`` stub is installed into ``sys.modules``
at collection time so modules that do ``from pyspark.sql import SparkSession``
can be imported and their pure-Python surface exercised without a real Spark
runtime. Tests that need Spark must still rely on the integration tier.
"""
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).parent.parent.parent
ENGINE_ROOT = REPO_ROOT / "engine"

# Ensure watchdog package is importable from engine/src/
sys.path.insert(0, str(ENGINE_ROOT / "src"))

# ── pyspark stub ────────────────────────────────────────────────────────────
# The unit tier runs without pyspark installed; stub the submodules the
# production code imports at module-load time. Tests that exercise Spark
# logic replace spark with their own MagicMock — these stubs only satisfy
# ``from pyspark.sql import SparkSession`` and ``import pyspark.sql.types``.
if "pyspark" not in sys.modules:
    _pyspark = types.ModuleType("pyspark")
    _pyspark_sql = types.ModuleType("pyspark.sql")
    _pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
    _pyspark_sql_types = types.ModuleType("pyspark.sql.types")

    class _StructField:
        def __init__(self, name=None, dataType=None, nullable=True, *args, **kwargs):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

    class _StructType(list):
        def __init__(self, fields=None):
            fields = list(fields or [])
            super().__init__(fields)
            # Some test modules (and pyspark itself) access ``.fields`` on the
            # schema object — keep both in sync so either style works.
            self.fields = fields

    class _DataType:
        """Stand-in for any pyspark.sql.types scalar/container. Callable so
        parameterised types like MapType(StringType(), StringType()) don't
        blow up when they instantiate."""

        def __init__(self, *_args, **_kwargs):
            pass

        def __call__(self, *_args, **_kwargs):
            return _DataType()

    _pyspark_sql.SparkSession = MagicMock
    _pyspark_sql.DataFrame = MagicMock
    _pyspark_sql.Row = MagicMock
    _pyspark_sql_types.StructType = _StructType
    _pyspark_sql_types.StructField = _StructField
    for _name in (
        "StringType", "IntegerType", "BooleanType", "TimestampType",
        "DoubleType", "MapType", "ArrayType", "LongType",
    ):
        setattr(_pyspark_sql_types, _name, _DataType)
    _pyspark_sql_functions.current_timestamp = lambda *_a, **_k: None
    _pyspark_sql_functions.col = lambda *_a, **_k: None
    _pyspark_sql_functions.lit = lambda *_a, **_k: None

    _pyspark.sql = _pyspark_sql
    sys.modules["pyspark"] = _pyspark
    sys.modules["pyspark.sql"] = _pyspark_sql
    sys.modules["pyspark.sql.functions"] = _pyspark_sql_functions
    sys.modules["pyspark.sql.types"] = _pyspark_sql_types

# ── databricks.sdk / databricks.sql stub ────────────────────────────────────
# Same trick for the Databricks SDK so entrypoints.py can import it without
# pulling in the real dependency for the unit tier.
if "databricks" not in sys.modules:
    _db = types.ModuleType("databricks")
    _db_sdk = types.ModuleType("databricks.sdk")
    _db_sdk_service = types.ModuleType("databricks.sdk.service")
    _db_sdk_service_catalog = types.ModuleType("databricks.sdk.service.catalog")
    _db_sql = types.ModuleType("databricks.sql")
    _db_sdk.WorkspaceClient = MagicMock
    _db_sdk_service_catalog.SecurableType = MagicMock
    _db_sql.connect = MagicMock
    _db.sdk = _db_sdk
    _db.sql = _db_sql
    sys.modules["databricks"] = _db
    sys.modules["databricks.sdk"] = _db_sdk
    sys.modules["databricks.sdk.service"] = _db_sdk_service
    sys.modules["databricks.sdk.service.catalog"] = _db_sdk_service_catalog
    sys.modules["databricks.sql"] = _db_sql

import pytest


@pytest.fixture(scope="session")
def ontology_dir() -> str:
    """Path to the engine ontologies/ directory."""
    return str(ENGINE_ROOT / "ontologies")


@pytest.fixture(scope="session")
def policies_dir() -> str:
    """Path to the engine policies/ directory."""
    return str(ENGINE_ROOT / "policies")
