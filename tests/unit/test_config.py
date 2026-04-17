"""Unit tests for watchdog.config — env-driven configuration."""

import os

import pytest
from watchdog.config import WatchdogConfig


@pytest.fixture
def clean_env(monkeypatch):
    """Strip all WATCHDOG_* env vars so defaults apply deterministically."""
    for key in list(os.environ):
        if key.startswith("WATCHDOG_"):
            monkeypatch.delenv(key, raising=False)
    return monkeypatch


class TestDefaults:
    def test_defaults(self, clean_env):
        config = WatchdogConfig()
        assert config.catalog == "platform"
        assert config.schema == "watchdog"
        assert config.secret_scope == ""
        assert config.metastore_ids == []
        assert config.is_multi_metastore is False

    def test_qualified_schema_format(self, clean_env):
        config = WatchdogConfig()
        assert config.qualified_schema == "platform.watchdog"


class TestEnvOverrides:
    def test_catalog_override(self, clean_env):
        clean_env.setenv("WATCHDOG_CATALOG", "main")
        assert WatchdogConfig().catalog == "main"

    def test_schema_override(self, clean_env):
        clean_env.setenv("WATCHDOG_SCHEMA", "governance")
        assert WatchdogConfig().schema == "governance"

    def test_secret_scope_override(self, clean_env):
        clean_env.setenv("WATCHDOG_SECRET_SCOPE", "team-secrets")
        assert WatchdogConfig().secret_scope == "team-secrets"


class TestMultiMetastore:
    def test_single_metastore_env(self, clean_env):
        clean_env.setenv("WATCHDOG_METASTORE_IDS", "m-123")
        config = WatchdogConfig()
        assert config.metastore_ids == ["m-123"]
        # A single id is not 'multi'.
        assert config.is_multi_metastore is False

    def test_multiple_metastores(self, clean_env):
        clean_env.setenv("WATCHDOG_METASTORE_IDS", "m-1,m-2,m-3")
        config = WatchdogConfig()
        assert config.metastore_ids == ["m-1", "m-2", "m-3"]
        assert config.is_multi_metastore is True

    def test_ignores_blank_entries(self, clean_env):
        clean_env.setenv("WATCHDOG_METASTORE_IDS", "m-1, ,m-2,")
        assert WatchdogConfig().metastore_ids == ["m-1", "m-2"]

    def test_trims_whitespace(self, clean_env):
        clean_env.setenv("WATCHDOG_METASTORE_IDS", " m-1 , m-2 ")
        assert WatchdogConfig().metastore_ids == ["m-1", "m-2"]
