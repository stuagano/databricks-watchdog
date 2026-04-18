"""Unit tests for the custom exception hierarchy."""
import pytest
from watchdog.exceptions import (
    CrawlError,
    PermanentCrawlError,
    PermanentWatchdogError,
    PolicyLoadError,
    RemediationError,
    RemediationVerificationError,
    TransientCrawlError,
    TransientWatchdogError,
    WatchdogError,
)


class TestHierarchy:
    def test_transient_and_permanent_inherit_from_base(self):
        assert issubclass(TransientWatchdogError, WatchdogError)
        assert issubclass(PermanentWatchdogError, WatchdogError)

    def test_crawl_errors_carry_resource_type(self):
        err = TransientCrawlError("table", "metastore down")
        assert err.resource_type == "table"
        assert err.message == "metastore down"
        assert "table" in str(err)
        assert "metastore down" in str(err)

    def test_transient_crawl_is_both_crawl_and_transient(self):
        err = TransientCrawlError("grant", "rate limited")
        assert isinstance(err, CrawlError)
        assert isinstance(err, TransientWatchdogError)
        assert isinstance(err, WatchdogError)

    def test_permanent_crawl_is_both_crawl_and_permanent(self):
        err = PermanentCrawlError("table", "missing SELECT grant")
        assert isinstance(err, CrawlError)
        assert isinstance(err, PermanentWatchdogError)

    def test_policy_load_error_is_permanent(self):
        err = PolicyLoadError("bad yaml")
        assert isinstance(err, PermanentWatchdogError)

    def test_remediation_verification_is_remediation_error(self):
        err = RemediationVerificationError("grant did not take effect")
        assert isinstance(err, RemediationError)
        assert isinstance(err, WatchdogError)


class TestCallerRouting:
    """Callers should be able to distinguish error classes without string-matching."""

    def test_caller_can_catch_only_transient(self):
        def raise_transient():
            raise TransientCrawlError("table", "timeout")

        with pytest.raises(TransientWatchdogError):
            raise_transient()

    def test_caller_catching_permanent_does_not_catch_transient(self):
        with pytest.raises(TransientCrawlError):
            try:
                raise TransientCrawlError("table", "timeout")
            except PermanentWatchdogError:
                pytest.fail("transient error should not match permanent")
