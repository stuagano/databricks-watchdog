"""Unit tests for watchdog.notifications — webhook payloads, digest math.

Spark-dependent functions (build_owner_digests, write_to_queue) are covered
by integration tests. These unit tests focus on pure-Python paths:
  - OwnerDigest aggregation
  - build_webhook_payload for each supported flavor
  - send_webhook_notifications delivery with a fake urlopen
"""

from unittest.mock import MagicMock, patch

import pytest
from watchdog.notifications import (
    OwnerDigest,
    build_webhook_payload,
    send_webhook_notifications,
)


def _violation(severity="high", policy="POL-C002", resource="dev.default.t",
               detail="Missing tag", remediation="Add tag"):
    return {
        "violation_id": f"v-{resource}",
        "policy_id": policy,
        "severity": severity,
        "resource_name": resource,
        "detail": detail,
        "remediation": remediation,
    }


def _digest(owner="alice@co.com", critical=0, high=0, medium=0, low=0):
    violations = []
    for i in range(critical):
        violations.append(_violation(severity="critical", resource=f"t{i}-c"))
    for i in range(high):
        violations.append(_violation(severity="high", resource=f"t{i}-h"))
    for i in range(medium):
        violations.append(_violation(severity="medium", resource=f"t{i}-m"))
    for i in range(low):
        violations.append(_violation(severity="low", resource=f"t{i}-l"))
    return OwnerDigest(
        owner=owner, violations=violations,
        critical=critical, high=high, medium=medium, low=low,
    )


class TestOwnerDigest:
    def test_total_sums_severities(self):
        d = _digest(critical=2, high=3, medium=1, low=4)
        assert d.total == 10

    def test_severity_summary_skips_zero(self):
        d = _digest(critical=0, high=2, medium=0, low=1)
        assert d.severity_summary == "2 high, 1 low"

    def test_empty_digest_summary(self):
        d = _digest()
        assert d.severity_summary == "none"
        assert d.total == 0


class TestBuildWebhookPayload:
    def test_generic_includes_counts_and_violations(self):
        d = _digest(high=2)
        payload = build_webhook_payload(d, dashboard_url="https://example.com")
        assert payload["owner"] == "alice@co.com"
        assert payload["total"] == 2
        assert payload["high"] == 2
        assert payload["dashboard_url"] == "https://example.com"
        assert len(payload["violations"]) == 2
        assert payload["violations"][0]["policy_id"] == "POL-C002"

    def test_slack_flavor_shape(self):
        d = _digest(high=1)
        payload = build_webhook_payload(d, flavor="slack")
        assert "blocks" in payload
        assert payload["blocks"][0]["type"] == "header"
        # header text mentions owner and count
        assert "alice@co.com" in payload["blocks"][0]["text"]["text"]

    def test_slack_truncates_long_lists(self):
        d = _digest(high=20)
        payload = build_webhook_payload(d, flavor="slack")
        # 1 header + 10 sections + 1 context footer = 12 blocks
        assert len(payload["blocks"]) == 12
        # The footer block must mention the remainder
        assert "10 more" in payload["blocks"][-1]["elements"][0]["text"]

    def test_teams_flavor_shape(self):
        d = _digest(critical=1, high=2)
        payload = build_webhook_payload(d, flavor="teams",
                                         dashboard_url="https://x")
        assert payload["@type"] == "MessageCard"
        assert payload["themeColor"] == "B00020"  # critical present
        # facts include all severity labels
        fact_names = {f["name"] for f in payload["sections"][0]["facts"]}
        assert fact_names == {"Critical", "High", "Medium", "Low"}
        assert payload["potentialAction"][0]["targets"][0]["uri"] == "https://x"


class TestSendWebhookNotifications:
    def test_rejects_non_http_url(self):
        assert send_webhook_notifications([_digest(high=1)], "ftp://evil.example") == 0

    def test_rejects_empty_url(self):
        assert send_webhook_notifications([_digest(high=1)], "") == 0

    def test_posts_one_per_digest(self):
        digests = [_digest(owner=f"u{i}@co.com", high=1) for i in range(3)]

        fake_response = MagicMock()
        fake_response.status = 200
        # urlopen is a context manager
        fake_response.__enter__ = MagicMock(return_value=fake_response)
        fake_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=fake_response) as m:
            sent = send_webhook_notifications(
                digests, "https://hooks.example/abc", flavor="generic",
            )
        assert sent == 3
        assert m.call_count == 3

    def test_counts_only_2xx_as_sent(self):
        responses = []
        for status in (200, 500, 204):
            r = MagicMock()
            r.status = status
            r.__enter__ = MagicMock(return_value=r)
            r.__exit__ = MagicMock(return_value=False)
            responses.append(r)

        with patch("urllib.request.urlopen", side_effect=responses):
            sent = send_webhook_notifications(
                [_digest(owner=f"u{i}@co.com", high=1) for i in range(3)],
                "https://hooks.example/abc",
            )
        assert sent == 2

    def test_swallows_exceptions(self):
        with patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
            sent = send_webhook_notifications(
                [_digest(high=1)], "https://hooks.example/abc",
            )
        assert sent == 0
