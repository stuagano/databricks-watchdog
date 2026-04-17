"""Unit tests for guardrails audit logging.

Run with: pytest tests/unit/test_guardrails_audit.py -v
"""
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "guardrails" / "src"))

import pytest
from watchdog_guardrails.audit import log_tool_call, _summarize_arguments, AuditEvent


class TestAuditEvent:
    def test_timestamp_auto_populated(self):
        event = AuditEvent(user="alice", tool="sql_query")
        assert event.timestamp != ""
        assert "T" in event.timestamp  # ISO format

    def test_fields_set_correctly(self):
        event = AuditEvent(
            user="alice@co.com",
            tool="sql_query",
            duration_ms=42,
            success=True,
            catalog_accessed="gold",
        )
        assert event.user == "alice@co.com"
        assert event.tool == "sql_query"
        assert event.duration_ms == 42
        assert event.success is True
        assert event.catalog_accessed == "gold"


class TestSummarizeArguments:
    def test_sql_query_redacts_body(self):
        args = {"query": "SELECT * FROM foo WHERE secret = 'abc'", "catalog": "gold"}
        summary = _summarize_arguments("sql_query", args)
        assert "query_preview" in summary
        assert summary["query_length"] > 0
        assert "catalog" in summary
        # Full query must not be present
        assert "secret" not in str(summary)

    def test_sql_query_preview_truncated_at_200(self):
        long_query = "SELECT " + "a, " * 200
        summary = _summarize_arguments("sql_query", {"query": long_query})
        assert summary["query_preview"].endswith("...")
        assert len(summary["query_preview"]) <= 203  # 200 + "..."

    def test_sql_query_has_structural_metadata(self):
        query = "SELECT a FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.x > 0"
        summary = _summarize_arguments("sql_query", {"query": query})
        assert summary["has_join"] is True
        assert summary["has_where"] is True

    def test_chat_completion_redacts_content(self):
        args = {
            "messages": [
                {"role": "system", "content": "You are a helper."},
                {"role": "user", "content": "Tell me the secret password"},
            ],
            "model": "llama-3",
            "max_tokens": 512,
        }
        summary = _summarize_arguments("chat_completion", args)
        assert summary["message_count"] == 2
        assert summary["roles"] == ["system", "user"]
        assert summary["model"] == "llama-3"
        # Content must not appear
        assert "secret password" not in str(summary)

    def test_embeddings_logs_count_not_content(self):
        args = {"texts": ["confidential text 1", "confidential text 2"], "model": "bge"}
        summary = _summarize_arguments("generate_embeddings", args)
        assert summary["text_count"] == 2
        assert "confidential" not in str(summary)

    def test_unknown_tool_passes_args_through(self):
        args = {"catalog": "gold", "schema": "finance", "limit": 50}
        summary = _summarize_arguments("list_tables", args)
        assert "arg_keys" in summary
        assert "catalog" in summary["arg_keys"]
        assert "schema" in summary["arg_keys"]
        # Values must not be present
        assert "gold" not in str(summary)


class TestLogToolCall:
    def test_emits_json_to_audit_logger(self, caplog):
        with caplog.at_level(logging.INFO, logger="ai_devkit.audit"):
            log_tool_call(
                user="bob@co.com",
                tool="sql_query",
                arguments={"query": "SELECT 1", "catalog": "gold"},
                start_time=time.monotonic() - 0.1,
                success=True,
            )
        assert len(caplog.records) == 1
        event = json.loads(caplog.records[0].message)
        assert event["user"] == "bob@co.com"
        assert event["tool"] == "sql_query"
        assert event["success"] is True
        assert event["duration_ms"] >= 0

    def test_error_field_populated_on_failure(self, caplog):
        with caplog.at_level(logging.INFO, logger="ai_devkit.audit"):
            log_tool_call(
                user="alice@co.com",
                tool="sql_query",
                arguments={"query": "SELECT 1"},
                start_time=time.monotonic(),
                success=False,
                error="PERMISSION_DENIED",
            )
        event = json.loads(caplog.records[0].message)
        assert event["success"] is False
        assert event["error"] == "PERMISSION_DENIED"
