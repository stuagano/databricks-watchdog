"""Unit tests for AI guardrails safety checks.

Run with: pytest tests/unit/test_guardrails.py -v
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "guardrails" / "src"))

from watchdog_guardrails.guardrails import (
    GuardrailResult,
    check_sql_query,
    check_chat_completion,
    check_embeddings,
    MAX_SQL_LENGTH,
    MAX_CHAT_TOKENS,
    MAX_EMBEDDING_TEXTS,
)


class TestCheckSqlQuery:
    def test_select_allowed(self):
        result = check_sql_query("SELECT * FROM catalog.schema.table")
        assert result.allowed

    def test_select_with_join_allowed(self):
        result = check_sql_query(
            "SELECT a.id, b.name FROM catalog.schema.a JOIN catalog.schema.b ON a.id = b.id"
        )
        assert result.allowed

    def test_drop_table_blocked(self):
        result = check_sql_query("DROP TABLE catalog.schema.my_table")
        assert not result.allowed
        assert "DROP" in result.reason

    def test_delete_from_blocked(self):
        result = check_sql_query("DELETE FROM catalog.schema.my_table WHERE id = 1")
        assert not result.allowed
        assert "DELETE" in result.reason

    def test_truncate_blocked(self):
        result = check_sql_query("TRUNCATE TABLE catalog.schema.my_table")
        assert not result.allowed

    def test_alter_table_blocked(self):
        result = check_sql_query("ALTER TABLE catalog.schema.my_table ADD COLUMN foo STRING")
        assert not result.allowed

    def test_create_table_blocked(self):
        result = check_sql_query("CREATE TABLE catalog.schema.new_table AS SELECT 1")
        assert not result.allowed

    def test_insert_into_blocked(self):
        result = check_sql_query("INSERT INTO catalog.schema.my_table VALUES (1, 'a')")
        assert not result.allowed

    def test_update_set_blocked(self):
        result = check_sql_query("UPDATE catalog.schema.my_table SET col = 1 WHERE id = 2")
        assert not result.allowed

    def test_merge_into_blocked(self):
        result = check_sql_query("MERGE INTO catalog.schema.target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET col = source.col")
        assert not result.allowed

    def test_grant_blocked(self):
        result = check_sql_query("GRANT SELECT ON TABLE catalog.schema.my_table TO user@co.com")
        assert not result.allowed

    def test_revoke_blocked(self):
        result = check_sql_query("REVOKE SELECT ON TABLE catalog.schema.my_table FROM user@co.com")
        assert not result.allowed

    def test_empty_query_blocked(self):
        result = check_sql_query("")
        assert not result.allowed

    def test_whitespace_only_blocked(self):
        result = check_sql_query("   ")
        assert not result.allowed

    def test_query_over_length_blocked(self):
        long_query = "SELECT " + "a, " * 5000
        assert len(long_query) > MAX_SQL_LENGTH
        result = check_sql_query(long_query)
        assert not result.allowed
        assert str(MAX_SQL_LENGTH) in result.reason

    def test_case_insensitive_blocking(self):
        result = check_sql_query("drop table catalog.schema.foo")
        assert not result.allowed


class TestCheckChatCompletion:
    def test_valid_messages_allowed(self):
        result = check_chat_completion({
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024,
        })
        assert result.allowed

    def test_at_token_limit_allowed(self):
        result = check_chat_completion({
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": MAX_CHAT_TOKENS,
        })
        assert result.allowed

    def test_over_token_limit_blocked(self):
        result = check_chat_completion({
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": MAX_CHAT_TOKENS + 1,
        })
        assert not result.allowed
        assert str(MAX_CHAT_TOKENS) in result.reason

    def test_empty_messages_blocked(self):
        result = check_chat_completion({"messages": []})
        assert not result.allowed

    def test_no_messages_key_blocked(self):
        result = check_chat_completion({})
        assert not result.allowed


class TestCheckEmbeddings:
    def test_single_text_allowed(self):
        result = check_embeddings({"texts": ["hello world"]})
        assert result.allowed

    def test_at_limit_allowed(self):
        result = check_embeddings({"texts": ["text"] * MAX_EMBEDDING_TEXTS})
        assert result.allowed

    def test_over_limit_blocked(self):
        result = check_embeddings({"texts": ["text"] * (MAX_EMBEDDING_TEXTS + 1)})
        assert not result.allowed
        assert str(MAX_EMBEDDING_TEXTS) in result.reason

    def test_empty_texts_blocked(self):
        result = check_embeddings({"texts": []})
        assert not result.allowed

    def test_no_texts_key_blocked(self):
        result = check_embeddings({})
        assert not result.allowed
