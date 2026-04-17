"""Governance guardrails for AI DevKit MCP tools.

Enforces platform-level restrictions that apply regardless of the
caller's UC permissions. UC handles "can this user see this data?"
— guardrails handle "should this operation be allowed at all through
the AI service?"

These are defense-in-depth rules, not access control. Even if a user
has DDL privileges in UC, they shouldn't be running DROP TABLE through
an MCP tool designed for analytics consumption.
"""

import re
from dataclasses import dataclass

_BLOCKED_SQL_PATTERNS = [
    (r"\b(DROP)\s+(TABLE|SCHEMA|CATALOG|DATABASE|VIEW|FUNCTION)", "DROP operations"),
    (r"\b(DELETE)\s+FROM\b", "DELETE statements"),
    (r"\b(TRUNCATE)\s+TABLE\b", "TRUNCATE statements"),
    (r"\b(ALTER)\s+(TABLE|SCHEMA|CATALOG)", "ALTER operations"),
    (r"\b(CREATE)\s+(TABLE|SCHEMA|CATALOG|DATABASE|VIEW)", "CREATE operations"),
    (r"\b(CREATE)\s+OR\s+REPLACE\s+(TABLE|VIEW|FUNCTION)", "CREATE OR REPLACE operations"),
    (r"\b(INSERT)\s+INTO\b", "INSERT statements"),
    (r"\b(UPDATE)\s+[\w.]+\s+SET\b", "UPDATE statements"),
    (r"\b(MERGE)\s+INTO\b", "MERGE statements"),
    (r"\b(GRANT)\b", "GRANT statements"),
    (r"\b(REVOKE)\b", "REVOKE statements"),
]

MAX_SQL_LENGTH = 10_000

_SQL_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_SQL_LINE_COMMENT = re.compile(r"--[^\n]*")


def _strip_sql_comments(query: str) -> str:
    """Remove SQL comments before pattern matching to prevent injection bypass."""
    query = _SQL_BLOCK_COMMENT.sub(" ", query)
    query = _SQL_LINE_COMMENT.sub(" ", query)
    return query
MAX_CHAT_TOKENS = 8192
MAX_EMBEDDING_TEXTS = 150


@dataclass
class GuardrailResult:
    allowed: bool
    reason: str = ""


def check_sql_query(query: str) -> GuardrailResult:
    """Enforce read-only SQL through the AI service."""
    if not query or not query.strip():
        return GuardrailResult(False, "Empty query")

    if len(query) > MAX_SQL_LENGTH:
        return GuardrailResult(
            False, f"Query exceeds {MAX_SQL_LENGTH} character limit"
        )

    # Strip comments before pattern matching to prevent injection bypass
    stripped = _strip_sql_comments(query)
    upper = stripped.upper()
    for pattern, description in _BLOCKED_SQL_PATTERNS:
        if re.search(pattern, upper):
            return GuardrailResult(
                False,
                f"{description} not allowed through AI guardrails MCP. "
                f"Use a notebook or SQL editor for data modifications.",
            )

    return GuardrailResult(True)


def check_chat_completion(args: dict) -> GuardrailResult:
    """Validate chat completion parameters."""
    messages = args.get("messages", [])
    if not messages:
        return GuardrailResult(False, "No messages provided")

    max_tokens = args.get("max_tokens", 1024)
    if max_tokens > MAX_CHAT_TOKENS:
        return GuardrailResult(
            False,
            f"max_tokens ({max_tokens}) exceeds limit ({MAX_CHAT_TOKENS}). "
            f"Reduce to avoid excessive cost.",
        )

    return GuardrailResult(True)


def check_embeddings(args: dict) -> GuardrailResult:
    """Validate embedding request parameters."""
    texts = args.get("texts", [])
    if not texts:
        return GuardrailResult(False, "No texts provided")

    if len(texts) > MAX_EMBEDDING_TEXTS:
        return GuardrailResult(
            False,
            f"Too many texts ({len(texts)}). Max {MAX_EMBEDDING_TEXTS} per request.",
        )

    return GuardrailResult(True)
