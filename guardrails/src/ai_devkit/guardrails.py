"""Governance guardrails for AI DevKit MCP tools.

Enforces platform-level restrictions that apply regardless of the
caller's UC permissions. UC handles "can this user see this data?"
— guardrails handle "should this operation be allowed at all through
the AI service?"

These are defense-in-depth rules, not access control. Even if a user
has DDL privileges in UC, they shouldn't be running DROP TABLE through
an MCP tool designed for analytics consumption.
"""

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# SQL statements that are never allowed through the AI DevKit.
# Users who need DDL/DML should use notebooks or the SQL editor directly.
_BLOCKED_SQL_PATTERNS = [
    (r"\b(DROP)\s+(TABLE|SCHEMA|CATALOG|DATABASE|VIEW|FUNCTION)", "DROP operations"),
    (r"\b(DELETE)\s+FROM\b", "DELETE statements"),
    (r"\b(TRUNCATE)\s+TABLE\b", "TRUNCATE statements"),
    (r"\b(ALTER)\s+(TABLE|SCHEMA|CATALOG)", "ALTER operations"),
    (r"\b(CREATE)\s+(TABLE|SCHEMA|CATALOG|DATABASE|VIEW)", "CREATE operations"),
    (r"\b(INSERT)\s+INTO\b", "INSERT statements"),
    (r"\b(UPDATE)\s+\w+\s+SET\b", "UPDATE statements"),
    (r"\b(MERGE)\s+INTO\b", "MERGE statements"),
    (r"\b(GRANT)\b", "GRANT statements"),
    (r"\b(REVOKE)\b", "REVOKE statements"),
]

# Maximum query length to prevent abuse
MAX_SQL_LENGTH = 10_000

# Maximum tokens per chat completion request
MAX_CHAT_TOKENS = 8192

# Maximum texts per embedding request
MAX_EMBEDDING_TEXTS = 150


@dataclass
class GuardrailResult:
    allowed: bool
    reason: str = ""


def check_sql_query(query: str) -> GuardrailResult:
    """Enforce read-only SQL through the AI DevKit.

    The MCP service is a consumption layer — analytics, exploration,
    and AI-powered queries. Mutations belong in notebooks, pipelines,
    or the SQL editor where there's proper review and version control.
    """
    if not query or not query.strip():
        return GuardrailResult(False, "Empty query")

    if len(query) > MAX_SQL_LENGTH:
        return GuardrailResult(
            False, f"Query exceeds {MAX_SQL_LENGTH} character limit"
        )

    upper = query.upper()

    for pattern, description in _BLOCKED_SQL_PATTERNS:
        if re.search(pattern, upper):
            return GuardrailResult(
                False,
                f"{description} not allowed through AI DevKit MCP. "
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
