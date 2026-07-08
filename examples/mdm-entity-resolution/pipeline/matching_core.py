"""Match scoring core (U4) — dependency-free, meant to be shared between the
batch matcher (``pipeline.match``) and any interactive/agent-based consumer
that needs to score the same pair identically (e.g. a chat agent surfacing
"is this the same part?" against the same golden records).

Pure Python, stdlib only: no pyspark, no Databricks SDK, no I/O. Safe to import
from a Spark job/notebook *and* from any other Python runtime.

Categorizes a candidate pair ``(a, b)`` plus a similarity ``score`` into one of
four categories (an electronic-parts obsolescence story: is a replacement part
the same, a minor variant, or a cross-manufacturer functional equivalent?) and
a decision:

  EXACT                 — same normalized MPN
  ALT_PART              — same manufacturer, MPN differs only by a
                           revision/packaging suffix
  FUNCTIONAL_EQUIVALENT — different manufacturer, same commodity, high
                           functional similarity -> candidate obsolescence
                           replacement; ALWAYS routed to steward review
  NO_MATCH              — nothing close enough

  auto_match    — EXACT or high-confidence ALT_PART
  needs_review  — FUNCTIONAL_EQUIVALENT, or any other match scoring 0.70-0.90
  no_match      — NO_MATCH

Category assignment requires a real grounding signal (same normalized MPN, or
same manufacturer + a variant suffix, or cross-manufacturer + same commodity)
-- never the raw similarity ``score`` alone. An embedding similarity score is
not a reliable same-part signal by itself: within one commodity family, MPNs
share a template ("IC-opamp OPxxx revN") that embeds very similarly even
across genuinely different parts, so a score-only fallback (an earlier
version of this function had one) auto-matched unrelated same-manufacturer
parts as EXACT purely from a high score, tanking live precision to 0.40 on
`scripts/verify_match.py`'s bar of >=0.95 -- confirmed by replaying the actual
false-positive pairs against a live run (e.g. two different Photonis PMT part
numbers scoring 0.95+ on embed_func despite no MPN relationship).
"""

from __future__ import annotations

import re
from typing import Any


def norm_mpn(s: str | None) -> str:
    """Normalize a manufacturer part number for matching/dedup.

    Strips whitespace, dashes, underscores, dots and slashes, then upper-cases,
    so 'CR-2032', 'cr2032', 'CR 2032' all collapse to the same key.
    """
    return re.sub(r"[\s\-_./]", "", s or "").upper()


def mpn_variant(a: str | None, b: str | None) -> bool:
    """True if one normalized MPN is a prefix of the other (revision/packaging
    suffix), e.g. 'LM317T' vs 'LM317TG'. Not identical, but the same base part.
    """
    ka, kb = norm_mpn(a), norm_mpn(b)
    if not ka or not kb or ka == kb:
        return False
    short, long_ = sorted((ka, kb), key=len)
    return long_.startswith(short) and (len(long_) - len(short)) <= 3


def categorize(a: dict[str, Any], b: dict[str, Any], score: float) -> dict[str, Any]:
    """Categorize a candidate pair given a similarity score.

    ``a``/``b`` are record-shaped dicts with (at least) ``mpn``, ``manufacturer``,
    ``commodity`` — the ``silver.source_records`` shape, so the batch matcher can
    pass rows straight through. ``score`` is the raw similarity (e.g. from a
    Vector Search query) before any category-driven confidence bump.

    Returns ``{"category": ..., "decision": ..., "confidence": ...}``.
    """
    a_mpn = a.get("mpn", "")
    b_mpn = b.get("mpn", "")
    a_mfr = (a.get("manufacturer", "") or "").strip().lower()
    b_mfr = (b.get("manufacturer", "") or "").strip().lower()
    a_comm = (a.get("commodity", "") or "").strip().lower()
    b_comm = (b.get("commodity", "") or "").strip().lower()

    same_mpn = bool(a_mpn) and norm_mpn(a_mpn) == norm_mpn(b_mpn)
    same_mfr = bool(a_mfr) and a_mfr == b_mfr
    same_comm = bool(a_comm) and a_comm == b_comm
    variant = mpn_variant(a_mpn, b_mpn)

    conf = float(score)
    if same_mpn:
        category, conf = "EXACT", max(conf, 0.98)
    elif same_mfr and variant:
        category, conf = "ALT_PART", max(conf, 0.85)
    elif not same_mfr and same_comm and conf >= 0.80:
        category = "FUNCTIONAL_EQUIVALENT"
    else:
        category = "NO_MATCH"

    needs_review = category == "FUNCTIONAL_EQUIVALENT" or (
        category != "NO_MATCH" and conf < 0.90
    )
    if category == "NO_MATCH":
        decision = "no_match"
    elif needs_review:
        decision = "needs_review"
    else:
        decision = "auto_match"

    return {"category": category, "decision": decision, "confidence": round(conf, 3)}
