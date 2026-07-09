"""ctk demonstration: claim-vs-reality checks for matching_core.categorize.

A live run of this pipeline (er-live-7, see the CHANGELOG / matching_core's
module docstring) shipped a version of categorize() with score-only auto_match
fallback branches -- no same-mpn or same-mfr+variant grounding at all -- and
every existing plain-assert unit test still passed, because those tests only
ever checked a handful of hand-picked pairs. The bug wasn't "the function
crashes"; it was "the function claims auto_match and that claim is wrong."

claim_vs_reality makes that distinction a first-class check: the *claim* is
categorize()'s own decision ("auto_match"); the *reality* is whether a real
grounding signal (same normalized MPN, or same-manufacturer + a variant
suffix) actually justifies it. If a future change reintroduces a score-only
auto_match path, this fails with "SILENT FAILURE" instead of quietly passing.
"""

from ctk import claim_vs_reality
from pipeline.matching_core import categorize, mpn_variant, norm_mpn


def test_exact_auto_match_has_real_mpn_grounding():
    a = {"mpn": "CR-2032", "manufacturer": "Renata", "commodity": "battery"}
    b = {"mpn": "CR2032", "manufacturer": "Renata", "commodity": "battery"}
    result = categorize(a, b, 0.8)

    def check_grounded():
        assert norm_mpn(a["mpn"]) == norm_mpn(b["mpn"]), (
            "EXACT auto_match claimed with no same-normalized-mpn grounding"
        )

    claim_vs_reality(
        claimed_success=(result["decision"] == "auto_match"),
        verifier=check_grounded,
        claim_label="categorize() EXACT auto_match",
    )


def test_alt_part_auto_match_has_real_variant_grounding():
    a = {"mpn": "LM317T", "manufacturer": "TI", "commodity": "IC-regulator"}
    b = {"mpn": "LM317TG", "manufacturer": "TI", "commodity": "IC-regulator"}
    result = categorize(a, b, 0.9)

    def check_grounded():
        same_mfr = a["manufacturer"].strip().lower() == b["manufacturer"].strip().lower()
        assert same_mfr and mpn_variant(a["mpn"], b["mpn"]), (
            "ALT_PART auto_match claimed with no same-manufacturer+variant grounding"
        )

    claim_vs_reality(
        claimed_success=(result["decision"] == "auto_match"),
        verifier=check_grounded,
        claim_label="categorize() ALT_PART auto_match",
    )


def test_high_score_same_mfr_different_part_never_claims_auto_match():
    # The exact live false-positive pair (two different Photonis PMT part
    # numbers, no MPN relationship, high embedding score). The regression
    # test in test_matching_core.py already pins the category/decision
    # directly; this pins the same fact from the claim side: no auto_match
    # claim should ever be made here, so there's nothing for reality to
    # contradict.
    a = {"mpn": "R039", "manufacturer": "Photonis", "commodity": "PMT"}
    b = {"mpn": "R047", "manufacturer": "Photonis", "commodity": "PMT"}
    result = categorize(a, b, 0.96)
    assert result["decision"] != "auto_match"
