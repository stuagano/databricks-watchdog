from pipeline.matching_core import categorize


def test_exact_same_mpn_auto_match():
    d = categorize({"mpn":"CR-2032","manufacturer":"Renata","commodity":"battery"},
                   {"mpn":"CR2032","manufacturer":"Renata","commodity":"battery"}, 0.8)
    assert d["category"]=="EXACT" and d["decision"]=="auto_match"
def test_cross_mfr_functional_needs_review():
    d = categorize({"mpn":"OP07CP","manufacturer":"Analog Devices","commodity":"IC-opamp"},
                   {"mpn":"OPA277P","manufacturer":"Texas Instruments","commodity":"IC-opamp"}, 0.88)
    assert d["category"]=="FUNCTIONAL_EQUIVALENT" and d["decision"]=="needs_review"
def test_low_score_no_match():
    d = categorize({"mpn":"X","commodity":"a","manufacturer":"m"},
                   {"mpn":"Y","commodity":"b","manufacturer":"n"}, 0.3)
    assert d["decision"]=="no_match"
def test_high_score_same_mfr_different_part_no_match():
    # Regression: er-live-7's live run auto-matched unrelated same-manufacturer
    # parts as EXACT purely from a high embedding score (e.g. two different
    # Photonis PMT part numbers at 0.95+), tanking precision to 0.40. A high
    # score alone must never substitute for same_mpn or same_mfr+variant.
    d = categorize({"mpn":"R039","manufacturer":"Photonis","commodity":"PMT"},
                   {"mpn":"R047","manufacturer":"Photonis","commodity":"PMT"}, 0.96)
    assert d["category"]=="NO_MATCH" and d["decision"]=="no_match"
