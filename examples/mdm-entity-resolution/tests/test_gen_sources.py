from pipeline.gen_sources import generate


def test_overlap_and_conflicts_exist():
    d = generate(seed=7)
    assert set(d) == {"erp", "plm", "procurement", "truth"}
    ids = {r["source_record_id"] for s in ("erp", "plm", "procurement") for r in d[s]}
    # every truth member is a real emitted source record (grounding)
    for t in d["truth"]:
        for m in t["members"]:
            assert m in ids
    # at least some entities appear in >=2 sources (real matching work)
    assert sum(1 for t in d["truth"] if len(t["members"]) >= 2) >= 20


def test_deterministic():
    assert generate(seed=7) == generate(seed=7)
