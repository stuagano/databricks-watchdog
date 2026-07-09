from pipeline.survivorship import merge_entity, pick

R = [
 {"source_record_id":"plm:1","description":"Precision op-amp DIP-8","source_trust":3,"attribute_ts":"2026-06-01","lifecycle_status":"active"},
 {"source_record_id":"erp:1","description":"opamp","source_trust":2,"attribute_ts":"2026-01-01","lifecycle_status":"obsolete"},
]
def test_most_trusted_source():
    v, src = pick("description", "MOST_TRUSTED_SOURCE", R)
    assert src == "plm:1"
def test_most_recent():
    v, src = pick("lifecycle_status", "MOST_RECENT", R)
    assert src == "plm:1"
def test_most_complete():
    v, src = pick("description", "MOST_COMPLETE", R)
    assert src == "plm:1"  # longer/non-null wins
def test_provenance_traces_to_real_record():
    g = merge_entity(R, {"description":"MOST_TRUSTED_SOURCE","lifecycle_status":"MOST_RECENT"})
    assert g["field_provenance"]["description"] in {"plm:1","erp:1"}
