from pipeline.steward import to_review_row


def test_review_row_shape():
    r = to_review_row({"left_record_id":"erp:1","right_record_id":"plm:9","score":0.83,
                       "category":"FUNCTIONAL_EQUIVALENT"},
                      {"erp:1":{"mpn":"OP07CP"},"plm:9":{"mpn":"OPA277P","part_id":"plm:9"}})
    assert r["source_mpn"]=="OP07CP" and r["matched_mpn"]=="OPA277P"
    assert r["status"]=="pending" and r["needs_review"] is True
