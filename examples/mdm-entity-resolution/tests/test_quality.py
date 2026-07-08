from watchdog.mdm_checks import interpret


def test_dedup_detects_duplicate_entity_ids():
    check={"id":"POL-GOLD-001","kind":"dedup","table":"gold.entities","keys":["entity_id"]}
    assert interpret(check, [{"entity_id":"e1","n":2}])["passed"] is False
    assert interpret(check, [])["passed"] is True
