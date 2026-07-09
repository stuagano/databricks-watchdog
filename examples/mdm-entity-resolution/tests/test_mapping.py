from pipeline.mapping import CANONICAL_FIELDS, load_spec, validate


def test_spec_covers_every_field_for_every_source(tmp_path):
    spec = load_spec("config/mapping_spec.json")
    errs = validate(spec, ["erp", "plm", "procurement"])
    assert errs == [], errs
    for s in ("erp", "plm", "procurement"):
        assert set(spec[s]) >= set(CANONICAL_FIELDS)
