from pipeline.config import Cfg


def test_fqn_composition():
    c = Cfg(catalog="cat")
    assert c.tbl("silver", "source_records") == "cat.mdm_ref_silver.source_records"
    assert c.tbl("gold", "entities") == "cat.mdm_ref_gold.entities"


def test_default_catalog():
    c = Cfg()
    assert c.catalog == "main"
    assert c.tbl("match", "pairs") == "main.mdm_ref_match.pairs"


def test_schema_properties():
    c = Cfg(catalog="cat")
    assert c.bronze == "cat.mdm_ref_bronze"
    assert c.silver == "cat.mdm_ref_silver"
    assert c.match == "cat.mdm_ref_match"
    assert c.mdm == "cat.mdm_ref_mdm"
    assert c.gold == "cat.mdm_ref_gold"
