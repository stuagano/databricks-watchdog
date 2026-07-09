from pipeline.standardize import norm_mpn


def test_mpn_normalization():
    assert norm_mpn("op07-cp") == "OP07CP"
    assert norm_mpn("CR 2032") == "CR2032"


def test_silver_conforms(spark, tmp_path):
    from pipeline.config import Cfg
    from pipeline.gen_sources import generate
    from pipeline.mapping import load_spec
    from pipeline.standardize import to_silver

    d = generate(7)
    for s in ("erp", "plm", "procurement"):
        spark.createDataFrame(d[s]).createOrReplaceTempView(f"bronze_{s}")
    df = to_silver(spark, Cfg(catalog="_"), load_spec("config/mapping_spec.json"), views=True)
    cols = set(df.columns)
    assert {"source_record_id", "source_system", "mpn_key", "source_trust", "embed_func"} <= cols
    assert df.filter("source_record_id IS NULL").count() == 0
