from pipeline.crosswalk import connected_components


def test_transitive_closure():
    cc = connected_components([("a","b"),("b","c"),("d","e")])
    assert cc["a"]==cc["b"]==cc["c"]           # one cluster
    assert cc["d"]==cc["e"] and cc["d"]!=cc["a"]  # separate cluster


def test_singletons_isolated():
    cc = connected_components([("a","b")])
    assert "z" not in cc                        # nodes not in any pair aren't invented


def test_id_stability_and_merge():
    from pipeline.crosswalk import stable_assign
    # first run: two clusters
    xw = stable_assign(clusters={"r1":"A","r2":"A","r3":"B"}, prior={})
    assert xw["r1"]==xw["r2"] and xw["r1"]!=xw["r3"]
    # second run, same clusters, prior known -> ids unchanged
    xw2 = stable_assign(clusters={"r1":"A","r2":"A","r3":"B"}, prior=xw)
    assert xw2==xw
    # merge: r3 now joins r1's cluster -> keep r1's id, supersede B
    xw3 = stable_assign(clusters={"r1":"A","r2":"A","r3":"A"}, prior=xw)
    assert xw3["r3"]==xw3["r1"]
