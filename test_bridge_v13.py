"""v13 multi-collection retrieval tests — radio-free (ast-extraction harness,
same pattern as test_bridge_acks.py: bridge.py is never imported)."""
import ast, io, os

BRIDGE = os.path.join(os.path.dirname(__file__), "bridge.py")
SRC = io.open(BRIDGE, encoding="utf-8").read()


def _extract(func_name, extra_globals=None):
    tree = ast.parse(SRC)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            mod = ast.Module(body=[node], type_ignores=[])
            ns = dict(extra_globals or {})
            exec(compile(mod, BRIDGE, "exec"), ns)
            return ns[func_name], ns
    raise AssertionError("function {} not found in bridge.py".format(func_name))


def _search(collections, results):
    """qdrant_search with a stubbed per-collection searcher. `results` maps
    collection name -> hit list or None (failure)."""
    fn, _ = _extract("qdrant_search", {
        "QDRANT_COLLECTIONS": collections,
        "_qdrant_search_one": lambda vector, limit, coll: results[coll],
    })
    return fn


def test_merges_across_collections_score_desc():
    fn = _search(["a", "b"], {
        "a": [{"score": 0.9, "payload": {"text": "a1"}}, {"score": 0.5, "payload": {"text": "a2"}}],
        "b": [{"score": 0.7, "payload": {"text": "b1"}}],
    })
    out = fn([0.0], 8)
    assert [h["score"] for h in out] == [0.9, 0.7, 0.5]


def test_limit_trims_merged_result():
    fn = _search(["a", "b"], {
        "a": [{"score": 0.9, "payload": {}}, {"score": 0.8, "payload": {}}],
        "b": [{"score": 0.85, "payload": {}}, {"score": 0.1, "payload": {}}],
    })
    out = fn([0.0], 2)
    assert [h["score"] for h in out] == [0.9, 0.85]


def test_one_collection_down_degrades_not_blinds():
    fn = _search(["a", "b"], {
        "a": None,
        "b": [{"score": 0.7, "payload": {"text": "b1"}}],
    })
    out = fn([0.0], 8)
    assert out == [{"score": 0.7, "payload": {"text": "b1"}}]


def test_all_collections_down_returns_none():
    fn = _search(["a", "b"], {"a": None, "b": None})
    assert fn([0.0], 8) is None


def test_empty_hits_is_not_failure():
    # a collection returning [] (no matches) must not count as a failure
    fn = _search(["a", "b"], {"a": [], "b": None})
    assert fn([0.0], 8) == []


def test_single_collection_backcompat():
    fn = _search(["only"], {"only": [{"score": 0.6, "payload": {}}]})
    assert fn([0.0], 8) == [{"score": 0.6, "payload": {}}]


def test_source_env_parsing_and_backcompat():
    assert 'QDRANT_COLLECTIONS = [c.strip() for c in os.environ.get("QDRANT_COLLECTIONS", "").split(",") if c.strip()] or [QDRANT_COLLECTION]' in SRC
    assert "def _qdrant_search_one(vector, limit, collection):" in SRC
