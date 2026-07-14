"""v16 reranker tests — radio-free (ast-extraction harness, same pattern as
test_bridge_v13.py: bridge.py is never imported)."""
import ast, io, os, time

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


def _rr(response=None, exc=None, floor=0.001):
    """rerank_hits with a stubbed requests.post. Returns (fn, calls)."""
    calls = []

    class Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return response

    def post(url, json=None, timeout=None):
        calls.append({"url": url, "json": json, "timeout": timeout})
        if exc:
            raise exc
        return Resp()

    fn, _ = _extract("rerank_hits", {
        "requests": type("R", (), {"post": staticmethod(post)}),
        "RERANK_URL": "http://rr:8091",
        "RERANK_TIMEOUT_S": 1.0,
        "RERANK_MIN_SCORE": floor,
        "log": lambda *a, **k: None,
        "time": time,
    })
    return fn, calls


def _hits():
    return [
        {"score": 0.71, "payload": {"text": "ROS nodes can be listed with rosnode list"}},
        {"score": 0.70, "payload": {"text": "Cool the burn under running water for 10 minutes"}},
        {"score": 0.69, "payload": {"text": "TRON is a blockchain platform"}},
    ]


def test_reorders_by_rerank_score_and_drops_noise():
    fn, _ = _rr([{"index": 1, "score": 0.9}, {"index": 0, "score": 0.4},
                 {"index": 2, "score": 0.00002}])
    hits = _hits()
    out = fn("how do I treat a burn", hits)
    assert [h["payload"]["text"][:4] for h in out] == ["Cool", "ROS "]
    assert out[0]["rerank"] == 0.9 and out[1]["rerank"] == 0.4


def test_unsorted_response_is_sorted_desc():
    fn, _ = _rr([{"index": 0, "score": 0.1}, {"index": 1, "score": 0.8},
                 {"index": 2, "score": 0.5}])
    out = fn("q", _hits())
    assert [h["rerank"] for h in out] == [0.8, 0.5, 0.1]


def test_all_noise_returns_empty_list():
    fn, _ = _rr([{"index": 0, "score": 0.00002}, {"index": 1, "score": 0.00001},
                 {"index": 2, "score": 0.00003}])
    assert fn("which nodes are online", _hits()) == []


def test_service_down_degrades_to_input_order():
    fn, _ = _rr(exc=OSError("connection refused"))
    hits = _hits()
    assert fn("q", hits) == hits


def test_malformed_responses_degrade_to_input_order():
    hits = _hits()
    bad = [
        "not a list",
        [],                                                            # empty ranking for non-empty input
        [{"index": 0, "score": 0.9}, {"index": 1, "score": 0.5},
         {"index": 2, "score": 0.4}, {"index": 0, "score": 0.3}],      # longer than input
        [{"index": 0, "score": 0.9}, {"index": 0, "score": 0.5},
         {"index": 2, "score": 0.4}],                                  # duplicate index
        [{"index": 0, "score": 0.9}, {"index": 5, "score": 0.5},
         {"index": 2, "score": 0.4}],                                  # out of range
        [{"index": 0, "score": 0.9}, {"index": True, "score": 0.5},
         {"index": 2, "score": 0.4}],                                  # bool index
        [{"index": 0, "score": True}, {"index": 1, "score": 0.5},
         {"index": 2, "score": 0.4}],                                  # bool score
        [{"index": 0, "score": 0.9}, {"score": 0.5}, {"index": 2, "score": 0.4}],  # missing key
    ]
    for resp in bad:
        fn, _ = _rr(resp)
        assert fn("q", hits) == hits, resp


def test_partial_ranking_ranks_what_came_back():
    """A TEI config that returns only its top-N (rather than one entry per text)
    must still rerank those N — not silently no-op the whole feature."""
    fn, _ = _rr([{"index": 1, "score": 0.9}, {"index": 0, "score": 0.4}])
    out = fn("how do I treat a burn", _hits())
    assert [h["payload"]["text"][:4] for h in out] == ["Cool", "ROS "]
    assert all("rerank" in h for h in out)


def test_empty_hits_short_circuits_without_calling_service():
    fn, calls = _rr([])
    assert fn("q", []) == []
    assert calls == []


def test_posts_query_texts_and_truncate():
    fn, calls = _rr([{"index": 0, "score": 0.9}, {"index": 1, "score": 0.8},
                     {"index": 2, "score": 0.7}])
    fn("treat a burn", _hits())
    assert len(calls) == 1
    body = calls[0]["json"]
    assert body["query"] == "treat a burn"
    assert body["texts"][1].startswith("Cool the burn")
    assert body["truncate"] is True
    assert calls[0]["url"] == "http://rr:8091/rerank"
    assert calls[0]["timeout"] == 1.0


def test_wired_into_library_context():
    # rerank runs behind the flag, on floor-passing candidates, after the top-score gate
    assert "if RERANK_ENABLED:" in SRC
    assert "hits = rerank_hits(query, kept)" in SRC
    i_floor = SRC.index('if hits[0]["score"] < QDRANT_MIN_SCORE:')
    i_rerank = SRC.index("hits = rerank_hits(query, kept)")
    i_assembly = SRC.index("parts, total, winners, too_short")
    assert i_floor < i_rerank < i_assembly
    # the v1 "not wired" warning is gone with the wiring
    assert "not wired" not in SRC
    assert "_rerank_warned" not in SRC
