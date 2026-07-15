"""v18 MeshMonitor-backed pong count tests — radio-free (ast-extraction harness,
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


def _mm():
    fn, _ = _extract("mm_online_count")
    return fn


def _fresh_cache():
    return {"count": None, "ts": 0.0}


def mm_nodes(*last_heards):
    return [{"lastHeard": lh} for lh in last_heards]


NOW = 1_000_000.0
WIN = 7200


def test_unset_url_returns_none_without_fetching():
    calls = []
    fn = _mm()
    assert fn(NOW, "", WIN, _fresh_cache(), 60, lambda u: calls.append(u)) is None
    assert calls == []


def test_counts_only_windowed_nodes():
    fn = _mm()
    payload = mm_nodes(NOW - 10, NOW - WIN + 1, NOW - WIN - 1, NOW - 90000)
    got = fn(NOW, "http://mm:3001", WIN, _fresh_cache(), 60, lambda u: payload)
    assert got == 2


def test_fetch_url_is_the_nodes_endpoint():
    fn = _mm()
    seen = []
    fn(NOW, "http://mm:3001", WIN, _fresh_cache(), 60,
       lambda u: seen.append(u) or mm_nodes(NOW))
    assert seen == ["http://mm:3001/api/nodes"]


def test_dict_payload_with_nodes_key_accepted():
    fn = _mm()
    got = fn(NOW, "http://mm:3001", WIN, _fresh_cache(), 60,
             lambda u: {"nodes": mm_nodes(NOW - 5)})
    assert got == 1


def test_garbage_payloads_return_none():
    fn = _mm()
    for payload in ("nope", {"nodes": "nope"}, [42, None, "x"],
                    [{"lastHeard": True}, {"lastHeard": "12"}]):
        got = fn(NOW, "http://mm:3001", WIN, _fresh_cache(), 60, lambda u: payload)
        assert got is None, payload


def test_all_stale_but_usable_data_returns_zero_not_none():
    # 0 is a real answer ("nobody heard lately"); None means "no data at all".
    fn = _mm()
    got = fn(NOW, "http://mm:3001", WIN, _fresh_cache(), 60,
             lambda u: mm_nodes(NOW - 90000))
    assert got == 0


def test_fetch_failure_returns_none_and_is_cached():
    fn = _mm()
    cache, calls = _fresh_cache(), []

    def boom(u):
        calls.append(u)
        raise RuntimeError("down")

    assert fn(NOW, "http://mm:3001", WIN, cache, 60, boom) is None
    assert fn(NOW + 30, "http://mm:3001", WIN, cache, 60, boom) is None
    assert len(calls) == 1, "failure must be cached for the TTL"
    assert fn(NOW + 61, "http://mm:3001", WIN, cache, 60, boom) is None
    assert len(calls) == 2, "TTL expiry must retry"


def test_success_is_cached_within_ttl():
    fn = _mm()
    cache, calls = _fresh_cache(), []

    def fetch(u):
        calls.append(u)
        return mm_nodes(NOW - 5, NOW - 6)

    assert fn(NOW, "http://mm:3001", WIN, cache, 60, fetch) == 2
    assert fn(NOW + 59, "http://mm:3001", WIN, cache, 60, fetch) == 2
    assert len(calls) == 1


def test_live_online_count_prefers_mm_and_falls_back():
    fn, _ = _extract("live_online_count", {
        "MESHMONITOR_API_URL": "http://mm:3001", "ONLINE_WINDOW_S": WIN,
        "_mm_online_cache": _fresh_cache(), "MM_ONLINE_TTL_S": 60,
        "requests": None,  # never touched: mm_online_count stub below wins
        "mm_online_count": lambda now, url, w, c, t, f: 95,
        "count_online": lambda nodes, now: 29,
    })
    assert fn({}, NOW) == 95

    fn, _ = _extract("live_online_count", {
        "MESHMONITOR_API_URL": "", "ONLINE_WINDOW_S": WIN,
        "_mm_online_cache": _fresh_cache(), "MM_ONLINE_TTL_S": 60,
        "requests": None,
        "mm_online_count": lambda now, url, w, c, t, f: None,
        "count_online": lambda nodes, now: 29,
    })
    assert fn({}, NOW) == 29


def test_health_exposes_mm_cache():
    assert '"mm_online": _mm_online_cache["count"]' in SRC
    assert '"mm_online_ts": _mm_online_cache["ts"] or None' in SRC


def test_banner_is_v18():
    assert 'log("mesh-ai-bridge v18 starting' in SRC
