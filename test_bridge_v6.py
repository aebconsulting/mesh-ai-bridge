#!/usr/bin/env python3
"""v6 unit tests. Run ON AIBOX: MEM_DB=/tmp/v6test.db ~/meshenv/bin/python test_bridge_v6.py"""
import importlib.util, os, queue, socket, sqlite3, sys, time, json, threading, urllib.request, urllib.error

os.environ.setdefault("MEM_DB", "/tmp/v6test.db")
if os.path.exists(os.environ["MEM_DB"]):
    os.remove(os.environ["MEM_DB"])

spec = importlib.util.spec_from_file_location("bridge", os.path.join(os.path.dirname(__file__), "bridge.py"))
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)

def test_wal_mode():
    with bridge.db() as c:
        assert c.execute("PRAGMA journal_mode").fetchone()[0] == "wal", "journal_mode must be wal"
        assert c.execute("PRAGMA busy_timeout").fetchone()[0] == 5000

def test_msg_log_index():
    with bridge.db() as c:
        names = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='index'")]
    assert "idx_msg_log_ts" in names

def test_retention_prunes_old_rows_only():
    old = time.time() - 91 * 86400
    bridge.log_traffic("in", "!aaaaaaaa", "OLD", 0, 0, 0, "ancient")
    with bridge.db() as c:
        c.execute("UPDATE msg_log SET ts=? WHERE text='ancient'", (old,))
    bridge.log_traffic("in", "!bbbbbbbb", "NEW", 0, 0, 0, "fresh")
    # Tripwire: memory.db is also the AI's long-term memory. Old rows in
    # messages/facts must SURVIVE the prune — only msg_log may be deleted.
    bridge.add_msg("!selftest", "user", "old memory row")
    bridge.add_fact("!selftest", "old fact row")
    with bridge.db() as c:
        c.execute("UPDATE messages SET ts=? WHERE content='old memory row'", (old,))
        c.execute("UPDATE facts SET ts=? WHERE content='old fact row'", (old,))
    bridge.prune_msg_log()
    with bridge.db() as c:
        texts = [r[0] for r in c.execute("SELECT text FROM msg_log")]
        n_msg = c.execute("SELECT COUNT(*) FROM messages WHERE content LIKE 'old %'").fetchone()[0]
        n_fact = c.execute("SELECT COUNT(*) FROM facts WHERE content LIKE 'old %'").fetchone()[0]
    assert "ancient" not in texts and "fresh" in texts
    assert n_msg == 1, "prune must NOT touch messages (AI memory)"
    assert n_fact == 1, "prune must NOT touch facts (AI memory)"

class StubIface:
    def __init__(self): self.sent = []
    def sendText(self, text, channelIndex=None, destinationId=None, **kw):
        self.sent.append((text, channelIndex, destinationId))
    def getMyNodeInfo(self): return {"user": {"longName": "STUB"}}
    nodes = {}

def _post(path, body, token=None):
    req = urllib.request.Request("http://127.0.0.1:8700" + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", **({"X-Send-Token": token} if token else {})})
    try:
        r = urllib.request.urlopen(req, timeout=5)
        return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

def test_send_api():
    bridge.SEND_TOKEN = "testtoken123"
    bridge.iface = StubIface()
    bridge.start_send_api()
    time.sleep(0.3)
    assert _post("/api/send", {"text": "hi"})[0] == 401, "no token must be rejected"
    assert _post("/api/send", {"text": "hi"}, "wrong")[0] == 401
    assert _post("/api/send", {"text": "x" * 400}, "testtoken123")[0] == 400, "oversize must be rejected"
    assert _post("/api/send", {"text": "hi", "channel": 5}, "testtoken123")[0] == 400, "disallowed channel"
    assert _post("/api/send", {"text": "hi", "to": "../etc"}, "testtoken123")[0] == 400, "bad destination"
    assert _post("/api/send", {"text": "hi", "to": "!1a2b3c4d\n"}, "testtoken123")[0] == 400, \
        "trailing-newline destination must be rejected (fullmatch regression)"
    code, out = _post("/api/send", {"text": "hello mesh", "channel": 0}, "testtoken123")
    assert code == 200 and out["ok"] is True
    assert bridge.iface.sent == [("hello mesh", 0, None)], "sendText not called correctly"
    with bridge.db() as c:
        row = c.execute("SELECT direction, node_id, text FROM msg_log ORDER BY id DESC LIMIT 1").fetchone()
    assert row == ("out", "dashboard", "hello mesh"), "outbound send must be logged"
    # DM success path — clear the dashboard cooldown (A5's own bucket) left by the broadcast send
    bridge._dash_recent.clear(); bridge._dash_last.clear()
    code, out = _post("/api/send", {"text": "dm test", "to": "!1a2b3c4d"}, "testtoken123")
    assert code == 200 and out["ok"] is True, "DM send must succeed"
    assert bridge.iface.sent[-1] == ("dm test", None, "!1a2b3c4d"), "DM must use destinationId"
    with bridge.db() as c:
        row = c.execute("SELECT direction, node_id, is_dm FROM msg_log ORDER BY id DESC LIMIT 1").fetchone()
    assert row == ("out", "!1a2b3c4d", 1), "DM send must be logged with is_dm=1"
    # radio not connected -> 503 (dashboard_allowed_now runs before the iface check, so clear cooldowns)
    bridge._dash_recent.clear(); bridge._dash_last.clear()
    bridge.iface = None
    assert _post("/api/send", {"text": "hi", "channel": 0}, "testtoken123")[0] == 503, "iface None must 503"
    # radio send raises -> 502 AND is logged with ack_state='failed' (5a delivery-tracking contract)
    bridge._dash_recent.clear(); bridge._dash_last.clear()
    class BoomIface(StubIface):
        def sendText(self, *a, **k): raise RuntimeError("radio boom")
    bridge.iface = BoomIface()
    with bridge.db() as c:
        n_before = c.execute("SELECT COUNT(*) FROM msg_log").fetchone()[0]
    assert _post("/api/send", {"text": "boom", "channel": 0}, "testtoken123")[0] == 502, "radio failure must 502"
    with bridge.db() as c:
        n_after = c.execute("SELECT COUNT(*) FROM msg_log").fetchone()[0]
        row = c.execute("SELECT ack_state, text FROM msg_log ORDER BY id DESC LIMIT 1").fetchone()
    assert n_after == n_before + 1, "failed send must be logged as exactly ONE new row (5a contract)"
    assert row == ("failed", "boom"), "failed send row must carry ack_state='failed' (powers the dashboard X glyph)"
    bridge.iface = StubIface()
    hc = urllib.request.urlopen("http://127.0.0.1:8700/api/health", timeout=5)
    body = json.loads(hc.read())
    assert hc.status == 200 and body["node"] == "STUB"
    assert body["api"] is True, "send-API thread must report alive once started (A1)"

class BrokenNodeInfoIface(StubIface):
    def getMyNodeInfo(self):
        raise RuntimeError("radio comms broken")

def test_health_reports_ok_false_on_node_info_failure():
    """A2: /api/health must fold a getMyNodeInfo() failure into ok, not just iface presence."""
    bridge.iface = BrokenNodeInfoIface()
    hc = urllib.request.urlopen("http://127.0.0.1:8700/api/health", timeout=5)
    body = json.loads(hc.read())
    assert hc.status == 200
    assert body["ok"] is False, "ok must go False when getMyNodeInfo raises, not stay True"
    assert body["node"] is None
    assert "api" in body, "health must expose send-API thread liveness (A1)"
    bridge.iface = StubIface()

def _raw_post(path, extra_headers, body):
    """Craft a raw HTTP POST so we can send a Content-Length header the stdlib
    client would never let us set to a bogus value (A4 negative/oversize test)."""
    s = socket.create_connection(("127.0.0.1", 8700), timeout=5)
    try:
        lines = ["POST {} HTTP/1.1".format(path), "Host: 127.0.0.1"]
        for k, v in extra_headers.items():
            lines.append("{}: {}".format(k, v))
        lines.append("Connection: close")
        s.sendall(("\r\n".join(lines) + "\r\n\r\n").encode() + body)
        resp = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            resp += chunk
    finally:
        s.close()
    return int(resp.split(b"\r\n", 1)[0].decode().split()[1])

def test_content_length_guards():
    """A4: negative and oversize Content-Length must 400, not block forever or read unbounded."""
    bridge.iface = StubIface()
    body = b'{"text": "hi", "channel": 0}'
    neg_headers = {"Content-Type": "application/json", "X-Send-Token": "testtoken123", "Content-Length": "-1"}
    assert _raw_post("/api/send", neg_headers, body) == 400, "negative Content-Length must 400"
    big_headers = {"Content-Type": "application/json", "X-Send-Token": "testtoken123", "Content-Length": "999999"}
    assert _raw_post("/api/send", big_headers, body) == 400, "oversize Content-Length must 400"

def test_dashboard_rate_bucket_independent_of_mesh():
    """A5: a saturated dashboard send bucket must never starve the mesh @ai bucket."""
    bridge._dash_recent.clear(); bridge._dash_last.clear()
    bridge.recent.clear(); bridge.last_by_node.clear()
    now = time.time()
    for _ in range(bridge.SEND_PER_MIN):
        bridge._dash_recent.append(now)
    assert bridge.dashboard_allowed_now("probe") is False, "dashboard bucket must be exhausted"
    assert bridge.allowed_now("!realmeshnode") is True, \
        "mesh @ai bucket must be unaffected by a full dashboard bucket"
    bridge._dash_recent.clear(); bridge._dash_last.clear()
    bridge.recent.clear(); bridge.last_by_node.clear()

def test_non_object_json_body():
    """A8: non-object JSON bodies ([]/string/number/bool/null) must get a clean 400, not crash."""
    bridge.iface = StubIface()
    bridge._dash_recent.clear(); bridge._dash_last.clear()
    for payload in ([], "just a string", 5, True, None):
        code, out = _post("/api/send", payload, "testtoken123")
        assert code == 400, "non-object JSON body {!r} must 400, got {}".format(payload, code)
        assert "error" in out

# ---------- 2b-i (qdrant rewrite): semantic library retrieval ----------
class FakeResponse:
    """Stand-in for requests.Response - covers both the .text (Kiwix GET) and
    .json()/.raise_for_status() (embed/qdrant POST) call shapes used by bridge.py."""
    def __init__(self, text=None, json_data=None, status=200):
        self.text = text
        self._json = json_data
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception("http {}".format(self.status_code))

    def json(self):
        return self._json


def _save_requests():
    return bridge.requests.get, bridge.requests.post


def _restore_requests(saved):
    bridge.requests.get, bridge.requests.post = saved


# F7: save/restore the module-level library config any test mutates, so one test's
# tweaks (LIBRARY_BOOKS, budget, floor, _book_ids) never leak into the next.
# getattr with a default keeps this usable against a PRE-FIX bridge.py that has no
# MIN_CHUNK_CHARS yet (clean RED, no setup AttributeError).
_LIB_STATE_KEYS = ("_book_ids", "LIBRARY_BOOKS", "LIBRARY_MAX_BOOKS",
                   "LIBRARY_CONTEXT_CHARS", "MIN_CHUNK_CHARS", "QDRANT_MIN_SCORE")


def _save_lib_state():
    return {k: getattr(bridge, k, None) for k in _LIB_STATE_KEYS}


def _restore_lib_state(s):
    for k, v in s.items():
        if v is not None:
            setattr(bridge, k, v)


def _long(marker):
    """A chunk text comfortably above MIN_CHUNK_CHARS (default 120) that embeds `marker`."""
    return marker + " " + ("clinical padding detail " * 8)   # ~200 chars


def test_embed_query_ok():
    saved = _save_requests()
    try:
        bridge.requests.post = lambda *a, **k: FakeResponse(json_data={"embedding": [0.1] * 768})
        assert bridge.embed_query("hello") == [0.1] * 768
    finally:
        _restore_requests(saved)


def test_embed_query_fallback_on_error():
    saved = _save_requests()
    try:
        def boom(*a, **k):
            raise RuntimeError("embed down")
        bridge.requests.post = boom
        assert bridge.embed_query("hello") is None
    finally:
        _restore_requests(saved)


def test_qdrant_search_validates_hits():
    """A malformed hit (missing score, non-dict payload, bool score, non-dict
    element) must never crash the caller - only structurally-valid hits survive."""
    saved = _save_requests()
    try:
        bridge.requests.post = lambda *a, **k: FakeResponse(json_data={"result": [
            {"score": 0.8, "payload": {"text": "good"}},
            {"payload": {"text": "missing score"}},
            {"score": 0.5, "payload": "not a dict"},
            {"score": True, "payload": {"text": "bool score"}},   # F4: bool must NOT pass as a score
            "not even a dict",                                     # B2: non-dict element skipped, no crash
        ]})
        out = bridge.qdrant_search([0.1] * 768, 5)
    finally:
        _restore_requests(saved)
    assert out == [{"score": 0.8, "payload": {"text": "good"}}], \
        "only the structurally-valid hit may survive validation"


def test_qdrant_search_malformed_result_not_list():
    """B2: a 200 whose `result` is not a list is a MALFORMED RESPONSE -> return None,
    logged as malformed, NOT conflated with a transport 'qdrant unavailable'."""
    saved = _save_requests()
    logs = []
    orig_log = bridge.log
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge.requests.post = lambda *a, **k: FakeResponse(json_data={"result": "oops not a list"})
        out = bridge.qdrant_search([0.1] * 768, 5)
    finally:
        _restore_requests(saved)
        bridge.log = orig_log
    assert out is None, "a non-list result must return None (safe fallback)"
    assert any("malformed response (result not a list)" in l for l in logs), \
        "malformed shape must be logged distinctly"
    assert not any("qdrant unavailable" in l for l in logs), \
        "a 200 with a bad shape must NOT be logged as a transport failure (wrong category)"


def test_qdrant_search_drops_nonstr_text_and_counts():
    """B1 belt + B3: a hit whose text is a non-str (int) is dropped, and the drop
    is counted+logged so an operator can tell it from a genuine empty result."""
    saved = _save_requests()
    logs = []
    orig_log = bridge.log
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge.requests.post = lambda *a, **k: FakeResponse(json_data={"result": [
            {"score": 0.9, "payload": {"text": "good text"}},
            {"score": 0.8, "payload": {"text": 123}},   # non-str text -> dropped by the belt
        ]})
        out = bridge.qdrant_search([0.1] * 768, 5)
    finally:
        _restore_requests(saved)
        bridge.log = orig_log
    assert out == [{"score": 0.9, "payload": {"text": "good text"}}], \
        "the non-str-text hit must be dropped by qdrant_search's belt"
    assert any("dropped 1 malformed hit(s) of 2" in l for l in logs), \
        "dropped count must be logged (B3)"


def test_qdrant_search_fallback_on_error():
    saved = _save_requests()
    try:
        def boom(*a, **k):
            raise RuntimeError("qdrant down")
        bridge.requests.post = boom
        assert bridge.qdrant_search([0.1] * 768, 5) is None
    finally:
        _restore_requests(saved)


def test_library_context_injects_top_chunk():
    """HAPPY PATH: embed ok, qdrant returns 2 hits (0.80, 0.72) above the floor.
    Both chunk texts must be injected, the article_title must appear, and the
    order must follow the qdrant score-desc order."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    try:
        def fake_post(url, json=None, timeout=None):
            if "embeddings" in url:
                return FakeResponse(json_data={"embedding": [0.2] * 768})
            return FakeResponse(json_data={"result": [
                {"score": 0.80, "payload": {"text": _long("CHUNK_ONE"),
                                            "article_title": "Article One", "section_title": "Sec A"}},
                {"score": 0.72, "payload": {"text": _long("CHUNK_TWO"),
                                            "article_title": "Article Two", "section_title": "Sec B"}},
            ]})
        bridge.requests.post = fake_post
        ctx = bridge.library_context("medical query")
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
    assert "CHUNK_ONE" in ctx and "CHUNK_TWO" in ctx, "both chunk texts must be injected"
    assert "Article One" in ctx, "article_title must be cited"
    assert ctx.index("CHUNK_ONE") < ctx.index("CHUNK_TWO"), "must be injected in score-desc order"


def test_library_context_floor_blocks_weak():
    """Medical-safety gate: a top score below QDRANT_MIN_SCORE (0.65) means
    nothing in the library is on-topic -> inject nothing, log distinctly."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    logs = []
    orig_log = bridge.log
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))

        def fake_post(url, json=None, timeout=None):
            if "embeddings" in url:
                return FakeResponse(json_data={"embedding": [0.1] * 768})
            return FakeResponse(json_data={"result": [
                {"score": 0.50, "payload": {"text": "weak text", "article_title": "Weak"}}
            ]})
        bridge.requests.post = fake_post
        ctx = bridge.library_context("totally unrelated query")
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
        bridge.log = orig_log
    assert ctx == "", "below-floor top score must mean no context injected"
    assert any("top score" in l and "floor" in l for l in logs), \
        "floor case must log its own distinct message"


def test_library_context_mid_list_floor_break():
    """Scores [0.80, 0.72, 0.40]: only the two above the 0.65 floor are injected;
    the below-floor third stops the loop (hits are score-desc, break at floor)."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    try:
        def fake_post(url, json=None, timeout=None):
            if "embeddings" in url:
                return FakeResponse(json_data={"embedding": [0.1] * 768})
            return FakeResponse(json_data={"result": [
                {"score": 0.80, "payload": {"text": _long("HIT_A"), "article_title": "A"}},
                {"score": 0.72, "payload": {"text": _long("HIT_B"), "article_title": "B"}},
                {"score": 0.40, "payload": {"text": _long("HIT_C"), "article_title": "C"}},
            ]})
        bridge.requests.post = fake_post
        ctx = bridge.library_context("q")
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
    assert "HIT_A" in ctx and "HIT_B" in ctx, "the two above-floor hits must be injected"
    assert "HIT_C" not in ctx, "a below-floor hit must not be injected (loop breaks at the floor)"


def test_library_context_budget_cap_stops():
    """Two above-floor hits whose combined text can't fit LIBRARY_CONTEXT_CHARS:
    the loop stops at budget and injects only what fits (the first)."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    try:
        bridge.LIBRARY_CONTEXT_CHARS = 200
        first = "FIRST_HIT " + ("x" * 190)     # 200 chars: >= MIN_CHUNK_CHARS, fits alone
        second = "SECOND_HIT " + ("y" * 190)

        def fake_post(url, json=None, timeout=None):
            if "embeddings" in url:
                return FakeResponse(json_data={"embedding": [0.1] * 768})
            return FakeResponse(json_data={"result": [
                {"score": 0.80, "payload": {"text": first, "article_title": "A"}},
                {"score": 0.72, "payload": {"text": second, "article_title": "B"}},
            ]})
        bridge.requests.post = fake_post
        ctx = bridge.library_context("q")
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
    assert "FIRST_HIT" in ctx, "the first hit must be injected"
    assert "SECOND_HIT" not in ctx, "the second hit must be dropped once the budget is full"


def test_library_context_skips_malformed_hit_end_to_end():
    """B1 END-TO-END (the bug both reviewers blocked on): a hit with a non-str text
    (123) driven through the full library_context() path must NOT raise (which
    handle_query would swallow as 'LLM unreachable', silently dropping the medical
    query). It is skipped; with no other usable hit, library_context returns ''."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    try:
        def fake_post(url, json=None, timeout=None):
            if "embeddings" in url:
                return FakeResponse(json_data={"embedding": [0.1] * 768})
            return FakeResponse(json_data={"result": [
                {"score": 0.80, "payload": {"text": 123, "article_title": "Bad"}},
            ]})
        bridge.requests.post = fake_post
        ctx = bridge.library_context("medical query")   # must NOT raise
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
    assert ctx == "", "a non-str-text hit must be skipped, yielding '' (never a crash)"


class _BoomStr:
    """A payload value whose str() blows up - to exercise the assembly-loop
    suspenders even if such a value ever slips past qdrant_search's belt."""
    def __str__(self):
        raise RuntimeError("boom str")


def test_library_context_assembly_survives_unstringable_payload():
    """B1 suspenders (defense in depth): even if a hit reaches the assembly loop
    with a payload field whose str() raises, the per-hit try/except must skip it and
    log 'skipping malformed hit', never propagate. Stub qdrant_search directly to
    bypass the belt."""
    saved_qs = bridge.qdrant_search
    saved_eq = bridge.embed_query
    libsaved = _save_lib_state()
    logs = []
    orig_log = bridge.log
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge.embed_query = lambda q: [0.1] * 768
        bridge.qdrant_search = lambda v, k: [{"score": 0.9, "payload": {"text": _BoomStr()}}]
        ctx = bridge.library_context("q")   # must NOT raise
    finally:
        bridge.qdrant_search = saved_qs
        bridge.embed_query = saved_eq
        _restore_lib_state(libsaved)
        bridge.log = orig_log
    assert ctx == "", "an unstringable payload must be skipped, not crash out of library_context"
    assert any("skipping malformed hit" in l for l in logs), \
        "the assembly-loop suspenders must log the skip"


def test_library_context_embed_down_uses_kiwix():
    """Embed unreachable -> degrade to the simple Kiwix first-wins fallback
    (NOT the old cross-encoder pipeline)."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    logs = []
    orig_log = bridge.log
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge._book_ids = {"BookOnly": "idOnly"}
        bridge.LIBRARY_BOOKS = ["BookOnly"]

        def fake_post(*a, **k):
            raise RuntimeError("embed down")

        def fake_get(url, params=None, timeout=None):
            if url == bridge.KIWIX_URL + "/search":
                xml = ("<item><title>Only Title</title><link>/only.html</link>"
                       "<description>only snippet</description></item>")
                return FakeResponse(text=xml)
            if url == bridge.KIWIX_URL + "/only.html":
                return FakeResponse(text="<p>" + ("Only article body padding text here. " * 8) + "</p>")
            raise AssertionError("unexpected GET " + url)

        bridge.requests.post = fake_post
        bridge.requests.get = fake_get
        ctx = bridge.library_context("some query")
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
        bridge.log = orig_log
    assert "Only article body" in ctx, "kiwix fallback must still inject the only candidate's article"
    assert "BookOnly" in ctx and "Only Title" in ctx
    assert any("embed down -> kiwix fallback" in l for l in logs), \
        "embed-down must be logged distinctly"


def test_library_context_qdrant_down_uses_kiwix():
    """Embed ok but qdrant unreachable -> degrade to the Kiwix fallback, logged
    distinctly from the embed-down case."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    logs = []
    orig_log = bridge.log
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge._book_ids = {"BookOnly": "idOnly"}
        bridge.LIBRARY_BOOKS = ["BookOnly"]

        def fake_post(url, json=None, timeout=None):
            if "embeddings" in url:
                return FakeResponse(json_data={"embedding": [0.1] * 768})
            raise RuntimeError("qdrant down")

        def fake_get(url, params=None, timeout=None):
            if url == bridge.KIWIX_URL + "/search":
                xml = ("<item><title>Only Title</title><link>/only.html</link>"
                       "<description>only snippet</description></item>")
                return FakeResponse(text=xml)
            if url == bridge.KIWIX_URL + "/only.html":
                return FakeResponse(text="<p>" + ("Only article body padding text here. " * 8) + "</p>")
            raise AssertionError("unexpected GET " + url)

        bridge.requests.post = fake_post
        bridge.requests.get = fake_get
        ctx = bridge.library_context("some query")
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
        bridge.log = orig_log
    assert "Only article body" in ctx, "kiwix fallback must still inject the only candidate's article"
    assert any("qdrant down -> kiwix fallback" in l for l in logs), \
        "qdrant-down must be logged distinctly (not conflated with embed-down)"


def test_library_context_zero_hits_empty():
    """qdrant reachable but returns zero hits -> empty context, logged distinctly
    from both the floor case and the down-fallback cases."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    logs = []
    orig_log = bridge.log
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))

        def fake_post(url, json=None, timeout=None):
            if "embeddings" in url:
                return FakeResponse(json_data={"embedding": [0.1] * 768})
            return FakeResponse(json_data={"result": []})
        bridge.requests.post = fake_post
        ctx = bridge.library_context("some query")
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
        bridge.log = orig_log
    assert ctx == "", "zero hits must mean no context injected"
    assert any("zero hits" in l for l in logs), "zero-hits case must log its own distinct message"


def test_kiwix_fallback_empty_book_ids():
    """B4: _kiwix_fallback with no book catalog returns '' (guarded)."""
    libsaved = _save_lib_state()
    logs = []
    orig_log = bridge.log
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge._book_ids = {}
        assert bridge._kiwix_fallback("q") == "", "no catalog must yield '' (no HTTP)"
    finally:
        _restore_lib_state(libsaved)
        bridge.log = orig_log
    assert any("no book catalog" in l for l in logs), "empty catalog must be logged distinctly"


def test_kiwix_fallback_second_book_failover():
    """B4: the first book's Kiwix search raises; the per-candidate try/except must
    move on and return the SECOND book's content."""
    saved = _save_requests()
    libsaved = _save_lib_state()
    try:
        bridge._book_ids = {"Book1": "id1", "Book2": "id2"}
        bridge.LIBRARY_BOOKS = ["Book1", "Book2"]
        bridge.LIBRARY_MAX_BOOKS = 3

        def fake_get(url, params=None, timeout=None):
            if url == bridge.KIWIX_URL + "/search":
                if params["books.id"] == "id1":
                    raise RuntimeError("book1 search down")
                xml = ("<item><title>Second Title</title><link>/two.html</link>"
                       "<description>snip</description></item>")
                return FakeResponse(text=xml)
            if url == bridge.KIWIX_URL + "/two.html":
                return FakeResponse(text="<p>" + ("Second book body content padding text. " * 8) + "</p>")
            raise AssertionError("unexpected GET " + url)

        bridge.requests.get = fake_get
        ctx = bridge._kiwix_fallback("q")
    finally:
        _restore_requests(saved)
        _restore_lib_state(libsaved)
    assert "Second book body" in ctx, "must fail over to the second book when the first errors"
    assert "Book2" in ctx and "Second Title" in ctx


# ---------- 2b-ii: bounded no-drop query queue + packet dedup + queue health ----------
def test_dedup_suppresses_retransmit():
    """A retransmitted mesh packet id must be flagged duplicate within DEDUP_TTL_S;
    a missing id (None) must never be treated as a duplicate."""
    bridge._seen_ids.clear()
    try:
        assert bridge._is_duplicate(123) is False, "first sighting must not be a duplicate"
        assert bridge._is_duplicate(123) is True, "retransmit within TTL must be flagged duplicate"
        assert bridge._is_duplicate(None) is False, "None id must never be a duplicate"
        assert bridge._is_duplicate(None) is False, "None id must never be a duplicate (repeat)"
    finally:
        bridge._seen_ids.clear()


def test_worker_processes_queued_item():
    """A single item put on the queue must be drained and passed to handle_query by
    the worker thread - proves the queue+worker actually wires up to handle_query."""
    orig_hq = bridge.handle_query
    orig_wq = bridge.work_q
    bridge.work_q = queue.Queue(maxsize=10)
    calls = []
    evt = threading.Event()

    def fake_handle_query(sender, ch, query, send):
        calls.append((sender, ch, query, send))
        evt.set()

    bridge.handle_query = fake_handle_query
    send = lambda c: None
    try:
        bridge.work_q.put(("!node1", 0, "hello", send))
        bridge.start_worker()
        assert evt.wait(timeout=5), "worker did not process the queued item in time"
        assert calls == [("!node1", 0, "hello", send)], "handle_query must receive the exact queued args"
    finally:
        bridge.handle_query = orig_hq
        bridge.work_q = orig_wq


def test_worker_survives_handler_exception():
    """handle_query raising on one item must NOT kill the worker - the next queued
    item must still be processed (liveness lesson from the send-API thread)."""
    orig_hq = bridge.handle_query
    orig_wq = bridge.work_q
    bridge.work_q = queue.Queue(maxsize=10)
    calls = []
    evt = threading.Event()

    def fake_handle_query(sender, ch, query, send):
        if query == "boom":
            raise RuntimeError("handler blew up")
        calls.append((sender, ch, query, send))
        evt.set()

    bridge.handle_query = fake_handle_query
    logs = []
    orig_log = bridge.log
    send = lambda c: None
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge.work_q.put(("!node1", 0, "boom", send))
        bridge.work_q.put(("!node1", 0, "second", send))
        bridge.start_worker()
        assert evt.wait(timeout=5), "worker must still process the item AFTER the exception"
        assert calls == [("!node1", 0, "second", send)], "the second item must reach handle_query"
        assert any("worker error" in l for l in logs), "the exception must be logged, not silently swallowed"
    finally:
        bridge.handle_query = orig_hq
        bridge.work_q = orig_wq
        bridge.log = orig_log


def test_health_includes_queue_fields():
    """C9: /api/health must expose queue_depth (int), worker (bool) liveness, and the
    worker_idle_s staleness metric — and queue_depth must reflect the ACTUAL number of
    queued items, not merely be an int."""
    bridge.iface = StubIface()
    orig_wq = bridge.work_q
    bridge.work_q = queue.Queue(maxsize=100)   # fresh queue, no worker draining it
    try:
        for i in range(3):
            bridge.work_q.put(("!n", 0, "q{}".format(i), lambda c: None))
        if not bridge.send_api_alive:
            bridge.SEND_TOKEN = bridge.SEND_TOKEN or "testtoken123"
            bridge.start_send_api()
            time.sleep(0.3)
        hc = urllib.request.urlopen("http://127.0.0.1:8700/api/health", timeout=5)
        body = json.loads(hc.read())
        assert hc.status == 200
        assert isinstance(body["queue_depth"], int) and body["queue_depth"] == 3, \
            "queue_depth must reflect the actual number of queued items (3), got {}".format(body.get("queue_depth"))
        assert "worker" in body and isinstance(body["worker"], bool), "worker liveness must be a bool"
        assert "worker_idle_s" in body and isinstance(body["worker_idle_s"], (int, float)), \
            "health must expose the worker_idle_s staleness metric (F7)"
    finally:
        bridge.work_q = orig_wq


def test_dedup_ttl_eviction():
    """C3: an id older than DEDUP_TTL_S must be evicted from _seen_ids and a post-TTL
    resubmission of that same id must be treated as NEW (returns False), not a duplicate."""
    bridge._seen_ids.clear()
    orig_ttl = bridge.DEDUP_TTL_S
    try:
        bridge.DEDUP_TTL_S = 100
        assert bridge._is_duplicate(42) is False, "first sighting is new"
        assert bridge._is_duplicate(42) is True, "immediate resubmit is a duplicate"
        # Backdate the stored timestamp beyond the TTL to simulate the passage of time.
        bridge._seen_ids[42] = time.time() - 200
        assert bridge._is_duplicate(42) is False, \
            "a stale id past DEDUP_TTL_S must be evicted and treated as new"
    finally:
        bridge.DEDUP_TTL_S = orig_ttl
        bridge._seen_ids.clear()


def test_on_receive_enqueues_and_dedupes_before_ratelimit():
    """C4 + C1 END-TO-END: drive on_receive() with a real-shaped @ai packet.
    First call must enqueue. A SECOND call with the SAME packet id must be dropped by
    DEDUP *before* the rate-limit (C1 ordering) — proven by: it did NOT enqueue again,
    it did NOT consume a rate slot, the dedup-drop log fired, and the 'rate-limited'
    log did NOT (dedup short-circuited before allowed_now was ever consulted)."""
    orig_wq = bridge.work_q
    orig_my_num = bridge.my_num
    orig_log = bridge.log
    bridge.work_q = queue.Queue(maxsize=100)
    bridge._seen_ids.clear()
    bridge.recent.clear(); bridge.last_by_node.clear()
    bridge.my_num = 999                        # so the @ai channel message isn't seen as "from me"/a DM
    logs = []
    iface = StubIface()
    packet = {"decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "@ai status?"},
              "from": 111, "fromId": "!nodeX", "to": 0, "channel": 0, "id": 777}
    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge.on_receive(packet, iface)
        assert bridge.work_q.qsize() == 1, "a fresh @ai query must be enqueued"
        assert len(bridge.recent) == 1, "the first (non-dup) query must consume exactly one rate slot"
        rate_slots_after_first = len(bridge.recent)
        # Retransmit: identical packet id.
        bridge.on_receive(packet, iface)
        assert bridge.work_q.qsize() == 1, "a retransmit (same pkt id) must NOT be enqueued a second time"
        assert len(bridge.recent) == rate_slots_after_first, \
            "a deduped retransmit must NOT consume a rate slot (dedup runs before rate-limit)"
        assert any("dropping duplicate retransmit from !nodeX (pkt_id=777" in l for l in logs), \
            "the dedup drop must be logged with pkt_id evidence (F5)"
        assert not any("rate-limited" in l for l in logs), \
            "the retransmit must be stopped by DEDUP, not the rate-limit — 'rate-limited' must never fire"
        assert sum(1 for l in logs if l.startswith("query from")) == 1, \
            "'query from' (post-rate-limit) must fire once (the real query), never for the dup"
    finally:
        bridge.work_q = orig_wq
        bridge.my_num = orig_my_num
        bridge.log = orig_log
        bridge._seen_ids.clear()
        bridge.recent.clear(); bridge.last_by_node.clear()


def test_worker_liveness_flips_false_on_death():
    """B1: if the worker loop EVER exits — even on a non-Exception throwable that the
    inner per-stage handlers don't catch — worker_alive must flip False and the death
    must be logged CRITICAL, so /api/health surfaces a wedged queue instead of lying.
    We force a SystemExit (a BaseException, NOT an Exception) out of work_q.get()."""
    orig_wq = bridge.work_q
    orig_alive = bridge.worker_alive
    orig_log = bridge.log
    logs = []

    class BoomQueue:
        def get(self):
            raise SystemExit("simulated fatal throwable")

    try:
        bridge.log = lambda *a: logs.append(" ".join(str(x) for x in a))
        bridge.work_q = BoomQueue()
        bridge.worker_alive = False
        t = threading.Thread(target=bridge._worker, daemon=True)
        t.start()
        t.join(timeout=5)
        assert not t.is_alive(), "the worker thread must exit after the fatal throwable, not hang"
        assert bridge.worker_alive is False, \
            "worker_alive must flip False when the worker loop exits (B1), not stay True"
        assert any("query worker thread died" in l for l in logs), \
            "the worker death must be logged CRITICAL, not silent"
    finally:
        bridge.work_q = orig_wq
        bridge.worker_alive = orig_alive
        bridge.log = orig_log


def test_enqueue_does_not_drop():
    """THE WHOLE POINT: with a small bounded queue, N enqueues via the SAME blocking
    put path used by on_receive must ALL be processed - none dropped. The put()
    call blocks (holds) when the queue is momentarily full instead of shedding."""
    orig_hq = bridge.handle_query
    orig_wq = bridge.work_q
    bridge.work_q = queue.Queue(maxsize=2)   # deliberately tiny so puts actually block
    N = 10
    processed = []
    proc_lock = threading.Lock()
    done_evt = threading.Event()

    def fake_handle_query(sender, ch, query, send):
        time.sleep(0.02)   # simulate real work so the small queue actually fills and blocks producers
        with proc_lock:
            processed.append(query)
            if len(processed) == N:
                done_evt.set()

    bridge.handle_query = fake_handle_query
    send = lambda c: None
    try:
        bridge.start_worker()
        for i in range(N):
            # Same enqueue path as on_receive: a BLOCKING put - holds if full, never drops.
            bridge.work_q.put(("!node1", 0, "q{}".format(i), send))
        assert done_evt.wait(timeout=10), "not all items were processed in time"
        assert len(processed) == N, "no-drop: every enqueued item must be processed exactly once"
        assert sorted(processed) == sorted("q{}".format(i) for i in range(N)), \
            "every distinct item must have made it through, none dropped"
    finally:
        bridge.handle_query = orig_hq
        bridge.work_q = orig_wq


if __name__ == "__main__":
    for f in [test_wal_mode, test_msg_log_index, test_retention_prunes_old_rows_only, test_send_api,
              test_health_reports_ok_false_on_node_info_failure, test_content_length_guards,
              test_dashboard_rate_bucket_independent_of_mesh, test_non_object_json_body,
              test_embed_query_ok, test_embed_query_fallback_on_error,
              test_qdrant_search_validates_hits, test_qdrant_search_malformed_result_not_list,
              test_qdrant_search_drops_nonstr_text_and_counts, test_qdrant_search_fallback_on_error,
              test_library_context_injects_top_chunk, test_library_context_floor_blocks_weak,
              test_library_context_mid_list_floor_break, test_library_context_budget_cap_stops,
              test_library_context_skips_malformed_hit_end_to_end,
              test_library_context_assembly_survives_unstringable_payload,
              test_library_context_embed_down_uses_kiwix, test_library_context_qdrant_down_uses_kiwix,
              test_library_context_zero_hits_empty,
              test_kiwix_fallback_empty_book_ids, test_kiwix_fallback_second_book_failover,
              test_dedup_suppresses_retransmit, test_worker_processes_queued_item,
              test_worker_survives_handler_exception, test_health_includes_queue_fields,
              test_enqueue_does_not_drop, test_dedup_ttl_eviction,
              test_on_receive_enqueues_and_dedupes_before_ratelimit,
              test_worker_liveness_flips_false_on_death]:
        f(); print("PASS", f.__name__)
    print("ALL v6 TESTS PASSED")
