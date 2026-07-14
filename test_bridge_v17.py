"""v17 traceroute tests — radio-free (ast-extraction harness, same pattern as
test_bridge_v14.py: bridge.py is never imported)."""
import ast, contextlib, io, json as _json, math, os, sqlite3, threading, time

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

def _pt():
    fn, _ = _extract("parse_traceroute")
    return fn

# REAL packet captured by the Task 1 gate probe (2026-07-14, RZRM direct, trimmed).
# NOTE the live shape: the traceroute dict carries an extra "raw" key (protobuf text
# repr) — the parser must ignore unknown keys. A DIRECT hit omits route/routeBack.
_RESP_FIXTURE = {
    "from": 488548270,          # !1d1ea7ae RZRM
    "to": 932925094,            # !379b4ea6 RZRB (base)
    "hopStart": 2,
    "rxSnr": 10.5,
    "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 2744067508, "bitfield": 0,
                "traceroute": {"snrTowards": [41], "snrBack": [42],
                               "raw": "snr_towards: 41\nsnr_back: 42\n"}},
}

def test_parses_real_direct_response_fixture():
    fn = _pt()
    req, r = fn(_RESP_FIXTURE)
    assert req == 2744067508
    assert r["route"] == [] and r["route_back"] == []   # direct: keys omitted entirely
    assert r["snr_towards"] == [10.25]                  # dB*4 wire format (41/4)
    assert r["snr_back"] == [10.5]                      # 42/4
    assert r["responder"] == "!1d1ea7ae"
    assert r["hop_start"] == 2

def test_multihop_route_and_unknown_snr():
    # synthetic: 1 intermediate hop each way; -128 = unknown SNR -> None
    fn = _pt()
    req, r = fn({"from": 0x0e57e001, "to": 0x379b4ea6, "hopStart": 4,
                 "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 99,
                             "traceroute": {"route": [305419896], "snrTowards": [22, 34],
                                            "routeBack": [305419896], "snrBack": [-128, 30]}}})
    assert req == 99
    assert r["route"] == ["!12345678"]
    assert r["snr_towards"] == [5.5, 8.5]
    assert r["route_back"] == ["!12345678"]
    assert r["snr_back"] == [None, 7.5]

def test_direct_hit_has_empty_route():
    # protobuf-dict OMITS empty fields: a direct (0-hop) trace has no "route" key
    fn = _pt()
    req, r = fn({"from": 1, "to": 2, "decoded": {"portnum": "TRACEROUTE_APP",
                "requestId": 7, "traceroute": {"snrTowards": [41]}}})
    assert req == 7 and r["route"] == [] and r["snr_towards"] == [10.25]
    # routeBack/snrBack are BOTH entirely absent here. Even a 0-hop (direct)
    # leg has exactly 1 real RF reading (the direct hop itself), so the wire
    # invariant len(snrBack) == len(routeBack)+1 == 1 is violated by an
    # entirely-missing snrBack (len 0) -- same mismatch class the SNR/route
    # cross-check (final finding) now catches. Silently returning [] here
    # would hide an unreported reading as if there were none to report;
    # the honest result is one unknown-SNR placeholder, not silence.
    assert r["route_back"] == [] and r["snr_back"] == [None]

def test_rejects_non_traceroute_and_missing_request_id():
    fn = _pt()
    assert fn({"decoded": {"portnum": "TEXT_MESSAGE_APP"}}) == (None, None)
    assert fn({"decoded": {"portnum": "TRACEROUTE_APP", "traceroute": {}}}) == (None, None)
    assert fn({}) == (None, None)
    assert fn(None) == (None, None)

# --- Review findings: fail-closed on malformed/hostile RF input (never crash) ---

def test_non_dict_packet_fails_closed():
    # a non-dict, non-None packet must not crash the `(packet or {}).get(...)` chain
    fn = _pt()
    assert fn(["not", "a", "dict"]) == (None, None)

def test_decoded_field_as_list_fails_closed():
    # dec.get("portnum") on a list would raise AttributeError without a guard
    fn = _pt()
    assert fn({"decoded": ["not", "a", "dict"]}) == (None, None)

def test_traceroute_field_as_list_fails_closed():
    # tr.get(key) on a list would raise AttributeError without a guard (finding 1)
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": ["not", "a", "dict"]}})
    assert (req, r) == (None, None)

def test_snr_towards_scalar_int_fails_closed():
    # iterating a scalar int would raise TypeError without a guard (finding 1)
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"snrTowards": 22}}})
    assert (req, r) == (None, None)

def test_list_with_invalid_element_fails_closed_not_silently_dropped():
    # a bad-typed element must fail the WHOLE parse, not silently drop and shift
    # the SNR-to-hop alignment (finding 2 / minor #5)
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"snrTowards": [22, "bad", 34]}}})
    assert (req, r) == (None, None)

def test_route_list_with_invalid_element_fails_closed():
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [1, None, 3]}}})
    assert (req, r) == (None, None)

def test_responder_uses_established_fromid_convention():
    # finding 3: responder must use packet.get("fromId") or "!{:08x}".format(packet.get("from", 0))
    # -- the same convention as on_neighbor/on_direct_neighbor/on_receive.
    fn = _pt()
    req, r = fn({"from": 999, "fromId": "!aabbccdd",
                "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5, "traceroute": {}}})
    assert r["responder"] == "!aabbccdd"

def test_responder_falls_back_to_from_when_fromid_absent():
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert r["responder"] == "!00000001"

def test_hop_start_non_int_yields_none():
    # finding 4: hopStart must get the same isinstance(int) and not isinstance(bool)
    # guard as every other numeric field in this function.
    fn = _pt()
    req, r = fn({"from": 1, "hopStart": "bogus",
                "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5, "traceroute": {}}})
    assert req == 5
    assert r["hop_start"] is None

def test_hop_start_bool_yields_none():
    fn = _pt()
    req, r = fn({"from": 1, "hopStart": True,
                "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5, "traceroute": {}}})
    assert r["hop_start"] is None

# --- Regression: responder fallback dropped the numeric guard when it adopted
# the fromId convention (finding 1). A malformed top-level `from` with no
# usable fromId must null the responder field, never raise. ---

def test_responder_from_string_yields_none_no_raise():
    fn = _pt()
    req, r = fn({"from": "bad", "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert req == 5
    assert r["responder"] is None

def test_responder_from_float_yields_none_no_raise():
    fn = _pt()
    req, r = fn({"from": 1.5, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert req == 5
    assert r["responder"] is None

def test_responder_from_none_yields_none_no_raise():
    fn = _pt()
    req, r = fn({"from": None, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert req == 5
    assert r["responder"] is None

def test_responder_from_list_yields_none_no_raise():
    fn = _pt()
    req, r = fn({"from": [1, 2], "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert req == 5
    assert r["responder"] is None

def test_responder_from_dict_yields_none_no_raise():
    fn = _pt()
    req, r = fn({"from": {"x": 1}, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert req == 5
    assert r["responder"] is None

def test_responder_from_bool_yields_none_no_raise():
    fn = _pt()
    req, r = fn({"from": True, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert req == 5
    assert r["responder"] is None

def test_responder_fromid_wins_over_malformed_from():
    # a usable fromId must short-circuit before the malformed `from` fallback
    # is ever evaluated -- fromId wins even when `from` itself would crash.
    fn = _pt()
    req, r = fn({"from": "bad", "fromId": "!aabbccdd",
                "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5, "traceroute": {}}})
    assert r["responder"] == "!aabbccdd"

# --- Regression: node numbers are uint32; out-of-range ints produced garbage
# ids (not valid 8-hex-digit ids) instead of failing closed (finding 2). ---

def test_route_with_negative_node_id_fails_closed():
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [-1]}}})
    assert (req, r) == (None, None)

def test_route_with_node_id_above_uint32_fails_closed():
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [99999999999]}}})
    assert (req, r) == (None, None)

def test_route_back_with_out_of_range_node_id_fails_closed():
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"routeBack": [0x100000000]}}})
    assert (req, r) == (None, None)

def test_route_boundary_zero_and_max_uint32_are_valid():
    # 0 and 0xFFFFFFFF are the uint32 boundary values -- both must parse
    # normally, not be mistaken for invalid input.
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [0, 0xFFFFFFFF]}}})
    assert req == 5
    assert r["route"] == ["!00000000", "!ffffffff"]

def test_responder_from_negative_yields_none_no_raise():
    fn = _pt()
    req, r = fn({"from": -1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert req == 5
    assert r["responder"] is None

def test_responder_from_above_uint32_yields_none_no_raise():
    fn = _pt()
    req, r = fn({"from": 99999999999, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert req == 5
    assert r["responder"] is None

def test_responder_from_boundary_zero_is_valid():
    fn = _pt()
    req, r = fn({"from": 0, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert r["responder"] == "!00000000"

def test_responder_from_boundary_max_uint32_is_valid():
    fn = _pt()
    req, r = fn({"from": 0xFFFFFFFF, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {}}})
    assert r["responder"] == "!ffffffff"

# --- Final finding: route and its SNR list are validated individually but never
# cross-checked. Wire format guarantees len(snrTowards) == len(route)+1 (and same
# for the back leg). A mismatch must NOT fail the whole parse (the route is real,
# received data) -- it must degrade the untrustworthy SNR list to a same-length
# list of None ("hops known, signal unknown"), same encoding as -128. The route
# is never discarded on an SNR-length mismatch. ---

def test_short_snr_towards_degrades_to_none_list_route_survives():
    # route: [1,2,3], snrTowards: [10] -- classic misalignment from the finding.
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [1, 2, 3], "snrTowards": [10]}}})
    assert req == 5 and r is not None
    assert r["route"] == ["!00000001", "!00000002", "!00000003"]
    assert r["snr_towards"] == [None, None, None, None]

def test_direct_with_oversized_snr_towards_degrades_to_single_none():
    # direct (route omitted, 0 hops) but 3 SNR entries reported -- also a mismatch.
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"snrTowards": [10, 20, 30]}}})
    assert req == 5 and r is not None
    assert r["route"] == []
    assert r["snr_towards"] == [None]

def test_snr_towards_absent_entirely_degrades_by_route_length():
    # key missing entirely (not even an empty list) is still a length-0 list --
    # same mismatch treatment as any other wrong length.
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [1]}}})
    assert req == 5 and r is not None
    assert r["route"] == ["!00000001"]
    assert r["snr_towards"] == [None, None]

def test_mismatched_snr_back_nulled_while_valid_snr_towards_preserved():
    # the two directions are independent: only the malformed side degrades.
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [1], "snrTowards": [10, 20],
                               "routeBack": [1], "snrBack": [5]}}})
    assert req == 5 and r is not None
    assert r["snr_towards"] == [2.5, 5.0]        # valid (len 2 == 1+1) -- untouched
    assert r["snr_back"] == [None, None]         # mismatched (len 1 != 1+1) -- nulled

def test_mismatched_snr_towards_nulled_while_valid_snr_back_preserved():
    # same independence, mirrored: the towards leg is the malformed one this time.
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [1], "snrTowards": [10],
                               "routeBack": [1], "snrBack": [10, 20]}}})
    assert req == 5 and r is not None
    assert r["snr_towards"] == [None, None]      # mismatched (len 1 != 1+1) -- nulled
    assert r["snr_back"] == [2.5, 5.0]            # valid (len 2 == 1+1) -- untouched

def test_correctly_sized_snr_list_passes_through_unchanged():
    # a well-formed SNR list (right length) is untouched by the new cross-check --
    # the existing -128 -> None sentinel mapping still applies alongside real values.
    fn = _pt()
    req, r = fn({"from": 1, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 5,
                "traceroute": {"route": [1, 2], "snrTowards": [10, -128, 30]}}})
    assert req == 5 and r is not None
    assert r["route"] == ["!00000001", "!00000002"]
    assert r["snr_towards"] == [2.5, None, 7.5]

# ---------- Task 3: traceroutes table + POST /api/traceroute endpoint ----------

def test_traceroute_endpoint_wired():
    # table migration
    assert "CREATE TABLE IF NOT EXISTS traceroutes(" in SRC
    # endpoint exists, token-gated, cooldown-gated, non-blocking sendData (never sendTraceRoute)
    assert '"/api/traceroute"' in SRC
    i_ep = SRC.index('"/api/traceroute"')
    i_end = SRC.index("def start_send_api")   # stable marker for the end of the endpoint method
    assert i_end > i_ep
    ep_src = SRC[i_ep:i_end]   # scope every assertion to the endpoint's own source region —
    # the old unscoped version searched the whole 1700-line file, so it would still pass if
    # the cooldown call / TRACEROUTE_APP / sweep SQL lived somewhere else entirely (finding 3)
    assert "traceroute_allowed_now()" in ep_src, "must use the dedicated thread-safe gate"
    assert "radio_check_allowed" not in ep_src, \
        "must NOT reuse the per-sender helper (unlocked get-then-set — unsafe on the threaded send API)"
    assert ep_src.count("traceroute_release()") == 2, \
        "must release the cooldown slot on exactly the two failure paths (503 radio-down, 502 send-failed)"
    assert "portnums_pb2.PortNum.TRACEROUTE_APP" in ep_src
    assert "wantResponse=True" in ep_src
    assert "sendTraceRoute" not in ep_src.replace("NEVER call `interface.sendTraceRoute", "")  # guard: blocking API banned
    # stale pendings swept to timeout on each new request
    assert "SET status='timeout' WHERE status='pending'" in ep_src

def test_radio_check_allowed_helper_still_works_standalone():
    # The traceroute endpoint no longer reuses this helper (Task 3 replaced it with the
    # thread-safe traceroute_allowed_now) — but radio_check_allowed is still load-bearing
    # for the radio-check responder, so it's exercised here standalone.
    fn, _ = _extract("radio_check_allowed")
    last = {}
    assert fn("global", 1000.0, last, 35) is True
    assert fn("global", 1030.0, last, 35) is False
    assert fn("global", 1036.0, last, 35) is True

# ---------- Findings 1+2: thread-safe traceroute gate, released on failed sends ----------

class _FakeTime:
    """Controllable clock injected as the `time` global for _extract()'d functions
    that call time.time() — lets retry_after assertions avoid real sleeps."""
    def __init__(self, t=0.0):
        self.t = t
    def time(self):
        return self.t

def _tr_gate(fake_time=None, cooldown=35):
    """Extract traceroute_allowed_now + traceroute_release sharing the SAME
    lock/dict objects — mutable objects passed by reference survive separate
    _extract() namespaces, same wiring as the real module."""
    tr_last = {}
    real_lock = threading.Lock()
    g = {"time": fake_time or _FakeTime(1000.0), "math": math, "lock": real_lock,
         "_tr_last": tr_last, "TRACEROUTE_COOLDOWN_S": cooldown}
    allowed_fn, _ = _extract("traceroute_allowed_now", g)
    release_fn, _ = _extract("traceroute_release", g)
    return allowed_fn, release_fn

def test_traceroute_allowed_now_blocks_with_decreasing_retry_after():
    ft = _FakeTime(1000.0)
    allowed_fn, _ = _tr_gate(fake_time=ft)
    assert allowed_fn() == (True, 0)
    ft.t = 1010.0   # +10s: still cooling down
    allowed, retry = allowed_fn()
    assert allowed is False
    assert retry == 25, "retry_after must be the ACTUAL remaining seconds, not the constant"
    ft.t = 1020.0   # +20s: retry_after must have decreased as real time elapses
    allowed, retry2 = allowed_fn()
    assert allowed is False
    assert retry2 == 15 and retry2 < retry
    ft.t = 1036.0   # +36s: cooldown fully elapsed
    assert allowed_fn() == (True, 0)

def test_traceroute_release_restores_immediate_availability():
    ft = _FakeTime(2000.0)
    allowed_fn, release_fn = _tr_gate(fake_time=ft)
    assert allowed_fn() == (True, 0)
    assert allowed_fn()[0] is False, "still inside the cooldown window"
    release_fn()
    assert allowed_fn() == (True, 0), "release must give the slot back with no wait"

def test_traceroute_allowed_now_concurrent_exactly_one_wins():
    """Regression test for Finding 1: the send API is a ThreadingHTTPServer (one
    thread per POST). An unlocked get-then-set gate lets two concurrent POSTs
    both read the stale timestamp before either writes — exactly the failure
    this test guards against."""
    allowed_fn, _ = _tr_gate(fake_time=_FakeTime(5000.0), cooldown=35)
    results = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(20)

    def worker():
        barrier.wait()   # line every thread up so they all call the gate together
        r = allowed_fn()
        with results_lock:
            results.append(r)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 20
    allowed_count = sum(1 for allowed, _ in results if allowed)
    assert allowed_count == 1, "exactly one concurrent probe may pass the global gate, got {}".format(allowed_count)

# ---------- Task 4: on_traceroute co-subscriber — correlate responses, honest failures ----------

def _mem_db():
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE traceroutes(id INTEGER PRIMARY KEY, ts REAL, dest TEXT, dest_name TEXT, "
                "request_id INTEGER, hop_limit INTEGER, status TEXT, route TEXT, snr_towards TEXT, "
                "route_back TEXT, snr_back TEXT, resp_ts REAL)")
    @contextlib.contextmanager
    def db():
        yield con
        con.commit()
    return con, db

def _ot(con, db):
    parse, _ = _extract("parse_traceroute")
    fn, _ = _extract("on_traceroute", {
        "db": db, "log": lambda *a: None, "time": time, "json": _json,
        "my_num": 0x379b4ea6, "parse_traceroute": parse,
    })
    return fn

def test_response_upgrades_pending_row_to_ok():
    con, db = _mem_db()
    con.execute("INSERT INTO traceroutes(ts, dest, request_id, status) VALUES(?, '!0e57e001', 42, 'pending')",
                (time.time(),))
    fn = _ot(con, db)
    fn(packet={"from": 0x0e57e001, "to": 0x379b4ea6,
               "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 42,
                           "traceroute": {"snrTowards": [40]}}}, interface=None)
    row = con.execute("SELECT status, route, snr_towards FROM traceroutes").fetchone()
    assert row[0] == "ok" and _json.loads(row[1]) == [] and _json.loads(row[2]) == [10.0]

def test_third_party_and_unmatched_ignored():
    con, db = _mem_db()
    fn = _ot(con, db)
    # addressed to someone else
    fn(packet={"from": 1, "to": 999, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 42,
               "traceroute": {}}}, interface=None)
    # matches nothing pending
    fn(packet={"from": 1, "to": 0x379b4ea6, "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 42,
               "traceroute": {}}}, interface=None)
    assert con.execute("SELECT COUNT(*) FROM traceroutes").fetchone()[0] == 0

def test_routing_error_marks_failed():
    con, db = _mem_db()
    con.execute("INSERT INTO traceroutes(ts, dest, request_id, status) VALUES(?, '!0e57e001', 42, 'pending')",
                (time.time(),))
    fn = _ot(con, db)
    fn(packet={"from": 0x0e57e001, "to": 0x379b4ea6,
               "decoded": {"portnum": "ROUTING_APP", "requestId": 42,
                           "routing": {"errorReason": "MAX_RETRANSMIT"}}}, interface=None)
    assert con.execute("SELECT status FROM traceroutes").fetchone()[0] == "failed:MAX_RETRANSMIT"

def test_routing_success_ack_is_not_terminal():
    # errorReason "NONE" (5a lesson: it's the STRING) = transit ack, row stays pending
    con, db = _mem_db()
    con.execute("INSERT INTO traceroutes(ts, dest, request_id, status) VALUES(?, '!0e57e001', 42, 'pending')",
                (time.time(),))
    fn = _ot(con, db)
    fn(packet={"from": 0x0e57e001, "to": 0x379b4ea6,
               "decoded": {"portnum": "ROUTING_APP", "requestId": 42,
                           "routing": {"errorReason": "NONE"}}}, interface=None)
    assert con.execute("SELECT status FROM traceroutes").fetchone()[0] == "pending"

def test_never_raises_and_subscribed():
    con, db = _mem_db()
    fn = _ot(con, db)
    fn(packet=None, interface=None)          # must swallow
    fn(packet={"decoded": "garbage"}, interface=None)
    assert 'pub.subscribe(on_traceroute, "meshtastic.receive")' in SRC
    assert "mesh-ai-bridge v17 starting" in SRC

# ---------- Review findings on on_traceroute (post-Task-4) ----------

def test_routing_app_bool_requestid_does_not_match_int_pending_row():
    # Finding 1: the ROUTING_APP branch read requestId with no bool guard, unlike
    # parse_traceroute's TRACEROUTE_APP path (isinstance(int) and not isinstance(bool)).
    # requestId=True is a bool, and True == 1 in both Python and SQLite -- it must NOT
    # correlate to a pending row whose real request_id is 1.
    con, db = _mem_db()
    con.execute("INSERT INTO traceroutes(ts, dest, request_id, status) VALUES(?, '!0e57e001', 1, 'pending')",
                (time.time(),))
    fn = _ot(con, db)
    fn(packet={"from": 0x0e57e001, "to": 0x379b4ea6,
               "decoded": {"portnum": "ROUTING_APP", "requestId": True,
                           "routing": {"errorReason": "MAX_RETRANSMIT"}}}, interface=None)
    assert con.execute("SELECT status FROM traceroutes").fetchone()[0] == "pending", \
        "a bool requestId must not be treated as a correlatable int and flip the row"

def test_third_party_response_does_not_flip_matching_pending_row():
    # Finding 2: isolates the to_num != my_num filter from the requestId match.
    # Seeds a pending row that WOULD match this response's requestId=42 if the
    # third-party filter were absent -- so this test only passes if that filter
    # is actually applied (unlike test_third_party_and_unmatched_ignored, whose
    # two sub-cases have no matching pending row regardless of the filter).
    con, db = _mem_db()
    con.execute("INSERT INTO traceroutes(ts, dest, request_id, status) VALUES(?, '!0e57e001', 42, 'pending')",
                (time.time(),))
    fn = _ot(con, db)
    fn(packet={"from": 0x0e57e001, "to": 999,   # addressed to a DIFFERENT node than my_num
               "decoded": {"portnum": "TRACEROUTE_APP", "requestId": 42,
                           "traceroute": {}}}, interface=None)
    assert con.execute("SELECT status FROM traceroutes").fetchone()[0] == "pending", \
        "a response addressed to a third party must not flip a same-requestId pending row"
