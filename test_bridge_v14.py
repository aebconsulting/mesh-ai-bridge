"""v14 radio-check responder tests — radio-free (ast-extraction harness,
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


def _rc():
    fn, _ = _extract("radio_check_reply", {"RADIO_CHECKS": {"ping": "pong", "test": "test OK"}})
    return fn


def test_ping_direct_with_snr():
    fn = _rc()
    assert fn("ping", {"hopStart": 3, "hopLimit": 3, "rxSnr": 8.25}, 236) ==         "pong — heard you direct, SNR 8.25 dB · bridge + \"@ai\" online, 236 nodes known"


def test_ping_direct_without_snr():
    fn = _rc()
    assert fn("ping", {"hopStart": 3, "hopLimit": 3}) == "pong — heard you direct · bridge + \"@ai\" online"


def test_ping_multihop_reports_hop_count():
    fn = _rc()
    assert fn("ping", {"hopStart": 3, "hopLimit": 1}, 10) ==         "pong — heard you via 2 hops · bridge + \"@ai\" online, 10 nodes known"
    assert fn("ping", {"hopStart": 3, "hopLimit": 2}) == "pong — heard you via 1 hop · bridge + \"@ai\" online"


def test_ping_no_hop_data_bare_word():
    fn = _rc()
    assert fn("ping", {}) == "pong · bridge + \"@ai\" online"


def test_test_word():
    fn = _rc()
    assert fn("test", {"hopStart": 5, "hopLimit": 4}) == "test OK — heard you via 1 hop · bridge + \"@ai\" online"


def test_case_and_punctuation_normalized():
    fn = _rc()
    assert fn("  PING! ", {}) == "pong · bridge + \"@ai\" online"
    assert fn("Test?", {}) == "test OK · bridge + \"@ai\" online"


def test_non_checks_return_none():
    fn = _rc()
    for q in ("ping me later", "testing", "what is ping", "", None, "pingtest"):
        assert fn(q, {}) is None, q


def test_reply_fits_one_lora_chunk():
    fn = _rc()
    worst = fn("test", {"hopStart": 7, "hopLimit": 0, "rxSnr": -20.25}, 99999)
    assert len(worst.encode("utf-8")) < 190


def test_bool_and_malformed_fields_are_safe():
    fn = _rc()
    # bools are int subclasses; malformed hopStart<hopLimit means unknown, not negative
    assert fn("ping", {"hopStart": True, "hopLimit": True}) == "pong · bridge + \"@ai\" online"
    assert fn("ping", {"hopStart": 1, "hopLimit": 3}) == "pong · bridge + \"@ai\" online"
    assert fn("ping", {"hopStart": 3, "hopLimit": 3, "rxSnr": True}) == "pong — heard you direct · bridge + \"@ai\" online"
    assert fn("ping", {}, True) == "pong · bridge + \"@ai\" online"


def test_wired_into_both_paths():
    # @ai path: after dedup, before the cooldown block; bare-DM path inside `if not is_ai`
    assert SRC.count("radio_check_reply(") >= 3          # def + 2 call sites
    # v15: both call sites pass total-known AND the live online count
    # (v18: live_online_count prefers MeshMonitor's view, falls back to count_online)
    assert "rc = radio_check_reply(query, packet, len(_nodes), live_online_count(_nodes, time.time()))" in SRC
    assert "rc = radio_check_reply(text, packet, len(_nodes), live_online_count(_nodes, time.time()))" in SRC
    # the @ai call must come AFTER the dedup guard and BEFORE the cooldown exemption comment
    i_dedup = SRC.index("dropping duplicate retransmit")
    i_rc = SRC.index("rc = radio_check_reply(query, packet,")
    i_more = SRC.index('query.lower() not in ("more", "continue")')
    assert i_dedup < i_rc < i_more


# ---------- v15: pong reports live mesh activity, not nodedb size ----------

def _co():
    fn, _ = _extract("count_online", {"ONLINE_WINDOW_S": 7200})
    return fn


def test_online_count_preferred_over_known():
    fn = _rc()
    assert fn("ping", {"hopStart": 3, "hopLimit": 3, "rxSnr": 8.25}, 236, 41) == \
        "pong — heard you direct, SNR 8.25 dB · bridge + \"@ai\" up, 41 nodes online"
    assert fn("test", {}, 10, 3) == "test OK · bridge + \"@ai\" up, 3 nodes online"


def test_online_missing_or_zero_falls_back_to_known():
    fn = _rc()
    # no online data (None) or a false-looking 0 -> old total-known tail
    assert fn("ping", {}, 236, None) == "pong · bridge + \"@ai\" online, 236 nodes known"
    assert fn("ping", {}, 236, 0) == "pong · bridge + \"@ai\" online, 236 nodes known"
    assert fn("ping", {}, 236, True) == "pong · bridge + \"@ai\" online, 236 nodes known"
    assert fn("ping", {}, None, None) == "pong · bridge + \"@ai\" online"


def test_online_reply_fits_one_lora_chunk():
    fn = _rc()
    worst = fn("test", {"hopStart": 7, "hopLimit": 0, "rxSnr": -20.25}, 99999, 99999)
    assert len(worst.encode("utf-8")) < 190


def test_count_online_windows_and_malformed():
    fn = _co()
    now = 100000.0
    nodes = {
        "!a": {"lastHeard": now - 60},        # fresh
        "!b": {"lastHeard": now - 7100},      # inside 2h
        "!c": {"lastHeard": now - 7300},      # stale
        "!d": {},                             # no field
        "!e": None,                           # malformed entry
        "!f": {"lastHeard": True},            # bool is not a timestamp
    }
    assert fn(nodes, now) == 2


def test_count_online_no_usable_data_returns_none():
    fn = _co()
    assert fn({}, 1000.0) is None
    assert fn(None, 1000.0) is None
    assert fn({"!a": {}, "!b": None, "!c": {"lastHeard": False}}, 1000.0) is None


def _rca():
    fn, _ = _extract("radio_check_allowed")
    return fn


def test_channel_cooldown_allows_then_blocks():
    fn = _rca()
    last = {}
    assert fn("!a", 1000.0, last, 120) is True
    assert fn("!a", 1060.0, last, 120) is False    # 60s later: blocked
    assert fn("!a", 1121.0, last, 120) is True     # 121s later: allowed again
    assert fn("!b", 1060.0, last, 120) is True     # other senders independent


def test_channel_path_wired_with_guards():
    # channel pong: env-gated, ALLOWED-gated, cooldown-gated, threaded via replyId
    assert "RADIO_CHECK_CHANNEL and ch in ALLOWED" in SRC
    assert "radio_check_allowed(sender, time.time(), _rc_last, RADIO_CHECK_COOLDOWN_S)" in SRC
    i_dm = SRC.index("if is_dm:", SRC.index("Bare \"ping\"/\"test\" is a radio check"))
    i_ch = SRC.index("elif (RADIO_CHECK_CHANNEL")
    assert i_dm < i_ch
