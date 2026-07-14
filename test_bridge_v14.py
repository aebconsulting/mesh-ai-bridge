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
    assert fn("ping", {"hopStart": 3, "hopLimit": 3, "rxSnr": 8.25}, 236) ==         "pong — heard you direct, SNR 8.25 dB · bridge + AI online, 236 nodes known"


def test_ping_direct_without_snr():
    fn = _rc()
    assert fn("ping", {"hopStart": 3, "hopLimit": 3}) == "pong — heard you direct · bridge + AI online"


def test_ping_multihop_reports_hop_count():
    fn = _rc()
    assert fn("ping", {"hopStart": 3, "hopLimit": 1}, 10) ==         "pong — heard you via 2 hops · bridge + AI online, 10 nodes known"
    assert fn("ping", {"hopStart": 3, "hopLimit": 2}) == "pong — heard you via 1 hop · bridge + AI online"


def test_ping_no_hop_data_bare_word():
    fn = _rc()
    assert fn("ping", {}) == "pong · bridge + AI online"


def test_test_word():
    fn = _rc()
    assert fn("test", {"hopStart": 5, "hopLimit": 4}) == "test OK — heard you via 1 hop · bridge + AI online"


def test_case_and_punctuation_normalized():
    fn = _rc()
    assert fn("  PING! ", {}) == "pong · bridge + AI online"
    assert fn("Test?", {}) == "test OK · bridge + AI online"


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
    assert fn("ping", {"hopStart": True, "hopLimit": True}) == "pong · bridge + AI online"
    assert fn("ping", {"hopStart": 1, "hopLimit": 3}) == "pong · bridge + AI online"
    assert fn("ping", {"hopStart": 3, "hopLimit": 3, "rxSnr": True}) == "pong — heard you direct · bridge + AI online"
    assert fn("ping", {}, True) == "pong · bridge + AI online"


def test_wired_into_both_paths():
    # @ai path: after dedup, before the cooldown block; bare-DM path inside `if not is_ai`
    assert SRC.count("radio_check_reply(") >= 3          # def + 2 call sites
    assert 'rc = radio_check_reply(query, packet, len(getattr(interface, "nodes", None) or {}))' in SRC
    assert 'rc = radio_check_reply(text, packet, len(getattr(interface, "nodes", None) or {}))' in SRC
    # the @ai call must come AFTER the dedup guard and BEFORE the cooldown exemption comment
    i_dedup = SRC.index("dropping duplicate retransmit")
    i_rc = SRC.index("rc = radio_check_reply(query, packet,")
    i_more = SRC.index('query.lower() not in ("more", "continue")')
    assert i_dedup < i_rc < i_more


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
