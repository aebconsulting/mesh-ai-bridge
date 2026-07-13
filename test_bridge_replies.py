"""Phase 6a replies/reactions tests — radio-free.

Same harness as test_bridge_acks.py: pure functions are ast-extracted from
bridge.py source and exec'd (bridge.py imports meshtastic at module load, so
it is never imported here); migrations/SQL are mirrored against temp sqlite
and asserted byte-identical to the shipped source.
"""
import ast, io, os, sqlite3, tempfile

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


# ---------- Task 1: migration ----------

def _apply_migration(dbpath):
    c = sqlite3.connect(dbpath)
    c.execute("CREATE TABLE IF NOT EXISTS msg_log(id INTEGER PRIMARY KEY, ts REAL, direction TEXT, "
              "node_id TEXT, node_name TEXT, channel INTEGER, is_dm INTEGER, is_ai INTEGER, text TEXT, "
              "mesh_id INTEGER, ack_state TEXT)")
    have = {r[1] for r in c.execute("PRAGMA table_info(msg_log)")}
    for name, decl in [("reply_to_id", "INTEGER"), ("is_reaction", "INTEGER")]:
        if name not in have:
            try:
                c.execute("ALTER TABLE msg_log ADD COLUMN {} {}".format(name, decl))
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise
    c.execute("CREATE INDEX IF NOT EXISTS idx_msg_log_reply_to ON msg_log(reply_to_id)")
    c.commit()
    return c


def test_replies_migration_idempotent():
    d = tempfile.mkdtemp(); p = os.path.join(d, "m.db")
    _apply_migration(p).close()
    _apply_migration(p).close()   # second run must not raise
    c = sqlite3.connect(p)
    cols = {r[1] for r in c.execute("PRAGMA table_info(msg_log)")}
    assert "reply_to_id" in cols and "is_reaction" in cols
    idx = {r[1] for r in c.execute("PRAGMA index_list(msg_log)")}
    assert "idx_msg_log_reply_to" in idx


def test_bridge_source_contains_replies_migration():
    assert '_add_cols(c, "msg_log", [("reply_to_id", "INTEGER"), ("is_reaction", "INTEGER")])' in SRC
    assert "idx_msg_log_reply_to" in SRC


# ---------- Task 2: log_traffic ----------

def test_log_traffic_inserts_reply_columns():
    # SQL split across string concatenation in source
    assert "INSERT INTO msg_log(ts, direction, node_id, node_name, channel, is_dm, is_ai, text, " in SRC
    assert "mesh_id, ack_state, reply_to_id, is_reaction)" in SRC
    assert "reply_to_id=None, is_reaction=None" in SRC   # kwargs with safe defaults


# ---------- Task 3: _send_and_log passthrough ----------

class _Pkt:
    def __init__(self, id=1234):
        self.id = id

def _send_and_log_replies_harness():
    calls = []
    def fake_log_traffic(direction, node_id, node_name, channel, is_dm, is_ai, text,
                         mesh_id=None, ack_state=None, reply_to_id=None, is_reaction=None):
        calls.append(dict(direction=direction, mesh_id=mesh_id, ack_state=ack_state,
                          reply_to_id=reply_to_id, is_reaction=is_reaction))
    fn, ns = _extract("_send_and_log", {"log_traffic": fake_log_traffic, "log": lambda *a: None,
                                        "sends_without_id": 0})
    return fn, calls

def test_send_and_log_passes_reply_metadata_on_success():
    fn, calls = _send_and_log_replies_harness()
    fn(lambda: _Pkt(77), "!aabbccdd", "X", 0, True, False, "hi", reply_to_id=55, is_reaction=1)
    assert calls[-1]["mesh_id"] == 77 and calls[-1]["reply_to_id"] == 55 and calls[-1]["is_reaction"] == 1

def test_send_and_log_passes_reply_metadata_on_failure():
    fn, calls = _send_and_log_replies_harness()
    def boom(): raise RuntimeError("radio")
    try:
        fn(boom, "!aabbccdd", "X", 0, True, False, "hi", reply_to_id=55, is_reaction=1)
    except RuntimeError:
        pass
    assert calls[-1]["ack_state"] == "failed" and calls[-1]["reply_to_id"] == 55 and calls[-1]["is_reaction"] == 1


# ---------- Task 4: _send_tapback ----------

class _FakeDecoded:
    def __init__(self):
        self.payload = b""; self.portnum = None; self.want_response = False
        self.reply_id = 0; self.emoji = 0

class _FakeMeshPacket:
    def __init__(self):
        self.decoded = _FakeDecoded(); self.channel = 0; self.id = 0; self.priority = None

class _FakeMeshPb2:
    MeshPacket = _FakeMeshPacket
    class MeshPacket_Priority:  # namespaced below via attribute assignment
        RELIABLE = 70
_FakeMeshPb2.MeshPacket.Priority = _FakeMeshPb2.MeshPacket_Priority

class _FakePortnums:
    class PortNum:
        TEXT_MESSAGE_APP = 1

class _FakeIface:
    def __init__(self):
        self.sent = None; self.kwargs = None
    def _generatePacketId(self):
        return 4242
    def _sendPacket(self, pkt, destinationId="^all", wantAck=False, **kw):
        self.sent = pkt; self.kwargs = dict(destinationId=destinationId, wantAck=wantAck)
        return pkt

def _tapback():
    fn, ns = _extract("_send_tapback", {"mesh_pb2": _FakeMeshPb2, "portnums_pb2": _FakePortnums})
    return fn

def test_tapback_packet_fields_dm():
    fn = _tapback(); iface = _FakeIface()
    pkt = fn(iface, "👍", 999, destinationId="!849a5bc8")
    assert iface.sent.decoded.emoji == 1
    assert iface.sent.decoded.reply_id == 999
    assert iface.sent.decoded.portnum == 1                       # TEXT_MESSAGE_APP
    assert iface.sent.decoded.payload == "👍".encode("utf-8")
    assert iface.sent.id == 4242
    assert iface.kwargs == {"destinationId": "!849a5bc8", "wantAck": True}
    assert pkt is iface.sent

def test_tapback_packet_fields_broadcast():
    fn = _tapback(); iface = _FakeIface()
    fn(iface, "❤️", 1000, channelIndex=0)
    assert iface.sent.channel == 0
    assert iface.kwargs["destinationId"] == "^all"


# ---------- Task 5: inbound meta + quoted send ----------

def test_inbound_meta_normal_message():
    fn, _ = _extract("_inbound_meta")
    assert fn({"id": 111}, {"text": "hello"}) == (111, None, None)

def test_inbound_meta_reply():
    fn, _ = _extract("_inbound_meta")
    assert fn({"id": 112}, {"text": "yes", "replyId": 111}) == (112, 111, None)

def test_inbound_meta_tapback():
    fn, _ = _extract("_inbound_meta")
    assert fn({"id": 113}, {"text": "👍", "replyId": 111, "emoji": 1}) == (113, 111, 1)

def test_inbound_meta_missing_id_is_none():
    fn, _ = _extract("_inbound_meta")
    assert fn({}, {}) == (None, None, None)

def test_quoted_send_quotes_only_first_call():
    fn, _ = _extract("make_quoted_send")
    seen = []
    send = fn(lambda c, rid=None: seen.append((c, rid)), 555)
    send("part 1"); send("part 2"); send("part 3")
    assert seen == [("part 1", 555), ("part 2", None), ("part 3", None)]

def test_on_receive_source_logs_meta_and_guards_reactions():
    # Shape assertions on the shipped source (on_receive itself needs the radio).
    assert "_inbound_meta(packet, dec)" in SRC
    assert "if is_reaction:" in SRC          # tapback logged, then return before @ai path
    assert "make_quoted_send(" in SRC


# ---------- Task 6: send API validation ----------

def _vrf():
    fn, _ = _extract("_validate_reply_fields")
    return fn

def test_reply_fields_absent_ok():
    assert _vrf()({}, "hi") == (None, None, False)

def test_reply_id_valid():
    assert _vrf()({"reply_id": 1}, "hi") == (None, 1, False)
    assert _vrf()({"reply_id": 0xFFFFFFFF}, "hi") == (None, 0xFFFFFFFF, False)

def test_reply_id_invalid():
    assert _vrf()({"reply_id": 0}, "hi")[0] is not None
    assert _vrf()({"reply_id": -5}, "hi")[0] is not None
    assert _vrf()({"reply_id": 0x100000000}, "hi")[0] is not None
    assert _vrf()({"reply_id": "12"}, "hi")[0] is not None
    assert _vrf()({"reply_id": True}, "hi")[0] is not None    # bool is not an int here

def test_react_requires_reply_id():
    assert _vrf()({"react": True}, "👍")[0] is not None

def test_react_caps_text_bytes():
    ok = _vrf()({"react": True, "reply_id": 5}, "👍")
    assert ok == (None, 5, True)
    assert _vrf()({"react": True, "reply_id": 5}, "way too long")[0] is not None

def test_react_multibyte_emoji_within_cap():
    assert _vrf()({"react": True, "reply_id": 5}, "🙏")[0] is None   # 4 bytes

def test_do_post_source_routes_react_to_tapback():
    assert "_validate_reply_fields(data, text)" in SRC
    assert "_send_tapback(" in SRC
    assert "replyId=reply_id" in SRC
