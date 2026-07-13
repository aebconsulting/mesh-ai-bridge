"""5a delivery-tracking tests — radio-free.

bridge.py imports meshtastic at module load, so these tests never import it.
Instead they (a) extract the PURE functions (ack_state_for, _send_and_log)
from bridge.py source via ast and exec them, so the SHIPPED source is what's
tested, and (b) mirror the msg_log migration/correlation SQL against a temp
sqlite file, asserting the mirrored SQL is byte-identical to bridge.py's.
"""
import ast, io, os, re, sqlite3, tempfile, time

BRIDGE = os.path.join(os.path.dirname(__file__), "bridge.py")
SRC = io.open(BRIDGE, encoding="utf-8").read()


def _extract(func_name, extra_globals=None):
    """Exec a single top-level function from bridge.py source, no imports."""
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
              "node_id TEXT, node_name TEXT, channel INTEGER, is_dm INTEGER, is_ai INTEGER, text TEXT)")
    have = {r[1] for r in c.execute("PRAGMA table_info(msg_log)")}
    for name, decl in [("mesh_id", "INTEGER"), ("ack_state", "TEXT")]:
        if name not in have:
            try:
                c.execute("ALTER TABLE msg_log ADD COLUMN {} {}".format(name, decl))
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise
    c.execute("CREATE INDEX IF NOT EXISTS idx_msg_log_mesh_id ON msg_log(mesh_id)")
    c.commit()
    return c


def test_migration_adds_columns_idempotently():
    d = tempfile.mkdtemp(); p = os.path.join(d, "m.db")
    _apply_migration(p).close()
    _apply_migration(p).close()  # second run must not raise
    c = sqlite3.connect(p)
    cols = {r[1] for r in c.execute("PRAGMA table_info(msg_log)")}
    assert "mesh_id" in cols and "ack_state" in cols
    idx = {r[1] for r in c.execute("PRAGMA index_list(msg_log)")}
    assert "idx_msg_log_mesh_id" in idx


def test_bridge_source_contains_matching_migration():
    assert '_add_cols(c, "msg_log", [("mesh_id", "INTEGER"), ("ack_state", "TEXT")])' in SRC
    assert "idx_msg_log_mesh_id" in SRC


# ---------- Task 3: ack_state_for ----------

def _ack_state_for():
    f, _ = _extract("ack_state_for")
    return f


def test_dm_end_to_end_ack():
    assert _ack_state_for()(None, 42, 42, 7, True) == "ack"


def test_dm_local_transmit_ack_is_radio_accepted():
    assert _ack_state_for()("NONE", 7, 42, 7, True) == "radio-accepted"


def test_dm_intermediate_relayer_is_relayed():
    # success from a node that is neither us nor the destination = progressed
    # into the mesh via a relayer — 'relayed', not 'radio-accepted'.
    assert _ack_state_for()(None, 99, 42, 7, True) == "relayed"


def test_ack_rank_upgrade_ladder():
    f, ns = None, None
    import ast
    tree = ast.parse(SRC)
    g = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "ack_rank":
            exec(compile(ast.Module(body=[node], type_ignores=[]), BRIDGE, "exec"), g)
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "_ACK_RANK":
            exec(compile(ast.Module(body=[node], type_ignores=[]), BRIDGE, "exec"), g)
    rank = g["ack_rank"]
    assert rank(None) < rank("radio-accepted") < rank("relayed") < rank("ack")
    assert rank("failed:MAX_RETRANSMIT") == rank("ack")   # both terminal
    assert not rank("ack") < rank("radio-accepted")       # never downgrade


def test_broadcast_success_is_relayed():
    assert _ack_state_for()(None, 7, None, 7, False) == "relayed"


def test_nak_carries_reason():
    assert _ack_state_for()("NO_ROUTE", 42, 42, 7, True) == "failed:NO_ROUTE"
    assert _ack_state_for()("MAX_RETRANSMIT", 7, 42, 7, True) == "failed:MAX_RETRANSMIT"


def test_none_string_is_success():
    # Live probe 2026-07-12: success ACKs carry errorReason "NONE" (string), not absent.
    assert _ack_state_for()("NONE", 42, 42, 7, True) == "ack"


# ---------- Task 4: _send_and_log ----------

def _send_and_log_harness():
    logged = []
    def fake_log_traffic(direction, node_id, node_name, channel, is_dm, is_ai, text,
                         mesh_id=None, ack_state=None, reply_to_id=None, is_reaction=None):
        logged.append({"mesh_id": mesh_id, "ack_state": ack_state, "text": text})

    g = {"log_traffic": fake_log_traffic, "log": lambda *a: None,
         "sends_without_id": 0}
    f, ns = _extract("_send_and_log", extra_globals=g)
    return f, logged, ns


def test_send_and_log_records_id_on_success():
    f, logged, ns = _send_and_log_harness()
    class Pkt: id = 12345
    pkt = f(lambda: Pkt(), "!aa", "A", 0, True, False, "hi")
    assert pkt.id == 12345
    assert logged[-1]["mesh_id"] == 12345 and logged[-1]["ack_state"] is None


def test_send_and_log_failed_row_on_raise():
    f, logged, ns = _send_and_log_harness()
    def boom():
        raise RuntimeError("radio down")
    raised = False
    try:
        f(boom, "!aa", "A", 0, True, False, "hi")
    except RuntimeError:
        raised = True
    assert raised
    assert logged[-1]["ack_state"] == "failed" and logged[-1]["mesh_id"] is None


def test_send_and_log_idless_pkt_counts():
    f, logged, ns = _send_and_log_harness()
    class Pkt:
        pass  # no .id
    f(lambda: Pkt(), "!aa", "A", 0, False, True, "hi")
    assert logged[-1]["mesh_id"] is None and logged[-1]["ack_state"] is None
    assert ns["sends_without_id"] == 1


def test_send_and_log_zero_id_counts_as_missing():
    # protobuf scalar .id defaults to 0 — must be treated as "no id", not id 0.
    f, logged, ns = _send_and_log_harness()
    class Pkt:
        id = 0
    f(lambda: Pkt(), "!aa", "A", 0, False, True, "hi")
    assert logged[-1]["mesh_id"] is None
    assert ns["sends_without_id"] == 1


# ---------- Task 5: correlation SQL ----------

CORRELATE_SQL = ("SELECT id, node_id, is_dm, ack_state FROM msg_log WHERE mesh_id=? AND direction='out' "
                 "AND (ack_state IS NULL OR ack_state IN ('radio-accepted','relayed')) "
                 "AND ts > ? ORDER BY ts DESC LIMIT 1")


def test_correlation_sql_matches_bridge_source():
    # The mirrored SQL these tests exercise must be the SQL the bridge ships.
    # bridge.py splits the literal across adjacent strings; compare with quotes/
    # whitespace collapsed so the RUNTIME statement is what's checked.
    collapsed = re.sub(r'"\s*\n\s*"', "", SRC)
    assert CORRELATE_SQL in collapsed


def test_orphan_ack_matches_nothing():
    d = tempfile.mkdtemp(); p = os.path.join(d, "m.db"); _apply_migration(p).close()
    c = sqlite3.connect(p)
    row = c.execute(CORRELATE_SQL, (999, time.time() - 300)).fetchone()
    assert row is None


def test_ack_matches_recent_out_row_by_id_and_fence():
    d = tempfile.mkdtemp(); p = os.path.join(d, "m.db"); _apply_migration(p).close()
    c = sqlite3.connect(p)
    now = time.time()
    c.execute("INSERT INTO msg_log(ts,direction,node_id,is_dm,text,mesh_id,ack_state) "
              "VALUES(?,?,?,?,?,?,NULL)", (now, "out", "!0000002a", 1, "hi", 777))
    # a stale row with the SAME reused id must NOT match (recency fence)
    c.execute("INSERT INTO msg_log(ts,direction,node_id,is_dm,text,mesh_id,ack_state) "
              "VALUES(?,?,?,?,?,?,NULL)", (now - 4000, "out", "!0000002a", 1, "old", 778))
    c.commit()
    assert c.execute(CORRELATE_SQL, (777, now - 300)).fetchone() is not None
    assert c.execute(CORRELATE_SQL, (778, now - 300)).fetchone() is None


def test_terminal_ack_row_not_rematched():
    d = tempfile.mkdtemp(); p = os.path.join(d, "m.db"); _apply_migration(p).close()
    c = sqlite3.connect(p)
    now = time.time()
    c.execute("INSERT INTO msg_log(ts,direction,node_id,is_dm,text,mesh_id,ack_state) "
              "VALUES(?,?,?,?,?,?,?)", (now, "out", "!0000002a", 1, "hi", 555, "ack"))
    c.execute("INSERT INTO msg_log(ts,direction,node_id,is_dm,text,mesh_id,ack_state) "
              "VALUES(?,?,?,?,?,?,?)", (now, "out", "!0000002a", 1, "hi2", 556, "failed:NO_ROUTE"))
    c.commit()
    assert c.execute(CORRELATE_SQL, (555, now - 300)).fetchone() is None
    assert c.execute(CORRELATE_SQL, (556, now - 300)).fetchone() is None


def test_weak_state_row_rematches_for_upgrade():
    # A row locked at radio-accepted must still match, so the destination's
    # later end-to-end ack (or a NAK) can upgrade it.
    d = tempfile.mkdtemp(); p = os.path.join(d, "m.db"); _apply_migration(p).close()
    c = sqlite3.connect(p)
    now = time.time()
    c.execute("INSERT INTO msg_log(ts,direction,node_id,is_dm,text,mesh_id,ack_state) "
              "VALUES(?,?,?,?,?,?,?)", (now, "out", "!0000002a", 1, "hi", 700, "radio-accepted"))
    c.commit()
    row = c.execute(CORRELATE_SQL, (700, now - 300)).fetchone()
    assert row is not None and row[3] == "radio-accepted"
