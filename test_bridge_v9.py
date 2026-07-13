#!/usr/bin/env python3
"""v9 unit tests. Run ON AIBOX: MEM_DB=/tmp/v9test.db ~/meshenv/bin/python test_bridge_v9.py"""
import importlib.util, os, time

os.environ.setdefault("MEM_DB", "/tmp/v9test.db")
if os.path.exists(os.environ["MEM_DB"]):
    os.remove(os.environ["MEM_DB"])

spec = importlib.util.spec_from_file_location("bridge", os.path.join(os.path.dirname(__file__), "bridge.py"))
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


def _cols(table):
    with bridge.db() as c:
        return {r[1] for r in c.execute("PRAGMA table_info({})".format(table))}


def _tables():
    with bridge.db() as c:
        return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}


NEW_NODE_COLS = {"hw_model", "role", "altitude", "voltage", "chan_util", "air_util_tx",
                 "uptime_s", "rssi", "via_mqtt", "sats", "loc_source"}


def test_migration_idempotent():
    """Calling db() twice must be safe: new nodes columns + telemetry/neighbors tables
    exist after the FIRST call, and a SECOND call must not error or duplicate anything
    (ALTER TABLE ADD COLUMN only fires for columns that are still missing)."""
    bridge.db().close()
    cols1 = _cols("nodes")
    tabs1 = _tables()
    assert NEW_NODE_COLS <= cols1, "all new nodes columns must exist after first db() call: missing {}".format(
        NEW_NODE_COLS - cols1)
    assert "telemetry" in tabs1 and "neighbors" in tabs1, "telemetry/neighbors tables must exist"
    # Second call: idempotent, no exception, same schema.
    bridge.db().close()
    cols2 = _cols("nodes")
    tabs2 = _tables()
    assert cols1 == cols2, "a second db() call must not change the nodes schema"
    assert tabs1 == tabs2, "a second db() call must not change the table set"


class StubIface:
    nodes = {}


def test_snapshot_captures_metadata():
    """A node dict with the full v9 metadata shape must land in the nodes row."""
    iface = StubIface()
    iface.nodes = {
        "!aaaaaaaa": {
            "user": {"shortName": "AB", "longName": "Alpha Bravo", "hwModel": "TBEAM", "role": "CLIENT"},
            "position": {"latitude": 30.1, "longitude": -85.2, "altitude": 42.0,
                        "satsInView": 9, "locationSource": "LOC_INTERNAL"},
            "deviceMetrics": {"batteryLevel": 87, "voltage": 4.01, "channelUtilization": 12.5,
                              "airUtilTx": 3.2, "uptimeSeconds": 123456},
            "snr": 7.25, "hopsAway": 2, "lastHeard": time.time(), "rssi": -91, "viaMqtt": True,
        }
    }
    n = bridge.snapshot_nodes(iface)
    assert n == 1, "snapshot_nodes must report 1 row processed, got {}".format(n)
    with bridge.db() as c:
        row = c.execute(
            "SELECT hw_model, role, voltage, chan_util, air_util_tx, uptime_s, altitude, rssi, "
            "via_mqtt, sats, loc_source FROM nodes WHERE node_id=?", ("!aaaaaaaa",)).fetchone()
    assert row is not None, "node row must exist after snapshot"
    (hw_model, role, voltage, chan_util, air_util_tx, uptime_s, altitude, rssi,
     via_mqtt, sats, loc_source) = row
    assert hw_model == "TBEAM"
    assert role == "CLIENT"
    assert voltage == 4.01
    assert chan_util == 12.5
    assert air_util_tx == 3.2
    assert uptime_s == 123456
    assert altitude == 42.0
    assert rssi == -91
    assert via_mqtt == 1, "viaMqtt=True must be coerced to int 1"
    assert sats == 9
    assert loc_source == "LOC_INTERNAL"


def test_store_telemetry_all_kinds():
    """Every numeric metric across the recognized telemetry groups must be recorded;
    bool flags must be skipped (bools sneak in as int subclass)."""
    with bridge.db() as c:
        c.execute("DELETE FROM telemetry")
    tele = {
        "deviceMetrics": {"batteryLevel": 91, "voltage": 3.9, "channelUtilization": 5.0},
        "environmentMetrics": {"temperature": 21.3, "relativeHumidity": 55.0},
        "powerMetrics": {"ch1Voltage": 12.1, "ch1Current": 0.5},
        "airQualityMetrics": {"pm25Standard": 8, "pm10Standard": 12},
        "someFlag": True,   # not a recognized group at all - must be ignored, not crash
    }
    tele["deviceMetrics"]["isCharging"] = True   # a bool flag sitting alongside real metrics
    bridge.store_telemetry("!bbbbbbbb", "Bravo", tele)
    with bridge.db() as c:
        rows = c.execute("SELECT kind, metric, value FROM telemetry WHERE node_id=?",
                         ("!bbbbbbbb",)).fetchall()
    got = {(k, m): v for k, m, v in rows}
    assert got[("deviceMetrics", "batteryLevel")] == 91.0
    assert got[("deviceMetrics", "voltage")] == 3.9
    assert got[("deviceMetrics", "channelUtilization")] == 5.0
    assert got[("environmentMetrics", "temperature")] == 21.3
    assert got[("environmentMetrics", "relativeHumidity")] == 55.0
    assert got[("powerMetrics", "ch1Voltage")] == 12.1
    assert got[("powerMetrics", "ch1Current")] == 0.5
    assert got[("airQualityMetrics", "pm25Standard")] == 8.0
    assert got[("airQualityMetrics", "pm10Standard")] == 12.0
    assert ("deviceMetrics", "isCharging") not in got, "bool flags must be skipped, not stored as 1.0/0.0"
    assert len(rows) == 9, "exactly the 9 numeric metrics must be stored (isCharging bool skipped), got {}".format(len(rows))


def test_on_neighbor():
    """A NEIGHBORINFO_APP packet with 2 neighbors must produce 2 rows with the numeric
    nodeId converted to !hex form; a TEXT_MESSAGE_APP packet must add nothing and never raise."""
    with bridge.db() as c:
        c.execute("DELETE FROM neighbors")
    packet = {
        "decoded": {
            "portnum": "NEIGHBORINFO_APP",
            "neighborinfo": {
                "neighbors": [
                    {"nodeId": 0xCCCCCCCC, "snr": 5.5},
                    {"nodeId": "!dddddddd", "snr": -2.25},
                ]
            },
        },
        "fromId": "!eeeeeeee",
        "from": 0xEEEEEEEE,
    }
    bridge.on_neighbor(packet, StubIface())
    with bridge.db() as c:
        rows = c.execute("SELECT node_id, neighbor_id, snr FROM neighbors ORDER BY id").fetchall()
    assert rows == [
        ("!eeeeeeee", "!cccccccc", 5.5),
        ("!eeeeeeee", "!dddddddd", -2.25),
    ], "neighbor rows must record node_id/neighbor_id (hex-converted)/snr correctly, got {}".format(rows)

    # Non-NEIGHBORINFO packet: must be a no-op and must never raise.
    text_packet = {"decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "hello"},
                   "fromId": "!eeeeeeee", "from": 0xEEEEEEEE}
    bridge.on_neighbor(text_packet, StubIface())
    with bridge.db() as c:
        n = c.execute("SELECT COUNT(*) FROM neighbors").fetchone()[0]
    assert n == 2, "a TEXT_MESSAGE_APP packet must not add any neighbor rows"

    # Malformed packet (None): must never raise out of the handler.
    bridge.on_neighbor(None, StubIface())
    bridge.on_neighbor({}, StubIface())


def test_prune_new_tables():
    """prune_msg_log must delete telemetry/neighbors rows older than RETENTION_DAYS and
    keep fresh rows, mirroring the existing env_log/msg_log behavior."""
    old = time.time() - (bridge.RETENTION_DAYS + 1) * 86400
    with bridge.db() as c:
        c.execute("DELETE FROM telemetry"); c.execute("DELETE FROM neighbors")
        c.execute("INSERT INTO telemetry(ts, node_id, node_name, kind, metric, value) VALUES(?,?,?,?,?,?)",
                  (old, "!old", "Old", "deviceMetrics", "batteryLevel", 50.0))
        c.execute("INSERT INTO telemetry(ts, node_id, node_name, kind, metric, value) VALUES(?,?,?,?,?,?)",
                  (time.time(), "!new", "New", "deviceMetrics", "batteryLevel", 60.0))
        c.execute("INSERT INTO neighbors(ts, node_id, neighbor_id, snr) VALUES(?,?,?,?)",
                  (old, "!old", "!oldnbr", 1.0))
        c.execute("INSERT INTO neighbors(ts, node_id, neighbor_id, snr) VALUES(?,?,?,?)",
                  (time.time(), "!new", "!newnbr", 2.0))
    assert bridge.prune_msg_log() is True
    with bridge.db() as c:
        tele_ids = [r[0] for r in c.execute("SELECT node_id FROM telemetry")]
        nbr_ids = [r[0] for r in c.execute("SELECT node_id FROM neighbors")]
    assert "!old" not in tele_ids and "!new" in tele_ids, \
        "prune must delete old telemetry rows and keep fresh ones, got {}".format(tele_ids)
    assert "!old" not in nbr_ids and "!new" in nbr_ids, \
        "prune must delete old neighbors rows and keep fresh ones, got {}".format(nbr_ids)


if __name__ == "__main__":
    for f in [test_migration_idempotent, test_snapshot_captures_metadata,
              test_store_telemetry_all_kinds, test_on_neighbor, test_prune_new_tables]:
        f(); print("PASS", f.__name__)
    print("ALL v9 TESTS PASSED")
