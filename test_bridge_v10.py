#!/usr/bin/env python3
"""v10 direct-neighbor derivation tests. Run ON AIBOX:
   MEM_DB=/tmp/v10test.db ~/meshenv/bin/python test_bridge_v10.py"""
import importlib.util, os, time

os.environ.setdefault("MEM_DB", "/tmp/v10test.db")
os.environ["DIRECT_NEIGHBOR_THROTTLE_S"] = "300"
if os.path.exists(os.environ["MEM_DB"]):
    os.remove(os.environ["MEM_DB"])

spec = importlib.util.spec_from_file_location("bridge", os.path.join(os.path.dirname(__file__), "bridge.py"))
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)

bridge.my_num = 0xAABBCCDD          # base node num
BASE = "!aabbccdd"

def _neighbors():
    with bridge.db() as c:
        return c.execute("SELECT node_id, neighbor_id, snr FROM neighbors ORDER BY id").fetchall()

def test_direct_reception_recorded():
    bridge._direct_seen.clear()
    bridge.on_direct_neighbor(packet={"from": 0x11112222, "fromId": "!11112222",
                                       "hopStart": 3, "hopLimit": 3, "rxSnr": 7.5}, interface=None)
    rows = _neighbors()
    assert rows == [(BASE, "!11112222", 7.5)], rows

def test_multihop_ignored():
    bridge._direct_seen.clear()
    before = len(_neighbors())
    bridge.on_direct_neighbor(packet={"from": 0x33334444, "fromId": "!33334444",
                                       "hopStart": 3, "hopLimit": 1, "rxSnr": 2.0}, interface=None)
    assert len(_neighbors()) == before, "multi-hop packet must not be recorded"

def test_mqtt_ignored():
    bridge._direct_seen.clear()
    before = len(_neighbors())
    bridge.on_direct_neighbor(packet={"from": 0x55556666, "fromId": "!55556666",
                                       "hopStart": 0, "hopLimit": 0, "viaMqtt": True, "rxSnr": 9.0}, interface=None)
    assert len(_neighbors()) == before, "viaMqtt packet must not be recorded"

def test_self_ignored():
    bridge._direct_seen.clear()
    before = len(_neighbors())
    bridge.on_direct_neighbor(packet={"from": bridge.my_num, "hopStart": 3, "hopLimit": 3, "rxSnr": 1.0}, interface=None)
    assert len(_neighbors()) == before, "packet from self must not be recorded"

def test_throttle():
    bridge._direct_seen.clear()
    before = len(_neighbors())
    for _ in range(3):  # three direct packets from the same sender, immediately
        bridge.on_direct_neighbor(packet={"from": 0x77778888, "fromId": "!77778888",
                                          "hopStart": 2, "hopLimit": 2, "rxSnr": 4.0}, interface=None)
    assert len(_neighbors()) == before + 1, "throttle must collapse rapid repeats to one row"

def test_missing_hop_info_ignored():
    bridge._direct_seen.clear()
    before = len(_neighbors())
    bridge.on_direct_neighbor(packet={"from": 0x9999aaaa, "fromId": "!9999aaaa", "rxSnr": 3.0}, interface=None)
    assert len(_neighbors()) == before, "packet with no hop info must not be recorded"

if __name__ == "__main__":
    for f in [test_direct_reception_recorded, test_multihop_ignored, test_mqtt_ignored,
              test_self_ignored, test_throttle, test_missing_hop_info_ignored]:
        f(); print("PASS", f.__name__)
    print("ALL v10 TESTS PASSED")
