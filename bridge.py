#!/usr/bin/env python3
"""mesh-ai-bridge v18 - Meshtastic (serial or TCP) -> local LLM, with persistent memory.

v18: the radio-check pong's online count prefers MeshMonitor's view. The bridge's
own nodedb sits BEHIND MeshMonitor's virtual-node feed, which only re-forwards
decoded app traffic -- MeshMonitor itself logs every packet HEADER the radio hears,
including encrypted foreign-channel traffic (measured 2026-07-15: MM 95 vs bridge
29 nodes in the same 2h window; the pong said 73 while the Meridian UI said 120).
With MESHMONITOR_API_URL set the pong quotes MeshMonitor's count; unset, down, or
garbage falls back to the bridge's own count -- exactly the v15 behavior. The
fetch is cached (successes AND failures) so the pong path stays instant.

v17 adds operator-triggered mesh traceroute: POST /api/traceroute fires a non-blocking
route-discovery probe (sendData(wantResponse=True), never the blocking sendTraceRoute)
and inserts a pending traceroutes row keyed by request_id; on_traceroute, an isolated
co-subscriber to meshtastic.receive, correlates the eventual TRACEROUTE_APP response (or
a terminal ROUTING_APP failure) back to that row by exact requestId. States are reported
honestly: pending (still in flight), ok (route filled in), failed:<REASON> (a routing
error other than the literal string "NONE" — which is just a transit ack, not a verdict,
and leaves the row pending), or timeout (swept on the next request past TRACEROUTE_TTL_S).
A global 35s cooldown mirrors the firmware's own traceroute rate limit. Purely additive -
no change to text handling, the query queue, or existing ACK tracking.

v16: wires the TEI cross-encoder reranker (bge-reranker-v2-m3, :8091) into
library retrieval behind RERANK_ENABLED. Floor-passing qdrant candidates are
re-scored against the query, reordered, and noise chunks (rerank score below
RERANK_MIN_SCORE) are dropped — cosine similarity retrieves look-alike noise
(ROS "node" articles for mesh-status questions) that the cross-encoder rates
~2e-5 vs 0.7+ for genuinely relevant text. Any reranker failure degrades to
qdrant order; reranking never costs an answer.

v15: the radio-check pong tail reports live mesh activity — nodes heard within
ONLINE_WINDOW_S (default 2h, the Meridian dashboard's "online" convention) —
instead of total nodedb size, which counts weeks-stale entries. Falls back to
"N nodes known" when the nodedb carries no lastHeard data.

v14 adds a deterministic radio-check responder: "@ai ping"/"@ai test", bare
"ping"/"test" DMs at the bridge, and bare channel radio checks (per-sender
cooldown, RADIO_CHECK_CHANNEL=0 to disable) answer instantly with the
mesh-conventional report (hop count, SNR when direct) from the packet's real
reception data — never touching the LLM, the queue, or conversation history.
LLM ping-roleplay hallucinations were observed when these reached the model.

v13 adds multi-collection retrieval: QDRANT_COLLECTIONS (comma-separated) searches every
listed collection and merges hits by score — e.g. the general zim library plus a curated
survival/medical corpus (SurvivalRAG) side by side. All collections must share the query
embedder (nomic-embed-text 768/cosine). Per-collection failures degrade to the remaining
collections; Kiwix fallback only when all are down.

v12 adds replies & reactions: msg_log stores every message's mesh packet id, reply target
(reply_to_id) and tapback flag (is_reaction); the send API accepts reply_id (quoted reply)
and react (emoji tapback via hand-built packet — no public API sets Data.emoji); @ai answers
quote the question on their first chunk. Additive only.

v9 adds "collect it all": full per-node metadata (hw model/role, GPS altitude/fix source,
device health, RF path), a general telemetry EAV table capturing every numeric metric from
every telemetry group (device/env/power/air-quality/etc.), and a neighbors table recording
NEIGHBORINFO links for a mesh topology view. Purely additive - no change to radio TX, the
send API, the query queue, or existing text handling.

v8 adds NET_BACKUP: when the host happens to have internet, live-data questions (weather/
forecast) are answered with real current conditions from keyless public APIs (Open-Meteo +
zippopotam.us zip geocoding). A cached connectivity probe gates every fetch; offline the
bridge behaves exactly as before (mesh sensors + honest "no live data"). Default OFF.

v7 adds ENVIRONMENT_METRICS capture: nodes with weather sensors (BME280/680) broadcast
temperature/humidity/pressure telemetry across the mesh; the bridge records it to env_log
and injects an aggregated "current local weather" line into the AI context — real local
conditions with no internet, the off-grid-native weather source.

v4 adds MESH_TCP_HOST: connect to the radio via TCP (ser2net/meshtasticd) for the
containerized deployment; unset = native serial mode (rollback path).
v2 added SQLite memory (per-sender conversation history + long-term facts).
"@ai remember <fact>" stores a fact; every query is answered with facts +
that sender recent turns in context. Env-configured; drops on LLM failure;
rate-limited; channel-scoped; LoRa-sized replies.
"""
import os, re, sys, time, threading, collections, sqlite3, json, hmac, math
import queue as _queue
import requests
from pubsub import pub
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import meshtastic.serial_interface
import meshtastic.tcp_interface
from meshtastic.protobuf import mesh_pb2, portnums_pb2

SERIAL = os.environ.get("MESH_SERIAL", "/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0")
# MESH_TCP_HOST set -> connect to the radio over TCP (ser2net or meshtasticd) instead of
# serial. This is how the containerized bridge reaches the radio (no device passthrough
# in NOMAD custom apps); leave unset for native serial mode (rollback path).
TCP_HOST = os.environ.get("MESH_TCP_HOST", "").strip()
TCP_PORT = int(os.environ.get("MESH_TCP_PORT", "4403"))
LLM_BASE = os.environ.get("LLM_BASE_URL", "http://127.0.0.1:11434/v1")
MODEL = os.environ.get("LLM_MODEL", "deepseek-r1:8b")
PREFIX = os.environ.get("TRIGGER_PREFIX", "@ai").lower()
ALLOWED = {int(c) for c in os.environ.get("ALLOWED_CHANNELS", "0").split(",") if c.strip() != ""}
CHUNK = int(os.environ.get("CHUNK_BYTES", "190"))
MAX_CHUNKS = int(os.environ.get("MAX_REPLY_CHUNKS", "2"))          # channel replies: shared airtime, stay lean
MAX_CHUNKS_DM = int(os.environ.get("MAX_REPLY_CHUNKS_DM", "3"))    # DMs: point-to-point, room for a full answer
COOLDOWN = int(os.environ.get("NODE_COOLDOWN_S", "30"))
PER_MIN = int(os.environ.get("GLOBAL_PER_MIN", "6"))
MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", "600"))
TIMEOUT = int(os.environ.get("LLM_TIMEOUT_S", "120"))
MEM_DB = os.environ.get("MEM_DB", "/opt/mesh-ai-bridge/memory.db")
RETENTION_DAYS = int(os.environ.get("MSG_LOG_RETENTION_DAYS", "90"))
ADMIN_NODES = {n.strip().lower() for n in os.environ.get("ADMIN_NODES", "").split(",") if n.strip()}
KIWIX_URL = os.environ.get("KIWIX_URL", "http://127.0.0.1:8090")
LIBRARY_BOOKS = [b.strip() for b in os.environ.get("LIBRARY_BOOKS", "").split(",") if b.strip()]
LIBRARY_MAX_BOOKS = int(os.environ.get("LIBRARY_MAX_BOOKS", "3"))
LIBRARY_CONTEXT_CHARS = int(os.environ.get("LIBRARY_CONTEXT_CHARS", "2500"))
# v6/2b-i (qdrant rewrite): semantic retrieval replaces the cross-encoder pipeline.
EMBED_URL = os.environ.get("EMBED_URL", "http://127.0.0.1:11434").rstrip("/")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333").rstrip("/")
QDRANT_COLLECTION = os.environ.get("QDRANT_COLLECTION", "knowledge_base")
# v13: search MULTIPLE collections and merge by score. Comma-separated names; unset =
# just QDRANT_COLLECTION (back-compat). All collections must share the query embedder
# (nomic-embed-text, 768/cosine) or the scores are not comparable.
QDRANT_COLLECTIONS = [c.strip() for c in os.environ.get("QDRANT_COLLECTIONS", "").split(",") if c.strip()] or [QDRANT_COLLECTION]
QDRANT_TOP_K = int(os.environ.get("QDRANT_TOP_K", "8"))
QDRANT_MIN_SCORE = float(os.environ.get("QDRANT_MIN_SCORE", "0.65"))          # measured: relevant medical ~0.74-0.84, noise <=0.63
QDRANT_TIMEOUT_S = int(os.environ.get("QDRANT_TIMEOUT_S", "10"))
EMBED_TIMEOUT_S = int(os.environ.get("EMBED_TIMEOUT_S", "15"))
MIN_CHUNK_CHARS = int(os.environ.get("MIN_CHUNK_CHARS", "120"))               # F1: skip tiny fragments; a 2-word snippet is not usable medical context
RERANK_ENABLED = os.environ.get("RERANK_ENABLED", "").lower() in ("1", "true", "yes")  # DEFAULT OFF: cross-encoder rerank (wired in v16)
# Reach the TEI reranker by CONTAINER NAME on the shared NOMAD network, not via a host
# gateway: :8091 is UFW-scoped to the LAN, so both 172.17.0.1 and 172.18.0.1 silently DROP
# (verified — a timeout, not a refusal), which would stall every library query for the full
# timeout and then degrade, reranking nothing. Container DNS answers in ~1.2s.
RERANK_URL = os.environ.get("RERANK_URL", "http://nomad_custom_reranker:80")
# This sits on the ~1.7s mesh reply path, so the timeout is a latency budget, not a
# generous ceiling: a reranker slower than this is worse than no reranker.
RERANK_TIMEOUT_S = float(os.environ.get("RERANK_TIMEOUT_S", "5"))
RERANK_MIN_SCORE = float(os.environ.get("RERANK_MIN_SCORE", "0.001"))          # measured: relevant 0.7+, look-alike noise ~2e-5
HISTORY_TURNS = int(os.environ.get("HISTORY_TURNS", "6"))
FACTS_MAX = int(os.environ.get("FACTS_MAX", "20"))
SYSPROMPT = os.environ.get("SYSTEM_PROMPT",
    "You are NOMAD, an off-grid AI assistant reached over a LoRa mesh radio network. "
    "Replies must be plain text, no markdown, and under 300 characters total. "
    "Be direct and useful; brevity is life-or-death bandwidth here.")
SEND_TOKEN = os.environ.get("SEND_TOKEN", "")
SEND_PORT = int(os.environ.get("SEND_PORT", "8700"))
SEND_PER_MIN = int(os.environ.get("SEND_PER_MIN", "6"))
SEND_COOLDOWN_S = int(os.environ.get("SEND_COOLDOWN_S", "5"))
# v6/2b-ii: bounded no-drop query queue in front of handle_query.
QUEUE_MAX = int(os.environ.get("QUEUE_MAX", "100"))
DEDUP_TTL_S = int(os.environ.get("DEDUP_TTL_S", "120"))
# v7: environmental telemetry -> "current local weather" from mesh sensor nodes.
ENV_WINDOW_S = int(os.environ.get("ENV_WINDOW_S", "3600"))   # only readings this fresh count as "current"
DIRECT_NEIGHBOR_THROTTLE_S = int(os.environ.get("DIRECT_NEIGHBOR_THROTTLE_S", "300"))  # v10: min gap between recorded base->direct-neighbor samples
ENV_TEMP_UNIT = os.environ.get("ENV_TEMP_UNIT", "F").upper()  # Meshtastic reports Celsius; display F (US) or C
# v8: internet backup for live-data queries. Keyless APIs only (Open-Meteo, zippopotam.us) -
# nothing to configure or leak. Default OFF so the public image stays offline-first.
NET_BACKUP = os.environ.get("NET_BACKUP", "").lower() in ("1", "true", "yes")
NET_TIMEOUT_S = float(os.environ.get("NET_TIMEOUT_S", "6"))          # per-request budget for live fetches
NET_PROBE_TTL_S = int(os.environ.get("NET_PROBE_TTL_S", "60"))       # cache the online/offline verdict this long
NET_CACHE_TTL_S = int(os.environ.get("NET_CACHE_TTL_S", "600"))      # reuse fetched conditions per place this long
NET_DEFAULT_PLACE = os.environ.get("NET_DEFAULT_PLACE", "").strip()  # used when a live query names no location
SEARX_URL = os.environ.get("SEARX_URL", "").strip()   # local SearXNG for general live search; empty disables
SEARCH_RESULTS = int(os.environ.get("SEARCH_RESULTS", "4"))          # top-N snippets injected per search
# v8.1: multi-chunk delivery. 2s spacing lost chunk 2 on a busy mesh (fire-and-forget TX
# collided with chunk 1's still-in-flight routing traffic); give each chunk clear air.
CHUNK_DELAY_S = float(os.environ.get("CHUNK_DELAY_S", "8"))

last_by_node = {}
recent = collections.deque()
lock = threading.Lock()
_dash_recent = collections.deque()   # v6/A5: dashboard send-API rate bucket, independent of the mesh @ai bucket above
_dash_last = {}
send_api_alive = False               # v6/A1: True while the send-API server thread is alive
# v6/2b-ii: single bounded FIFO queue feeding handle_query, and packet-id retransmit dedup.
# QUEUE_MAX is a memory backstop only — on_receive's enqueue is a BLOCKING put (holds the
# radio thread if momentarily full), never a drop. See _worker()/on_receive for the policy.
work_q = _queue.Queue(maxsize=QUEUE_MAX)
worker_alive = False
_pending = {}   # sender -> untransmitted reply overflow, pulled with "@ai more" (worker thread only)
last_progress_ts = 0.0                   # v6/2b-ii F7: last time the worker finished (or attempted) an item — staleness metric
_seen_ids = collections.OrderedDict()   # packet_id -> ts, for retransmit dedup

def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)

# ---------- memory ----------
def db():
    c = sqlite3.connect(MEM_DB)
    mode = c.execute("PRAGMA journal_mode=WAL").fetchone()[0]     # v6: readers never block the writer
    if str(mode).lower() != "wal":
        log("WARNING: journal_mode is '{}', not wal — reader/writer may block".format(mode))
    c.execute("PRAGMA busy_timeout=5000")
    c.execute("CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, sender TEXT, role TEXT, content TEXT, ts REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS facts(id INTEGER PRIMARY KEY, sender TEXT, content TEXT, ts REAL)")
    # msg_log: EVERY mesh text message seen (in + out), for the dashboard feed.
    c.execute("CREATE TABLE IF NOT EXISTS msg_log(id INTEGER PRIMARY KEY, ts REAL, direction TEXT, "
              "node_id TEXT, node_name TEXT, channel INTEGER, is_dm INTEGER, is_ai INTEGER, text TEXT)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_msg_log_ts ON msg_log(ts)")
    # nodes: latest telemetry snapshot per node (position, battery, signal, last heard).
    c.execute("CREATE TABLE IF NOT EXISTS nodes(node_id TEXT PRIMARY KEY, short_name TEXT, long_name TEXT, "
              "lat REAL, lon REAL, battery INTEGER, snr REAL, hops INTEGER, last_heard REAL, updated REAL)")
    # v7: env_log - append-only environmental telemetry (temp/humidity/pressure) per node, for
    # "current local weather" aggregation and (future) pressure-trend storm detection.
    c.execute("CREATE TABLE IF NOT EXISTS env_log(id INTEGER PRIMARY KEY, ts REAL, node_id TEXT, "
              "node_name TEXT, temperature REAL, humidity REAL, pressure REAL, lat REAL, lon REAL)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_env_log_ts ON env_log(ts)")

    def _add_cols(c, table, cols):
        have = {r[1] for r in c.execute("PRAGMA table_info({})".format(table))}
        for name, decl in cols:
            if name not in have:
                try:
                    c.execute("ALTER TABLE {} ADD COLUMN {} {}".format(table, name, decl))
                except sqlite3.OperationalError as e:
                    # First-deploy TOCTOU: another connection may have added this column
                    # between our table_info read and here. A "duplicate column name"
                    # means the column now exists (self-heals); anything else re-raises.
                    if "duplicate column name" not in str(e).lower():
                        raise

    # v9: "collect it all" - extra per-node metadata Meshtastic exposes but the bridge
    # previously discarded (hw model/role, GPS altitude/fix source, device health, RF path).
    _add_cols(c, "nodes", [
        ("hw_model", "TEXT"), ("role", "TEXT"), ("altitude", "REAL"), ("voltage", "REAL"),
        ("chan_util", "REAL"), ("air_util_tx", "REAL"), ("uptime_s", "INTEGER"),
        ("rssi", "REAL"), ("via_mqtt", "INTEGER"), ("sats", "INTEGER"), ("loc_source", "TEXT"),
    ])
    # v9: general telemetry (EAV) — every numeric metric from every telemetry packet.
    c.execute("CREATE TABLE IF NOT EXISTS telemetry(id INTEGER PRIMARY KEY, ts REAL, node_id TEXT, node_name TEXT, kind TEXT, metric TEXT, value REAL)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry(ts)")
    # v9: neighbor links (who each node hears directly) — for a mesh topology view.
    c.execute("CREATE TABLE IF NOT EXISTS neighbors(id INTEGER PRIMARY KEY, ts REAL, node_id TEXT, neighbor_id TEXT, snr REAL)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_neighbors_ts ON neighbors(ts)")
    # v17: mesh traceroute requests + (eventually, via Task 4's on_traceroute) responses.
    c.execute("CREATE TABLE IF NOT EXISTS traceroutes(id INTEGER PRIMARY KEY, ts REAL, dest TEXT, "
              "dest_name TEXT, request_id INTEGER, hop_limit INTEGER, status TEXT, route TEXT, "
              "snr_towards TEXT, route_back TEXT, snr_back TEXT, resp_ts REAL)")
    # v11/5a: per-message delivery tracking — outbound packet id + ACK/NAK state.
    _add_cols(c, "msg_log", [("mesh_id", "INTEGER"), ("ack_state", "TEXT")])
    c.execute("CREATE INDEX IF NOT EXISTS idx_msg_log_mesh_id ON msg_log(mesh_id)")
    # v12/6a: reply threading + emoji tapbacks
    _add_cols(c, "msg_log", [("reply_to_id", "INTEGER"), ("is_reaction", "INTEGER")])
    c.execute("CREATE INDEX IF NOT EXISTS idx_msg_log_reply_to ON msg_log(reply_to_id)")
    return c

def log_traffic(direction, node_id, node_name, channel, is_dm, is_ai, text, mesh_id=None, ack_state=None,
                reply_to_id=None, is_reaction=None):
    try:
        with db() as c:
            c.execute("INSERT INTO msg_log(ts, direction, node_id, node_name, channel, is_dm, is_ai, text, "
                      "mesh_id, ack_state, reply_to_id, is_reaction) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                      (time.time(), direction, node_id, node_name, channel, int(is_dm), int(is_ai), text,
                       mesh_id, ack_state, reply_to_id, is_reaction))
    except Exception as e:
        log("msg_log write failed: {}".format(e))

def snapshot_nodes(interface):
    try:
        rows = []
        for nid, n in (getattr(interface, "nodes", None) or {}).items():
            u = n.get("user", {}) or {}
            p = n.get("position", {}) or {}
            m = n.get("deviceMetrics", {}) or {}
            hw_model = u.get("hwModel")
            role = u.get("role")
            loc_source = p.get("locationSource")
            via_mqtt = n.get("viaMqtt")
            rows.append((nid, u.get("shortName"), u.get("longName"), p.get("latitude"), p.get("longitude"),
                         m.get("batteryLevel"), n.get("snr"), n.get("hopsAway"), n.get("lastHeard"), time.time(),
                         str(hw_model) if hw_model is not None else None,
                         str(role) if role is not None else None,
                         p.get("altitude"), m.get("voltage"), m.get("channelUtilization"), m.get("airUtilTx"),
                         m.get("uptimeSeconds"), n.get("rssi"),
                         int(bool(via_mqtt)) if via_mqtt is not None else None,
                         p.get("satsInView"),
                         str(loc_source) if loc_source is not None else None))
        with db() as c:
            c.executemany("INSERT INTO nodes(node_id, short_name, long_name, lat, lon, battery, snr, hops, "
                          "last_heard, updated, hw_model, role, altitude, voltage, chan_util, air_util_tx, "
                          "uptime_s, rssi, via_mqtt, sats, loc_source) "
                          "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                          "ON CONFLICT(node_id) DO UPDATE SET short_name=excluded.short_name, "
                          "long_name=excluded.long_name, lat=excluded.lat, lon=excluded.lon, "
                          "battery=excluded.battery, snr=excluded.snr, hops=excluded.hops, "
                          "last_heard=excluded.last_heard, updated=excluded.updated, "
                          "hw_model=excluded.hw_model, role=excluded.role, altitude=excluded.altitude, "
                          "voltage=excluded.voltage, chan_util=excluded.chan_util, "
                          "air_util_tx=excluded.air_util_tx, uptime_s=excluded.uptime_s, "
                          "rssi=excluded.rssi, via_mqtt=excluded.via_mqtt, sats=excluded.sats, "
                          "loc_source=excluded.loc_source", rows)
        return len(rows)
    except Exception as e:
        log("node snapshot failed: {}".format(e))
        return 0

def prune_msg_log():
    """v6/v7/v9: msg_log + env_log + telemetry + neighbors share memory.db with the AI memory
    (backed up nightly) - cap growth. Returns True on success, False on failure so the caller
    can retry (A3)."""
    try:
        with db() as c:
            cutoff = time.time() - RETENTION_DAYS * 86400
            n = c.execute("DELETE FROM msg_log WHERE ts < ?", (cutoff,)).rowcount
            e = c.execute("DELETE FROM env_log WHERE ts < ?", (cutoff,)).rowcount
            t = c.execute("DELETE FROM telemetry WHERE ts < ?", (cutoff,)).rowcount
            b = c.execute("DELETE FROM neighbors WHERE ts < ?", (cutoff,)).rowcount
        if n or e or t or b:
            log("pruned msg_log {} / env_log {} / telemetry {} / neighbors {} rows older than {}d".format(
                n, e, t, b, RETENTION_DAYS))
        return True
    except Exception as e:
        log("prune failed: {}".format(e))
        return False

# ---------- environmental telemetry (v7) ----------
_env_seen = False   # loud one-time confirmation that weather data IS flowing on this mesh

def log_env(node_id, node_name, temp, humidity, pressure, lat, lon):
    """Store one environmental reading. Loudly logs the FIRST env packet ever seen so a
    deploy immediately answers 'do any nodes broadcast weather telemetry?'."""
    global _env_seen
    if temp is None and humidity is None and pressure is None:
        return   # a telemetry packet with no environment fields (device metrics only) - ignore
    try:
        with db() as c:
            c.execute("INSERT INTO env_log(ts, node_id, node_name, temperature, humidity, pressure, lat, lon) "
                      "VALUES(?,?,?,?,?,?,?,?)",
                      (time.time(), node_id, node_name, temp, humidity, pressure, lat, lon))
        if not _env_seen:
            _env_seen = True
            log("ENV TELEMETRY CONFIRMED on mesh - first weather reading from {}: "
                "temp={}C humidity={}% pressure={}hPa".format(node_name, temp, humidity, pressure))
    except Exception as e:
        log("env_log write failed: {}".format(e))

_TELEMETRY_GROUPS = ("deviceMetrics", "environmentMetrics", "powerMetrics", "airQualityMetrics", "localStats", "healthMetrics")

def store_telemetry(node_id, node_name, tele):
    """v9: record every numeric metric from any telemetry group into the EAV telemetry table."""
    rows = []
    now = time.time()
    for kind in _TELEMETRY_GROUPS:
        grp = tele.get(kind) or {}
        if not isinstance(grp, dict):
            continue
        for metric, value in grp.items():
            if isinstance(value, bool):     # bools sneak in as ints — skip flags
                continue
            if isinstance(value, (int, float)):
                rows.append((now, node_id, node_name, kind, metric, float(value)))
    if not rows:
        return
    try:
        with db() as c:
            c.executemany("INSERT INTO telemetry(ts, node_id, node_name, kind, metric, value) VALUES(?,?,?,?,?,?)", rows)
    except Exception as e:
        log("telemetry write failed: {}".format(e))

def on_telemetry(packet=None, interface=None):
    """Subscribed to meshtastic.receive.telemetry. Extracts environmentMetrics (temp/humidity/
    pressure) and records them; ignores device-only telemetry. ADDITIONALLY (v9) records every
    numeric metric from every telemetry group (device/env/power/air-quality/etc.) into the
    general telemetry EAV table. Never raises out (a telemetry handler error must not affect
    text handling)."""
    try:
        tele = (packet or {}).get("decoded", {}).get("telemetry", {}) or {}
        env = tele.get("environmentMetrics") or {}
        sender = packet.get("fromId") or "!{:08x}".format(packet.get("from", 0))
        node_name = node_display(interface, sender)
        if env:
            n = (getattr(interface, "nodes", None) or {}).get(sender, {}) or {}
            p = n.get("position", {}) or {}
            log_env(sender, node_name,
                    env.get("temperature"), env.get("relativeHumidity"), env.get("barometricPressure"),
                    p.get("latitude"), p.get("longitude"))
        store_telemetry(sender, node_name, tele)
    except Exception as e:
        log("telemetry handler error: {}".format(e))

def weather_context():
    """Aggregate the freshest per-node environmental readings (last ENV_WINDOW_S) into a short
    'current local weather' line for the AI. Returns '' if no recent sensor data exists (so it
    injects nothing rather than a stale or empty claim)."""
    try:
        with db() as c:
            rows = c.execute("SELECT node_name, temperature, humidity, pressure, ts FROM env_log "
                             "WHERE ts > ? ORDER BY ts DESC", (time.time() - ENV_WINDOW_S,)).fetchall()
    except Exception as e:
        log("weather_context read failed: {}".format(e))
        return ""
    latest = {}
    for name, t, h, p, ts in rows:
        latest.setdefault(name, (t, h, p))   # rows are ts-desc, so first-seen per node = freshest
    def avg(i):
        vals = [v[i] for v in latest.values() if v[i] is not None]
        return sum(vals) / len(vals) if vals else None
    at, ah, ap = avg(0), avg(1), avg(2)
    parts = []
    if at is not None:
        parts.append("temp {:.0f}F".format(at * 9 / 5 + 32) if ENV_TEMP_UNIT == "F" else "temp {:.0f}C".format(at))
    if ah is not None:
        parts.append("humidity {:.0f}%".format(ah))
    if ap is not None:
        parts.append("pressure {:.0f} hPa".format(ap))
    if not parts:
        return ""
    return "Live local weather from {} mesh sensor node(s), measured within the last hour: {}.".format(
        len(latest), ", ".join(parts))

# ---------- internet backup for live-data queries (v8) ----------
# All state below is only touched from the single query-worker thread; no locking needed.
_net_verdict = (0.0, False)          # (checked_at, online) - cached connectivity probe
_geo_cache = {}                      # place-string -> (lat, lon, display_name)
_wx_cache = {}                       # display_name -> (fetched_at, context_line)
_WMO = {0: "clear", 1: "mostly clear", 2: "partly cloudy", 3: "overcast",
        45: "fog", 48: "fog", 51: "drizzle", 53: "drizzle", 55: "drizzle",
        61: "light rain", 63: "rain", 65: "heavy rain", 66: "freezing rain", 67: "freezing rain",
        71: "light snow", 73: "snow", 75: "heavy snow", 77: "snow", 80: "rain showers",
        81: "rain showers", 82: "violent rain showers", 85: "snow showers", 86: "snow showers",
        95: "thunderstorm", 96: "thunderstorm w/ hail", 99: "thunderstorm w/ hail"}
_LIVE_WX_RE = re.compile(r"\b(weather|forecast|temperature|temp|rain(ing)?|wind(y)?|storm|hurricane"
                         r"|humidity|heat index|hot out|cold out|uv|barometer)\b", re.I)

def _net_online():
    """Cached internet probe. One tiny request answers 'are we online?' for NET_PROBE_TTL_S,
    so offline deployments pay one fast timeout a minute at most, not one per query."""
    global _net_verdict
    checked, online = _net_verdict
    if time.time() - checked < NET_PROBE_TTL_S:
        return online
    try:
        requests.get("https://api.open-meteo.com/v1/forecast?latitude=0&longitude=0&current=temperature_2m",
                     timeout=min(NET_TIMEOUT_S, 3))
        online = True
    except Exception:
        online = False
    _net_verdict = (time.time(), online)
    log("net_backup: probe -> {}".format("online" if online else "offline"))
    return online

def _extract_place(query):
    """Pull a location out of the query: a US zip wins, then 'in/for/at/near <place>',
    then NET_DEFAULT_PLACE. Returns '' if nothing to go on."""
    z = re.search(r"\b(\d{5})\b", query)
    if z:
        return z.group(1)
    m = re.search(r"\b(?:in|for|at|near)\s+([A-Za-z][A-Za-z .'\-]{2,40}?)"
                  r"(?:[?.!,]|\s+(?:today|tonight|tomorrow|now|right|this|next)\b|$)", query)
    if m:
        return m.group(1).strip()
    return ""   # nothing explicit; caller falls back to GPS, then NET_DEFAULT_PLACE

def _node_latlon(node_id):
    """GPS position for a node from the 60s node snapshot; None if it has no fix."""
    if not node_id:
        return None
    try:
        with db() as c:
            r = c.execute("SELECT lat, lon FROM nodes WHERE node_id=? AND lat IS NOT NULL "
                          "AND lon IS NOT NULL", (node_id,)).fetchone()
        return (r[0], r[1]) if r else None
    except Exception:
        return None

def _revgeo(lat, lon):
    """Coords -> 'City, Region' via keyless BigDataCloud; falls back to raw coords. Cached
    (rounded to ~1km) so a stationary node costs one lookup, not one per weather query."""
    key = ("rev", round(lat, 2), round(lon, 2))
    if key in _geo_cache:
        return _geo_cache[key]
    name = "{:.3f}, {:.3f}".format(lat, lon)
    try:
        j = requests.get("https://api.bigdatacloud.net/data/reverse-geocode-client",
                         params={"latitude": lat, "longitude": lon, "localityLanguage": "en"},
                         timeout=NET_TIMEOUT_S).json()
        # locality is neighborhood/town-accurate; city is metro-level (returns "Miami" for
        # the whole South FL metro) - prefer locality.
        n = ", ".join(x for x in (j.get("locality") or j.get("city"), j.get("principalSubdivision")) if x)
        if n:
            name = n
    except Exception as e:
        log("net_backup: reverse geocode failed: {}".format(e))
    _geo_cache[key] = name
    return name

def _geocode(place):
    """place -> (lat, lon, display_name) via keyless APIs; None on failure. Cached forever
    (places do not move). US 5-digit zips use zippopotam.us; names use Open-Meteo geocoding."""
    if place in _geo_cache:
        return _geo_cache[place]
    try:
        if re.fullmatch(r"\d{5}", place):
            j = requests.get("https://api.zippopotam.us/us/" + place, timeout=NET_TIMEOUT_S).json()
            p = j["places"][0]
            out = (float(p["latitude"]), float(p["longitude"]),
                   "{}, {} {}".format(p["place name"], p["state abbreviation"], place))
        else:
            j = requests.get("https://geocoding-api.open-meteo.com/v1/search",
                             params={"name": place, "count": 1}, timeout=NET_TIMEOUT_S).json()
            r = (j.get("results") or [None])[0]
            if not r:
                return None
            out = (r["latitude"], r["longitude"],
                   ", ".join(x for x in (r.get("name"), r.get("admin1"), r.get("country_code")) if x))
        _geo_cache[place] = out
        return out
    except Exception as e:
        log("net_backup: geocode '{}' failed: {}".format(place, e))
        return None

def live_weather_context(query, sender=None):
    """v8: real current conditions + today's outlook from the internet, when (a) the feature is
    on, (b) the query looks weather/live-shaped, (c) the box is actually online. Location:
    explicit place in the query, else the asking node's GPS, else the bridge node's own GPS,
    else NET_DEFAULT_PLACE. Returns '' in every other case so offline behavior matches v7."""
    if not NET_BACKUP or not _LIVE_WX_RE.search(query):
        return ""
    place = _extract_place(query)
    lat = lon = name = None
    if not place:
        pos = _node_latlon(sender) or _node_latlon("!%08x" % my_num if my_num else None)
        if pos:
            lat, lon = pos
        elif NET_DEFAULT_PLACE:
            place = NET_DEFAULT_PLACE
        else:
            return ""
    if not _net_online():
        return ""
    if place:
        loc = _geocode(place)
        if not loc:
            return ""
        lat, lon, name = loc
    else:
        name = _revgeo(lat, lon)
    wx_key = (round(lat, 2), round(lon, 2))   # coord-keyed: same-named places can never collide
    cached = _wx_cache.get(wx_key)
    if cached and time.time() - cached[0] < NET_CACHE_TTL_S:
        return cached[1]
    try:
        t0 = time.time()
        j = requests.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": lat, "longitude": lon,
            "current": "temperature_2m,apparent_temperature,relative_humidity_2m,"
                       "weather_code,wind_speed_10m,wind_gusts_10m,precipitation",
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max",
            "forecast_days": 1, "timezone": "auto",
            "temperature_unit": "fahrenheit", "wind_speed_unit": "mph"},
            timeout=NET_TIMEOUT_S).json()
        c, d = j["current"], j["daily"]
        line = ("Current verified weather for {} (fetched seconds ago via the bridge's live data "
                "link; answer with these numbers directly and do NOT add disclaimers about "
                "internet or data access): "
                "{:.0f}F (feels like {:.0f}F), {}, humidity {:.0f}%, wind {:.0f} mph (gusts {:.0f}). "
                "Today: high {:.0f}F, low {:.0f}F, rain chance {:.0f}%.".format(
                    name, c["temperature_2m"], c["apparent_temperature"],
                    _WMO.get(c.get("weather_code"), "conditions n/a"), c["relative_humidity_2m"],
                    c["wind_speed_10m"], c["wind_gusts_10m"],
                    d["temperature_2m_max"][0], d["temperature_2m_min"][0],
                    d["precipitation_probability_max"][0] or 0))
        _wx_cache[wx_key] = (time.time(), line)
        log("net_backup: live weather for {} in {:.1f}s".format(name, time.time() - t0))
        return line
    except Exception as e:
        log("net_backup: forecast fetch for '{}' failed: {}".format(place, e))
        return ""

_LIVE_SEARCH_RE = re.compile(r"\b(news|latest|current(ly)?|today|tonight|breaking|headline"
                             r"|price|stock|crypto|bitcoin|market|score|game|won|election"
                             r"|update on|status of|traffic|open (now|today)|hours|schedule"
                             r"|recall|outage|closure)\b", re.I)
_search_cache = {}

def live_search_context(query):
    """v8: general live-data backup - web metasearch via the local SearXNG instance, when online.
    An explicit 'search ...' / 'look up ...' always searches; otherwise only live-data-shaped
    queries do. Returns '' offline or on no-match so offline behavior is unchanged."""
    if not NET_BACKUP or not SEARX_URL:
        return ""
    m = re.match(r"(?:search|look ?up|google)\s*(?:for|:)?\s+(.{3,})", query, re.I)
    if not m and not _LIVE_SEARCH_RE.search(query):
        return ""
    q = (m.group(1) if m else query).strip()
    key = q.lower()
    cached = _search_cache.get(key)
    if cached and time.time() - cached[0] < NET_CACHE_TTL_S:
        return cached[1]
    if not _net_online():
        return ""
    try:
        t0 = time.time()
        j = requests.get(SEARX_URL.rstrip("/") + "/search", params={"q": q, "format": "json"},
                         timeout=NET_TIMEOUT_S).json()
        bits = []
        for a in (j.get("answers") or [])[:2]:   # engine answer boxes, when present
            bits.append(a.get("answer") if isinstance(a, dict) else str(a))
        for r in (j.get("results") or [])[:SEARCH_RESULTS]:
            t, c = r.get("title", ""), re.sub(r"\s+", " ", r.get("content") or "").strip()
            if t or c:
                bits.append("{}: {}".format(t, c[:200]) if c else t)
        bits = [b for b in bits if b]
        if not bits:
            return ""
        line = ("Live web search results for '" + q + "' (fetched seconds ago via the bridge's "
                "live data link; answer from these snippets and do NOT add disclaimers about "
                "internet or data access): " + " | ".join(bits))[:1500]
        _search_cache[key] = (time.time(), line)
        log("net_backup: search '{}' -> {} snippets in {:.1f}s".format(q[:40], len(bits), time.time() - t0))
        return line
    except Exception as e:
        log("net_backup: search '{}' failed: {}".format(q[:40], e))
        return ""

def add_msg(sender, role, content):
    with db() as c:
        c.execute("INSERT INTO messages(sender, role, content, ts) VALUES(?,?,?,?)", (sender, role, content, time.time()))

def get_history(sender):
    with db() as c:
        rows = c.execute("SELECT role, content FROM messages WHERE sender=? ORDER BY id DESC LIMIT ?",
                         (sender, HISTORY_TURNS * 2)).fetchall()
    return [{"role": r, "content": t} for r, t in reversed(rows)]

def add_fact(sender, content):
    with db() as c:
        c.execute("INSERT INTO facts(sender, content, ts) VALUES(?,?,?)", (sender, content, time.time()))

def get_facts():
    with db() as c:
        rows = c.execute("SELECT sender, content FROM facts ORDER BY id DESC LIMIT ?", (FACTS_MAX,)).fetchall()
    return list(reversed(rows))

def build_messages(sender, query, is_dm=False):
    sys_parts = [SYSPROMPT]
    # Authoritative per-path size budget (overrides the generic prompt limit): the chunker
    # truncates at this many bytes, so anything past it is lost airtime.
    sys_parts.append("HARD LIMIT for this reply: {} characters. This overrides any other length "
                     "guidance. If the full answer will not fit, give only the most critical "
                     "facts and stop cleanly.".format(CHUNK * (MAX_CHUNKS_DM if is_dm else MAX_CHUNKS) - 40))
    lib = library_context(query)
    if lib:
        sys_parts.append("Offline library context (prefer this; cite the book briefly):")
        sys_parts.append(lib)
    wx = weather_context()   # v7: real current conditions from mesh sensor nodes (offline)
    if wx:
        sys_parts.append(wx)
    # v8: internet-backed live data, only when online - structured weather plus general web
    # search; a query like "news about the hurricane" legitimately gets both.
    lw = " ".join(x for x in (live_weather_context(query, sender), live_search_context(query)) if x)
    facts = get_facts()
    if facts:
        sys_parts.append("Known facts (remembered from prior conversations):")
        sys_parts += ["- [{}] {}".format(s, t) for s, t in facts]
    msgs = [{"role": "system", "content": "\n".join(sys_parts)}]
    msgs += get_history(sender)
    # v8: the live-data line rides WITH the user turn, not the system prompt. A sender whose
    # recent history is full of honest "no live data" replies otherwise pattern-matches their
    # own past answers over a system-prompt fact (observed: qwen3-30b parroted old refusals).
    if lw:
        msgs.append({"role": "user", "content":
                     "[{} Any earlier replies claiming no live data are outdated.]\n{}".format(lw, query)})
    else:
        msgs.append({"role": "user", "content": query})
    return msgs

# ---------- offline library retrieval (v6/2b-i: qdrant semantic search, replaces cross-encoder) ----------
_book_ids = {}

def load_books():
    global _book_ids
    try:
        x = requests.get(KIWIX_URL + "/catalog/v2/entries", timeout=10).text
        for m in re.finditer(r"<entry>(.*?)</entry>", x, re.S):
            e = m.group(1)
            n = re.search(r"<name>([^<]+)</name>", e)
            i = re.search(r"<id>(?:urn:uuid:)?([^<]+)</id>", e)
            if n and i:
                _book_ids[n.group(1)] = i.group(1)
        log("library: {} books available".format(len(_book_ids)))
    except Exception as e:
        log("library catalog unavailable: {}".format(e))

def embed_query(q):
    """Ollama embedding for the query. Returns list[float] or None on any failure (logged)."""
    try:
        r = requests.post(EMBED_URL + "/api/embeddings",
                          json={"model": EMBED_MODEL, "prompt": q}, timeout=EMBED_TIMEOUT_S)
        r.raise_for_status()
        v = r.json().get("embedding")
        if not isinstance(v, list) or not v:
            log("embed: malformed response (no embedding), falling back")
            return None
        return v
    except Exception as e:
        log("embed unavailable ({}): {}".format(type(e).__name__, e))
        return None

def qdrant_search(vector, limit):
    """v13: search every collection in QDRANT_COLLECTIONS and merge score-desc (all
    collections share the nomic-embed-text 768/cosine space, so scores are comparable).
    One down collection must not blind the others: per-collection failures are logged
    and skipped; None (-> Kiwix fallback) only when EVERY collection fails. Hit-shape
    guarantees are _qdrant_search_one's, unchanged."""
    merged, failures = [], 0
    for coll in QDRANT_COLLECTIONS:
        hits = _qdrant_search_one(vector, limit, coll)
        if hits is None:
            failures += 1
            continue
        merged.extend(hits)
    if failures == len(QDRANT_COLLECTIONS):
        return None
    merged.sort(key=lambda h: h["score"], reverse=True)
    return merged[:limit]

def _qdrant_search_one(vector, limit, collection):
    """qdrant vector search of ONE collection. Returns list of {score, payload} (score-desc)
    or None on failure (logged). A returned hit is guaranteed to have a numeric (non-bool)
    `score` and a dict `payload` whose `text` field, if present, is a string — so callers can
    safely subscript hit["score"]/hit["payload"] and read payload["text"]. It does NOT
    guarantee every other payload field is well-typed; the assembly loop in library_context()
    still coerces defensively. Structurally-invalid hits are dropped and counted; a non-list
    `result` or a transport failure returns None so callers fall back."""
    try:
        r = requests.post("{}/collections/{}/points/search".format(QDRANT_URL, collection),
                          json={"vector": vector, "limit": limit, "with_payload": True},
                          timeout=QDRANT_TIMEOUT_S)
        r.raise_for_status()
        raw = r.json().get("result", [])
        # B2: a 200 with a non-list `result` is a MALFORMED RESPONSE, not "qdrant unavailable" —
        # label it as such (returning None routes it to the same safe fallback either way).
        if not isinstance(raw, list):
            log("qdrant: malformed response (result not a list): {}".format(type(raw).__name__))
            return None
        out = []
        for h in raw:
            if not isinstance(h, dict):              # B2: skip non-dict elements before .get()
                continue
            s = h.get("score"); pl = h.get("payload")
            # F4: bool is an int subclass — exclude it so a stray True/False can't pass as a score.
            if not (isinstance(s, (int, float)) and not isinstance(s, bool) and isinstance(pl, dict)):
                continue
            txt = pl.get("text")
            if txt is not None and not isinstance(txt, str):  # B1 belt: non-str non-None text = schema bug -> drop
                continue
            out.append({"score": float(s), "payload": pl})
        dropped = len(raw) - len(out)
        if dropped:                                  # B3: distinguish all-dropped from a true-empty result
            log("qdrant {}: dropped {} malformed hit(s) of {}".format(collection, dropped, len(raw)))
        out.sort(key=lambda h: h["score"], reverse=True)  # F3: defensive re-sort (medical stakes)
        return out
    except Exception as e:
        log("qdrant {} unavailable ({}): {}".format(collection, type(e).__name__, e))
        return None

def _kiwix_fallback(query):
    """Degraded-path retrieval when embed/qdrant are unreachable: simple first-wins
    Kiwix search+fetch (the pre-2b-i behavior), NOT the cross-encoder pipeline. Only
    the FIRST hit per book is fetched; per-candidate try/except so one book's
    failure never aborts trying the next."""
    if not _book_ids:
        log("kiwix fallback: nothing (no book catalog)")
        return ""
    books = [b for b in LIBRARY_BOOKS if b in _book_ids] or list(_book_ids)
    for book in books[:LIBRARY_MAX_BOOKS]:
        try:
            xml = requests.get(KIWIX_URL + "/search", params={
                "books.id": _book_ids[book], "pattern": query,
                "pageLength": 3, "format": "xml"}, timeout=10).text
            m = re.search(r"<item>(.*?)</item>", xml, re.S)
            if not m:
                continue
            e = m.group(1)
            t = re.search(r"<title>(.*?)</title>", e, re.S)
            l = re.search(r"<link>(.*?)</link>", e, re.S)
            title = re.sub(r"<[^>]+>", "", t.group(1)).strip() if t else ""
            link = (l.group(1).strip().replace("&amp;", "&")) if l else ""
            if not link:
                continue
            art = requests.get(KIWIX_URL + link, timeout=10).text
            art = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", art, flags=re.S | re.I)
            art = re.sub(r"<[^>]+>", " ", art)
            art = re.sub(r"\s+", " ", art).strip()
            if len(art) > 200:
                log("kiwix fallback: injected from {}".format(book))
                return "From {} - {}: {}".format(book, title, art[:LIBRARY_CONTEXT_CHARS])
        except Exception as e:
            log("kiwix fallback: failed for {}: {}".format(book, e))
            continue
    log("kiwix fallback: nothing")
    return ""

def rerank_hits(query, hits):
    """v16: cross-encoder rerank via the TEI bge-reranker (:8091). Re-scores every
    candidate chunk against the actual query, reorders by relevance, and DROPS
    chunks below RERANK_MIN_SCORE — cosine retrieval returns look-alike noise
    (ROS articles for "which nodes are online?") that the cross-encoder scores at
    ~2e-5 vs 0.7+ for genuinely relevant text. Returns [] when EVERY candidate is
    noise (caller injects no context). Degrades to the input order on ANY failure
    (service down, timeout, malformed reply) — reranking must never cost a
    medical answer. Response is validated strictly: same length as the request,
    unique in-range int indices, numeric scores; anything else = malformed."""
    if not hits:
        return hits
    try:
        t0 = time.time()
        texts = [str((h.get("payload") or {}).get("text") or "") for h in hits]
        r = requests.post(RERANK_URL.rstrip("/") + "/rerank",
                          json={"query": query, "texts": texts, "truncate": True},
                          timeout=RERANK_TIMEOUT_S)
        r.raise_for_status()
        ranked = r.json()
        # Accept a SHORTER ranking (a TEI top-N config returns fewer entries than texts) —
        # rerank what came back rather than no-op'ing the feature. Empty or longer-than-input
        # is incoherent, though: treat it as malformed and degrade.
        if not isinstance(ranked, list) or not 0 < len(ranked) <= len(hits):
            raise ValueError("expected 1..{} entries, got {!r}".format(
                len(hits), len(ranked) if isinstance(ranked, list) else type(ranked).__name__))
        pairs, seen = [], set()
        for e in ranked:
            i, s = e["index"], e["score"]
            if not isinstance(i, int) or isinstance(i, bool) or not 0 <= i < len(hits) or i in seen:
                raise ValueError("bad index {!r}".format(i))
            if not isinstance(s, (int, float)) or isinstance(s, bool):
                raise ValueError("bad score {!r}".format(s))
            seen.add(i)
            pairs.append((s, i))
        pairs.sort(key=lambda p: -p[0])
        out = []
        for s, i in pairs:
            if s < RERANK_MIN_SCORE:
                break                  # sorted desc; everything past here is noise
            hits[i]["rerank"] = s
            out.append(hits[i])
        log("rerank: {} -> {} hits in {:.2f}s (top r={:.4f})".format(
            len(hits), len(out), time.time() - t0, out[0]["rerank"] if out else 0.0))
        return out
    except Exception as e:
        log("rerank unavailable ({}): {} — using qdrant order".format(type(e).__name__, e))
        return hits

def library_context(query):
    """Semantic retrieval: embed the query, vector-search the offline library, inject the best
    chunks. Falls back to Kiwix first-wins if embed/qdrant are down. Every no-context outcome is
    logged DISTINCTLY (embed-down / qdrant-down / zero-hits / below-floor / no-usable-text) so a
    missing medical answer is diagnosable. The assembly loop coerces every payload field with
    str() and is wrapped per-hit in try/except, so a malformed hit is skipped-and-logged and can
    NEVER raise out of here (which would be swallowed by handle_query's LLM-unreachable catch and
    silently drop a medical query)."""
    t0 = time.time()
    vec = embed_query(query)
    if vec is None:
        log("library_context: embed down -> kiwix fallback")
        return _kiwix_fallback(query)
    hits = qdrant_search(vec, QDRANT_TOP_K)
    if hits is None:
        log("library_context: qdrant down -> kiwix fallback")
        return _kiwix_fallback(query)
    if not hits:
        log("library_context: qdrant returned zero hits, no context injected ({:.2f}s)".format(time.time() - t0))
        return ""
    if hits[0]["score"] < QDRANT_MIN_SCORE:
        log("library_context: top score {:.3f} < floor {}, no context injected ({:.2f}s)".format(
            hits[0]["score"], QDRANT_MIN_SCORE, time.time() - t0))
        return ""
    if RERANK_ENABLED:
        # v16: cross-encoder rerank of the floor-passing candidates. [] = every
        # candidate was look-alike noise -> inject nothing (the LLM answers from
        # the system prompt alone, which is exactly right for status questions).
        kept = [h for h in hits if h["score"] >= QDRANT_MIN_SCORE]
        hits = rerank_hits(query, kept)
        if not hits:
            log("library_context: rerank dropped all {} candidate(s) as noise, no context injected ({:.2f}s)".format(
                len(kept), time.time() - t0))
            return ""
    parts, total, winners, too_short = [], 0, [], 0
    for h in hits:
        if h["score"] < QDRANT_MIN_SCORE:
            break                      # hits are score-desc; once below floor, stop
        try:
            pl = h["payload"]
            text = str(pl.get("text") or "").strip()      # B1 suspenders: coerce, never assume str
            if not text:
                continue
            if len(text) < MIN_CHUNK_CHARS:               # F1: a tiny fragment is not usable medical context
                too_short += 1
                continue
            text = text[:LIBRARY_CONTEXT_CHARS]           # F5: single-chunk truncation backstop
            title = str(pl.get("article_title") or pl.get("source") or "?")
            section = str(pl.get("section_title") or "")
            entry = "From {}{}: {}".format(title, " - " + section if section else "", text)
        except Exception as e:
            log("library_context: skipping malformed hit ({}): {}".format(type(e).__name__, e))
            continue
        if total + len(entry) > LIBRARY_CONTEXT_CHARS and parts:
            break                      # budget full
        parts.append(entry)
        total += len(entry)
        # rerank scores span 0.7 down to the 0.001 floor — 4dp, or a kept marginal chunk
        # logs as "/r0.00" and reads like noise that should have been dropped.
        winners.append("{}({:.2f}{})".format(
            title[:40], h["score"], "/r{:.4f}".format(h["rerank"]) if "rerank" in h else ""))
    if too_short:
        log("library_context: skipped {} too-short chunk(s) (< {}c)".format(too_short, MIN_CHUNK_CHARS))
    if not parts:
        # F2: top hit cleared the floor but every hit was empty/too-short/malformed. Use the same
        # "no context injected" vocabulary as the other no-context paths so it groups in logs.
        log("library_context: hits cleared floor but no usable text, no context injected ({:.2f}s)".format(
            time.time() - t0))
        return ""
    log("library_context: {} chunks, {}c, {:.2f}s, winners={}".format(
        len(parts), total, time.time() - t0, winners))   # audit trail — every answered query
    return "\n".join(parts)

# ---------- llm ----------
def ask_llm(messages):
    r = requests.post(LLM_BASE + "/chat/completions", json={
        "model": MODEL, "max_tokens": MAX_TOKENS, "messages": messages}, timeout=TIMEOUT)
    r.raise_for_status()
    out = r.json()["choices"][0]["message"]["content"]
    out = re.sub(r"<think>.*?</think>", "", out, flags=re.S).strip()
    return re.sub(r"\s+", " ", out)

def allowed_now(node):
    now = time.time()
    with lock:
        while recent and now - recent[0] > 60:
            recent.popleft()
        if len(recent) >= PER_MIN:
            return False
        if now - last_by_node.get(node, 0) < COOLDOWN:
            return False
        recent.append(now)
        last_by_node[node] = now
        return True

def dashboard_allowed_now(key="dashboard"):
    """v6/A5: dashboard send-API rate bucket. Mirrors allowed_now()'s window/cooldown
    logic but on its OWN deque/map so a busy dashboard can never drain the mesh @ai
    bucket (recent/last_by_node) and starve real mesh replies."""
    now = time.time()
    with lock:
        while _dash_recent and now - _dash_recent[0] > 60:
            _dash_recent.popleft()
        if len(_dash_recent) >= SEND_PER_MIN:
            return False
        if now - _dash_last.get(key, 0) < SEND_COOLDOWN_S:
            return False
        _dash_recent.append(now)
        _dash_last[key] = now
        return True

def _byte_prefix(s, max_bytes):
    """Longest prefix of s that fits in max_bytes UTF-8 bytes, without splitting a character.
    CHUNK is a *byte* budget (LoRa payload limit) but slicing by character can overflow it on
    any multibyte content (a degree sign, an accent), so all trimming goes through here."""
    b = s.encode()
    if len(b) <= max_bytes:
        return s
    return b[:max_bytes].decode("utf-8", "ignore")

def chunk_reply(text, max_chunks=None):
    """Split text into <=max_chunks LoRa-sized pieces, preferring sentence boundaries.
    Returns (chunks, remainder): remainder is the untransmitted tail ('' if it all fit) —
    the caller banks it for '@ai more' instead of amputating it."""
    mc = max_chunks or MAX_CHUNKS
    if not text:
        return [], ""
    chunks = []
    while text and len(chunks) < mc:
        if len(text.encode()) <= CHUNK:
            chunks.append(text)
            text = ""
            break
        cut = _byte_prefix(text, CHUNK)                 # byte-bounded, not char-bounded
        m = re.search(r"^.*[.!?]\s", cut, re.S)
        piece = (m.group(0) if m and len(m.group(0)) > CHUNK // 3 else cut).rstrip() or cut
        chunks.append(piece)
        text = text[len(piece):].lstrip()
    return chunks, text

def mark_more(chunks):
    """Stamp the last chunk with the continuation hint, keeping it inside the byte budget.
    Returns any text the stamp displaced so the caller banks it with the remainder —
    otherwise a near-full final chunk would silently lose up to 7 bytes."""
    if not chunks:
        return ""
    last = chunks[-1]
    kept = _byte_prefix(last, CHUNK - 7).rstrip()
    chunks[-1] = kept + " [more]"
    return last[len(kept):].strip()

# ---------- dashboard send API (v6) ----------
# Token-gated HTTP endpoint so the dashboard can transmit THROUGH the bridge
# (bridge is the single radio owner). Port is NOT published to the LAN -
# reachable only on the Docker network. Empty SEND_TOKEN disables the API.
def ack_state_for(error_reason, from_num, dest_num, my_num, is_dm):
    """Map a ROUTING_APP packet's fields to an ack_state token. Pure — no I/O.
    error_reason: routing.errorReason; None/""/"NONE" == success (live probe
    2026-07-12 showed success ACKs carry the STRING "NONE" on this firmware).
    DM: success from the destination = 'ack' (end-to-end); from our own node =
    'radio-accepted' (transmit-level only); from any other node (an intermediate
    relayer) = 'relayed' (progressed into the mesh, not delivered). Broadcast
    success = 'relayed' (a neighbor rebroadcast, NOT delivery). ACKs are
    unauthenticated RF: display-only, never automation."""
    if error_reason and str(error_reason).upper() not in ("NONE", ""):
        return "failed:{}".format(error_reason)
    if not is_dm:
        return "relayed"
    if dest_num is not None and from_num == dest_num:
        return "ack"
    if my_num is not None and from_num == my_num:
        return "radio-accepted"
    return "relayed"

# Delivery-state ranking: a ROUTING packet may only UPGRADE a row (a multi-hop
# DM's local/relay ack arrives BEFORE the destination's end-to-end ack — first-
# write-wins would lock the weaker state and orphan the definitive one).
# 'ack' and 'failed:*' are terminal.
_ACK_RANK = {None: 0, "radio-accepted": 1, "relayed": 2, "ack": 3}

def ack_rank(state):
    if state and str(state).startswith("failed"):
        return 3
    return _ACK_RANK.get(state, 0)

# Display-only health counters. Mutated with += from multiple threads without a
# lock: CPython += can drop an increment under contention, which is acceptable
# here — they are monotonic diagnostics that drive no automation.
sends_without_id = 0
acks_seen = acks_matched = ack_orphans = ack_db_errors = 0
last_ack_ts = 0.0
_ack_confirmed = False   # one-shot loud confirmation ACK tracking works on this mesh

def _send_tapback(interface, text, reply_id, destinationId=None, channelIndex=0):
    """Send an emoji tapback. meshtastic 2.7.10 has NO public API for the Data
    `emoji` flag (sendData sets reply_id but never emoji — verified 2026-07-13),
    so this replicates sendData's packet assembly exactly, plus emoji=1.
    Returns the sent packet (id populated) — same contract _send_and_log expects."""
    pkt = mesh_pb2.MeshPacket()
    pkt.channel = channelIndex
    pkt.decoded.payload = text.encode("utf-8")
    pkt.decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
    pkt.decoded.want_response = False
    pkt.id = interface._generatePacketId()
    pkt.decoded.reply_id = reply_id
    pkt.decoded.emoji = 1
    pkt.priority = mesh_pb2.MeshPacket.Priority.RELIABLE
    return interface._sendPacket(pkt, destinationId=destinationId or "^all", wantAck=True)

def _send_and_log(send_fn, node_id, node_name, ch, is_dm, is_ai, text, reply_to_id=None, is_reaction=None):
    """Three phases that must not cross-contaminate: (1) radio send — a raise
    here is a REAL send failure, recorded as a 'failed' row and re-raised;
    (2) id extraction — can NEVER turn a successful send into a failure; a
    missing id means a permanently glyphless row, counted; (3) log —
    log_traffic never raises. Deliberate tradeoff: send-then-log means a crash
    in the ms between TX and log loses the row; the inverse (log-then-send)
    records phantom sends that never hit RF — the worse lie on a life-safety
    mesh (operator believes help was called)."""
    global sends_without_id
    try:
        pkt = send_fn()
    except Exception:
        log_traffic("out", node_id, node_name, ch, is_dm, is_ai, text, mesh_id=None, ack_state="failed",
                    reply_to_id=reply_to_id, is_reaction=is_reaction)
        raise
    # protobuf scalar .id defaults to 0 (never None/AttributeError) — 0 == no id.
    mesh_id = getattr(pkt, "id", 0) or None
    if mesh_id is None:
        sends_without_id += 1
        log("send returned no packet id — row will stay glyphless")
    log_traffic("out", node_id, node_name, ch, is_dm, is_ai, text, mesh_id=mesh_id, ack_state=None,
                reply_to_id=reply_to_id, is_reaction=is_reaction)
    return pkt

def _validate_reply_fields(data, text):
    """Validate the optional reply/react send fields. Returns (error, reply_id, react).
    reply_id: mesh packet id being replied/reacted to, 1..0xFFFFFFFF.
    react: emoji tapback — requires reply_id, text capped at 8 bytes (an emoji)."""
    reply_id = data.get("reply_id")
    react = data.get("react", False)
    if reply_id is not None:
        if isinstance(reply_id, bool) or not isinstance(reply_id, int) or not (1 <= reply_id <= 0xFFFFFFFF):
            return "reply_id must be an integer 1..4294967295", None, False
    if react is not False and react is not True:
        return "react must be a boolean", None, False
    if react and reply_id is None:
        return "react requires reply_id", None, False
    if react and len(text.encode()) > 8:
        return "a reaction is a single emoji (max 8 bytes)", None, False
    return None, reply_id, react

class SendHandler(BaseHTTPRequestHandler):
    timeout = 10  # v6/A4: cap slowloris-style connections that dribble/withhold bytes

    def _reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # our log() covers sends; suppress per-request noise

    def do_GET(self):
        if self.path != "/api/health":
            return self._reply(404, {"error": "not found"})
        node, node_info_ok = None, True
        try:
            node = ((iface.getMyNodeInfo() or {}).get("user") or {}).get("longName")
        except Exception as e:
            node_info_ok = False
            log("health check: getMyNodeInfo failed: {}".format(e))
        self._reply(200, {"ok": iface is not None and node_info_ok, "node": node,
                          "api": send_api_alive, "queue_depth": work_q.qsize(), "worker": worker_alive,
                          "worker_idle_s": round(time.time() - last_progress_ts, 1),
                          "acks_seen": acks_seen, "acks_matched": acks_matched,
                          "ack_orphans": ack_orphans, "ack_db_errors": ack_db_errors,
                          "sends_without_id": sends_without_id,
                          "last_ack_ts": last_ack_ts or None,
                          "mm_online": _mm_online_cache["count"],
                          "mm_online_ts": _mm_online_cache["ts"] or None})

    def do_POST(self):
        if self.path == "/api/traceroute":
            return self._traceroute()
        if self.path != "/api/send":
            return self._reply(404, {"error": "not found"})
        if not SEND_TOKEN or not hmac.compare_digest(self.headers.get("X-Send-Token") or "", SEND_TOKEN):
            return self._reply(401, {"error": "unauthorized"})
        try:
            n = int(self.headers.get("Content-Length", "0"))
            if n < 0 or n > 4096:
                return self._reply(400, {"error": "bad content-length"})
            data = json.loads(self.rfile.read(n) or b"{}")
            if not isinstance(data, dict):
                return self._reply(400, {"error": "body must be a JSON object"})
        except Exception as e:
            log("sendapi bad request: {}".format(e))
            return self._reply(400, {"error": "invalid JSON"})
        text = data.get("text")
        if not isinstance(text, str) or not text.strip():
            return self._reply(400, {"error": "text required"})
        text = text.strip()
        if len(text.encode()) > CHUNK:
            return self._reply(400, {"error": "text over {} bytes".format(CHUNK)})
        to = data.get("to")
        ch = data.get("channel", 0)
        if to is not None and (not isinstance(to, str) or not re.fullmatch(r"![0-9a-fA-F]{8}", to)):
            return self._reply(400, {"error": "bad destination"})
        if not isinstance(ch, int) or (to is None and ch not in ALLOWED):
            return self._reply(400, {"error": "channel not allowed"})
        err, reply_id, react = _validate_reply_fields(data, text)
        if err:
            return self._reply(400, {"error": err})
        if not dashboard_allowed_now():
            return self._reply(429, {"error": "rate limited"})
        if iface is None:
            return self._reply(503, {"error": "radio not connected"})
        try:
            # 5a: wantAck=True is the OWNED TX change — operator sends now request a
            # delivery ACK (destination transmits an ack; firmware retransmits <=3x).
            if react:
                if to:
                    pkt = _send_and_log(lambda: _send_tapback(iface, text, reply_id, destinationId=to),
                                        to, node_display(iface, to), ch, True, False, text,
                                        reply_to_id=reply_id, is_reaction=1)
                else:
                    pkt = _send_and_log(lambda: _send_tapback(iface, text, reply_id, channelIndex=ch),
                                        "dashboard", "Dashboard", ch, False, False, text,
                                        reply_to_id=reply_id, is_reaction=1)
            elif to:
                pkt = _send_and_log(lambda: iface.sendText(text, destinationId=to, wantAck=True, replyId=reply_id),
                                    to, node_display(iface, to), ch, True, False, text, reply_to_id=reply_id)
            else:
                pkt = _send_and_log(lambda: iface.sendText(text, channelIndex=ch, wantAck=True, replyId=reply_id),
                                    "dashboard", "Dashboard", ch, False, False, text, reply_to_id=reply_id)
        except Exception as e:
            log("sendapi radio send failed: {}".format(e))
            return self._reply(502, {"error": "radio send failed"})
        log("sendapi TX {} {}B id={}: {}".format("dm " + to if to else "ch{}".format(ch),
            len(text.encode()), getattr(pkt, "id", None), repr(text)))
        self._reply(200, {"ok": True})

    def _traceroute(self):
        """v17: fire a route-discovery probe. NON-BLOCKING by design — sendData(wantResponse)
        returns immediately and on_traceroute (Task 4) fills the row in when (if) the mesh
        answers. NEVER call `interface.sendTraceRoute()` here — the deployed meshtastic lib
        blocks internally (waitForTraceRoute), prints to stdout, and raises MeshInterfaceError
        on timeout."""
        if not SEND_TOKEN or not hmac.compare_digest(self.headers.get("X-Send-Token") or "", SEND_TOKEN):
            return self._reply(401, {"error": "unauthorized"})
        try:
            n = int(self.headers.get("Content-Length", "0"))
            if n < 0 or n > 1024:
                return self._reply(400, {"error": "bad content-length"})
            data = json.loads(self.rfile.read(n) or b"{}")
            if not isinstance(data, dict):
                return self._reply(400, {"error": "body must be a JSON object"})
        except Exception as e:
            log("traceroute api bad request: {}".format(e))
            return self._reply(400, {"error": "invalid JSON"})
        to = data.get("to")
        if not isinstance(to, str) or not re.fullmatch(r"![0-9a-fA-F]{8}", to):
            return self._reply(400, {"error": "bad destination"})
        hop_limit = data.get("hop_limit", 4)
        if not isinstance(hop_limit, int) or isinstance(hop_limit, bool) or not 1 <= hop_limit <= 7:
            return self._reply(400, {"error": "hop_limit must be 1..7"})
        allowed, retry_after = traceroute_allowed_now()
        if not allowed:
            return self._reply(429, {"error": "traceroute cooling down (radio rate limit)",
                                     "retry_after": retry_after})
        if iface is None:
            traceroute_release()   # never reached the air — don't burn the firmware's rate limit
            return self._reply(503, {"error": "radio not connected"})
        try:
            pkt = iface.sendData(mesh_pb2.RouteDiscovery(), destinationId=to,
                                 portNum=portnums_pb2.PortNum.TRACEROUTE_APP,
                                 wantResponse=True, channelIndex=0, hopLimit=hop_limit)
        except Exception as e:
            log("traceroute radio send failed: {}".format(e))
            traceroute_release()   # send never transmitted — give the slot back
            return self._reply(502, {"error": "radio send failed"})
        # protobuf scalar .id defaults to 0 (never None) — 0 == no id (see _send_and_log).
        req_id = getattr(pkt, "id", 0) or None
        with db() as c:
            c.execute("UPDATE traceroutes SET status='timeout' WHERE status='pending' AND ts < ?",
                      (time.time() - TRACEROUTE_TTL_S,))
            cur = c.execute("INSERT INTO traceroutes(ts, dest, dest_name, request_id, hop_limit, status) "
                            "VALUES(?,?,?,?,?,'pending')",
                            (time.time(), to, node_display(iface, to), req_id, hop_limit))
            row_id = cur.lastrowid
        log("traceroute api TX to {} hop_limit={} request_id={}".format(to, hop_limit, req_id))
        return self._reply(200, {"ok": True, "id": row_id, "request_id": req_id})

def start_send_api():
    global send_api_alive
    if not SEND_TOKEN:
        log("send API disabled (SEND_TOKEN not set)")
        return
    srv = ThreadingHTTPServer(("0.0.0.0", SEND_PORT), SendHandler)

    def _serve():
        global send_api_alive
        try:
            srv.serve_forever()
        except Exception as e:
            send_api_alive = False
            log("CRITICAL: send API server thread died: {}".format(e))

    send_api_alive = True
    threading.Thread(target=_serve, daemon=True).start()
    log("send API listening on :{}".format(SEND_PORT))

# ---------- query queue (v6/2b-ii: bounded no-drop FIFO in front of handle_query) ----------
def _is_duplicate(pkt_id):
    """True if this mesh packet id was seen within DEDUP_TTL_S (a radio retransmit)."""
    if pkt_id is None:
        return False
    now = time.time()
    with lock:
        while _seen_ids and now - next(iter(_seen_ids.values())) > DEDUP_TTL_S:
            _seen_ids.popitem(last=False)
        if pkt_id in _seen_ids:
            return True
        _seen_ids[pkt_id] = now
        return False

def _worker():
    """Single FIFO consumer. Never dies on a handle_query error (logs + continues) — same
    liveness lesson as the send-API thread. handle_query itself drops on LLM-unreachable (the
    doc's rule); the worker just keeps draining. Each stage (get / unpack / handle_query) is
    isolated so a failure logs its own context; a malformed queue item is logged CRITICAL, not
    silently dropped. If the loop EVER exits (even on a non-Exception throwable), worker_alive
    flips False and the death is logged loud so /api/health surfaces a wedged queue."""
    global worker_alive, last_progress_ts
    worker_alive = True
    try:
        while True:
            try:
                item = work_q.get()
            except Exception as e:
                log("CRITICAL: work_q.get() failed: {}".format(e)); continue
            try:
                sender, ch, query, send, is_dm = item
            except Exception as e:
                log("CRITICAL: malformed queue item dropped (unpack failed): {} raw={!r}".format(e, item))
                try: work_q.task_done()
                except Exception: pass
                continue
            try:
                handle_query(sender, ch, query, send, is_dm)
            except Exception as e:
                log("worker error handling query from {} (continuing): {}".format(sender, e))
            finally:
                last_progress_ts = time.time()   # F7: mark forward progress even on a handled error
                try: work_q.task_done()
                except Exception as e2:
                    log("worker task_done error: {}".format(e2))   # B3: log, never silently pass
    except BaseException as e:
        worker_alive = False
        log("CRITICAL: query worker thread died — queue will no longer drain: {}".format(e))

def start_worker():
    global last_progress_ts
    last_progress_ts = time.time()   # F7: baseline so worker_idle_s is meaningful from the start
    threading.Thread(target=_worker, daemon=True).start()
    log("query worker started (queue max {}, no-drop hold policy)".format(QUEUE_MAX))

# ---------- handlers ----------
iface = None
my_num = None

def handle_query(sender, ch, query, send, is_dm=False):
    if query.lower() in ("more", "continue"):
        # Continuation pull: deliver the next installment of this sender's banked overflow.
        # No LLM call — this is a deterministic read of what was already generated.
        rest = _pending.pop(sender, "")
        if not rest:
            send("Nothing more pending.")
            return
        mc = MAX_CHUNKS_DM if is_dm else MAX_CHUNKS
        chunks, rest2 = chunk_reply(rest, mc)
        if rest2:
            _pending[sender] = (mark_more(chunks) + " " + rest2).strip()
        _send_chunks(chunks, send)
        return
    m = re.match(r"^(remember|forget)\s+(.+)$", query, re.I)
    if m:
        if sender.lower() not in ADMIN_NODES:
            send("Sorry, only my operator can teach me facts.")
            log("DENIED memory write from non-admin {}: {}".format(sender, repr(query)))
            return
        verb, arg = m.group(1).lower(), m.group(2).strip()
        if verb == "remember":
            add_fact(sender, arg)
            send(("Noted: " + arg)[:CHUNK])
            log("fact from {}: {}".format(sender, repr(arg)))
        else:
            with db() as c:
                n = c.execute("DELETE FROM facts WHERE content LIKE ?", ("%" + arg + "%",)).rowcount
            send("Forgot {} fact(s) matching: {}".format(n, arg)[:CHUNK])
            log("forget by {}: {} ({} rows)".format(sender, repr(arg), n))
        return
    try:
        reply = ask_llm(build_messages(sender, query, is_dm))
    except Exception as e:
        log("LLM unreachable, dropping message: {}".format(e))
        return
    mc = MAX_CHUNKS_DM if is_dm else MAX_CHUNKS
    chunks, rest = chunk_reply(reply, mc)
    if rest:
        # Overflow is banked, not discarded: the reply ends '[more]' and '@ai more' pulls the
        # next installment. (A model-side compression pass was tried and reverted — the LLM
        # doesn't reliably obey character limits and sometimes rewrote LONGER.)
        _pending[sender] = (mark_more(chunks) + " " + rest).strip()
    add_msg(sender, "user", query)
    add_msg(sender, "assistant", reply)
    _send_chunks(chunks, send)

def _send_chunks(chunks, send):
    for i, c in enumerate(chunks):
        send(c)
        log("reply {} ({}B): {}".format(i + 1, len(c.encode()), repr(c)))
        time.sleep(CHUNK_DELAY_S)

RADIO_CHECKS = {"ping": "pong", "test": "test OK"}
# Channel radio checks answer publicly (threaded to the ping) but per-sender
# cooldown-limited: the bridge is one of several auto-responders on ch0, and an
# unlimited public pong invites flood abuse. DMs at the bridge are exempt — a
# deliberate check of THIS node should always answer (dedup still guards RF
# retransmits). Set RADIO_CHECK_CHANNEL=0 to go DM-only if the mesh complains.
RADIO_CHECK_CHANNEL = os.environ.get("RADIO_CHECK_CHANNEL", "1").lower() in ("1", "true", "yes")
RADIO_CHECK_COOLDOWN_S = int(os.environ.get("RADIO_CHECK_COOLDOWN_S", "120"))
# v15: pong reports nodes heard within this window (2h = the dashboard's "online"
# convention, so the pong and Meridian agree) instead of total nodedb size.
ONLINE_WINDOW_S = int(os.environ.get("ONLINE_WINDOW_S", "7200"))
# v18: MeshMonitor's online count beats the bridge's own (see module docstring).
# Reachable by container name on project-nomad_default (e.g. http://meshmonitor-eval:3001).
MESHMONITOR_API_URL = os.environ.get("MESHMONITOR_API_URL", "").rstrip("/")
MM_ONLINE_TTL_S = int(os.environ.get("MM_ONLINE_TTL_S", "60"))
_mm_online_cache = {"count": None, "ts": 0.0}   # radio thread only
_rc_last = {}   # sender -> last channel-pong ts (radio thread only)

# v17: mesh traceroute. Firmware rate-limits route discovery (~30s); one GLOBAL
# 35s gate — a second concurrent trace would be refused by the radio anyway.
TRACEROUTE_COOLDOWN_S = int(os.environ.get("TRACEROUTE_COOLDOWN_S", "35"))
TRACEROUTE_TTL_S = int(os.environ.get("TRACEROUTE_TTL_S", "120"))   # pending older than this = timeout
_tr_last = {}

def traceroute_allowed_now():
    """Thread-safe global gate for route discovery (the send API is a
    ThreadingHTTPServer: one thread per POST, so an unlocked check-then-act
    would let two concurrent probes both pass). Returns (allowed, retry_after):
    retry_after is the ACTUAL seconds remaining, not the constant."""
    now = time.time()
    with lock:
        last = _tr_last.get("global", 0)
        remaining = TRACEROUTE_COOLDOWN_S - (now - last)
        if remaining > 0:
            return False, int(math.ceil(remaining))
        _tr_last["global"] = now
        return True, 0

def traceroute_release():
    """Give the cooldown slot back when a probe never reached the air (radio
    down, send raised). The cooldown mirrors the FIRMWARE's rate limit, so a
    transmission that never happened must not consume it."""
    with lock:
        _tr_last.pop("global", None)


def count_online(nodes, now, window_s=None):
    """Nodes heard within the online window, from a meshtastic interface.nodes
    dict. Returns None when NO entry carries a usable lastHeard (empty nodedb,
    firmware without the field) so the caller falls back to total-known instead
    of reporting a false 0 — the pinger we just heard is plainly online."""
    if window_s is None:
        window_s = ONLINE_WINDOW_S
    n, seen = 0, False
    for v in (nodes or {}).values():
        lh = (v or {}).get("lastHeard") if isinstance(v, dict) else None
        if isinstance(lh, (int, float)) and not isinstance(lh, bool):
            seen = True
            if now - lh <= window_s:
                n += 1
    return n if seen else None

def mm_online_count(now, url, window_s, cache, ttl_s, fetch):
    """Nodes MeshMonitor heard within window_s, or None (URL unset, fetch failed,
    or garbage payload -- the caller falls back to the bridge's own count).
    Failures are cached exactly like successes so a down MeshMonitor costs one
    short-timeout attempt per TTL, never one per ping: the pong path must stay
    effectively instant. fetch is injected (url -> parsed JSON) for testability."""
    if not url:
        return None
    if now - cache["ts"] < ttl_s:
        return cache["count"]
    count = None
    try:
        raw = fetch(url + "/api/nodes")
        raw = raw if isinstance(raw, list) else raw.get("nodes", [])
        if not isinstance(raw, list):
            raise ValueError("nodes payload is not a list")
        n, seen = 0, False
        for v in raw:
            lh = v.get("lastHeard") if isinstance(v, dict) else None
            if isinstance(lh, (int, float)) and not isinstance(lh, bool):
                seen = True
                if now - lh <= window_s:
                    n += 1
        count = n if seen else None
    except Exception:
        count = None
    cache["ts"] = now
    cache["count"] = count
    return count

def live_online_count(nodes, now):
    """The pong's online count: MeshMonitor's when available (closest to the
    radio's real reach), else the bridge's own nodedb count (v15 behavior)."""
    c = mm_online_count(now, MESHMONITOR_API_URL, ONLINE_WINDOW_S,
                        _mm_online_cache, MM_ONLINE_TTL_S,
                        lambda u: requests.get(u, timeout=2).json())
    return c if c is not None else count_online(nodes, now)

def radio_check_allowed(sender, now, last_map, cooldown_s):
    """Per-sender cooldown for PUBLIC (channel) radio-check pongs. Mutates
    last_map on allow. Pure enough to ast-extract and test."""
    last = last_map.get(sender, 0)
    if now - last < cooldown_s:
        return False
    last_map[sender] = now
    return True

def radio_check_reply(query, packet, node_count=None, online_count=None):
    """Deterministic radio-check responder. "ping"/"test" are mesh convention for
    "can anyone hear me?" — they must NEVER reach the LLM (which, given an
    encyclopedia article about ping and its own prior replies, will roleplay
    running one — observed 2026-07-14: invented nodes N1-N5 with fake statuses).
    Returns None unless the query is a bare ping/test; otherwise a signal report
    from the packet's REAL reception data (hop count, SNR when direct) plus a
    bridge-status tail — a radio check at the AI node is really asking "is the
    whole stack alive?", so answer that too."""
    q = (query or "").strip().lower().strip("!?.,~ ")
    word = RADIO_CHECKS.get(q)
    if not word:
        return None
    hs, hl = packet.get("hopStart"), packet.get("hopLimit")
    snr = packet.get("rxSnr")
    hops = None
    if (isinstance(hs, int) and not isinstance(hs, bool)
            and isinstance(hl, int) and not isinstance(hl, bool) and hs >= hl):
        hops = hs - hl
    if hops == 0:
        detail = "heard you direct"
        if isinstance(snr, (int, float)) and not isinstance(snr, bool):
            detail += ", SNR {} dB".format(round(snr, 2))
    elif hops is not None:
        detail = "heard you via {} hop{}".format(hops, "" if hops == 1 else "s")
    else:
        detail = None
    # Every pong teaches the mesh how to invoke the AI. v15: live activity (nodes
    # heard in the online window) is what a pinger actually wants — total nodedb
    # size counts weeks-stale entries. Fall back to total-known without lastHeard data.
    if isinstance(online_count, int) and not isinstance(online_count, bool) and online_count > 0:
        tail = 'bridge + "@ai" up, {} nodes online'.format(online_count)
    elif isinstance(node_count, int) and not isinstance(node_count, bool) and node_count > 0:
        tail = 'bridge + "@ai" online, {} nodes known'.format(node_count)
    else:
        tail = 'bridge + "@ai" online'
    head = "{} — {}".format(word, detail) if detail else word
    return "{} · {}".format(head, tail)

def _inbound_meta(packet, dec):
    """Pull (mesh_id, reply_to_id, is_reaction) from an inbound packet dict.
    Protobuf-dict defaults are OMITTED: replyId/emoji are absent on normal
    messages. is_reaction is 1 or None (never 0) to keep NULL semantics."""
    mesh_id = packet.get("id") or None
    reply_to = dec.get("replyId") or None
    reacted = 1 if dec.get("emoji") else None
    return mesh_id, reply_to, reacted

def parse_traceroute(packet):
    """Normalize a TRACEROUTE_APP response into route lists. Returns
    (request_id, result) or (None, None). SNR wire format is dB*4, -128 =
    unknown -> None. Protobuf-dict omits empty fields: absent route keys = [].
    route/routeBack are INTERMEDIATE hops only (endpoints implied). Node
    numbers are uint32 (0..0xFFFFFFFF) -- an out-of-range value cannot be
    formatted as a real 8-hex-digit id.
    Parses unauthenticated RF input: any structurally-invalid field (wrong
    type at any nesting level, a list containing a bad-typed element, or a
    node number outside uint32 range) fails the WHOLE parse closed --
    (None, None) -- rather than crash or emit a result with a bogus id.
    route/snrTowards and routeBack/snrBack are additionally cross-checked:
    the wire format guarantees len(snrTowards) == len(route)+1 (ditto for
    the back leg). A length mismatch degrades ONLY the SNR list to unknown
    (None) placeholders of the correct length -- the route itself is real,
    received data and is never discarded for an SNR-side mismatch."""
    if not isinstance(packet, dict):
        return None, None
    dec = packet.get("decoded")
    if not isinstance(dec, dict):
        return None, None
    if dec.get("portnum") != "TRACEROUTE_APP":
        return None, None
    req = dec.get("requestId")
    if not isinstance(req, int) or isinstance(req, bool):
        return None, None
    tr = dec.get("traceroute")
    if tr is None:
        tr = {}
    elif not isinstance(tr, dict):
        return None, None
    def _valid_node_num(x):
        # Meshtastic node numbers are uint32. Non-int/bool, negative, or
        # >0xFFFFFFFF values are not real node ids.
        return isinstance(x, int) and not isinstance(x, bool) and 0 <= x <= 0xFFFFFFFF
    def _valid_list(key, node_ids=False):
        # Absent key -> [] (protobuf-dict omits empty fields). Present but not
        # a list, or containing any non-int/bool element (or, for node-id
        # lists, any out-of-uint32-range element) -> None (invalid) so the
        # caller can fail the whole parse instead of silently dropping the
        # bad element and shifting every later hop's SNR-to-route alignment.
        v = tr.get(key)
        if v is None:
            return []
        if not isinstance(v, list):
            return None
        for x in v:
            if node_ids:
                if not _valid_node_num(x):
                    return None
            elif not isinstance(x, int) or isinstance(x, bool):
                return None
        return v
    route, snr_towards, route_back, snr_back = (
        _valid_list("route", node_ids=True), _valid_list("snrTowards"),
        _valid_list("routeBack", node_ids=True), _valid_list("snrBack"))
    if route is None or snr_towards is None or route_back is None or snr_back is None:
        return None, None
    def _degrade_if_misaligned(route_list, snr_list):
        # Wire format guarantees len(snrTowards) == len(route)+1 (one reading
        # per intermediate hop, plus a final entry for the destination's own
        # reading) -- same rule for snrBack vs routeBack. A length mismatch
        # means the SNR list cannot be trusted to line up with the route: hop
        # N could end up displayed with hop N+1's signal reading. The ROUTE is
        # real, received data and is never discarded for this -- only the SNR
        # list degrades, to -128 (unknown) sentinels of the CORRECT length, so
        # every hop is honestly reported as "signal unknown" instead of wrong.
        expected_len = len(route_list) + 1
        if len(snr_list) != expected_len:
            return [-128] * expected_len
        return snr_list
    snr_towards = _degrade_if_misaligned(route, snr_towards)
    snr_back = _degrade_if_misaligned(route_back, snr_back)
    def _snrs(vals):
        return [None if v == -128 else v / 4.0 for v in vals]
    def _ids(vals):
        return ["!{:08x}".format(n) for n in vals]
    hop_start = packet.get("hopStart")
    from_id = packet.get("fromId")
    if isinstance(from_id, str) and from_id:
        responder = from_id
    else:
        from_num = packet.get("from", 0)
        responder = "!{:08x}".format(from_num) if _valid_node_num(from_num) else None
    return req, {
        "route": _ids(route), "snr_towards": _snrs(snr_towards),
        "route_back": _ids(route_back), "snr_back": _snrs(snr_back),
        "responder": responder,
        "hop_start": hop_start if isinstance(hop_start, int) and not isinstance(hop_start, bool) else None,
    }

def make_quoted_send(send_raw, quote_id):
    """Wrap a two-arg send(chunk, rid) into the one-arg send(chunk) the worker
    uses, quoting quote_id on the FIRST call only — the @ai answer's first
    chunk renders attached to the question; continuations stay plain."""
    state = {"first": True}
    def send(c):
        rid = quote_id if state["first"] else None
        state["first"] = False
        return send_raw(c, rid)
    return send

def on_receive(packet=None, interface=None):
    try:
        dec = (packet or {}).get("decoded", {})
        if dec.get("portnum") != "TEXT_MESSAGE_APP":
            return
        if packet.get("from") == my_num:
            return
        ch = packet.get("channel", 0)
        text = dec.get("text", "").strip()
        sender = packet.get("fromId") or "!{:08x}".format(packet.get("from", 0))
        is_dm = packet.get("to") == my_num
        is_ai = text.lower().startswith(PREFIX)
        node_name = node_display(interface, sender)
        mesh_id, reply_to_id, is_reaction = _inbound_meta(packet, dec)
        # Log ALL inbound mesh text (the dashboard feed), not just @ai queries.
        log_traffic("in", sender, node_name, ch, is_dm, is_ai, text,
                    mesh_id=mesh_id, reply_to_id=reply_to_id, is_reaction=is_reaction)
        if is_reaction:
            return   # a tapback is never a query — logged flagged, nothing else
        if not is_ai:
            # Bare "ping"/"test" is a radio check — answer deterministically (no LLM,
            # no history). DMs at the bridge always answer; channel checks answer
            # publicly, threaded to the ping, behind a per-sender cooldown (the mesh
            # has other auto-responders and airtime is shared).
            _nodes = getattr(interface, "nodes", None) or {}
            rc = radio_check_reply(text, packet, len(_nodes), live_online_count(_nodes, time.time()))
            if rc is not None and not _is_duplicate(packet.get("id")):
                if is_dm:
                    _send_and_log(lambda: interface.sendText(rc, destinationId=sender, wantAck=True, replyId=mesh_id),
                                  sender, node_name, ch, True, True, rc, reply_to_id=mesh_id)
                elif (RADIO_CHECK_CHANNEL and ch in ALLOWED
                        and radio_check_allowed(sender, time.time(), _rc_last, RADIO_CHECK_COOLDOWN_S)):
                    _send_and_log(lambda: interface.sendText(rc, channelIndex=ch, wantAck=True, replyId=mesh_id),
                                  sender, node_name, ch, False, True, rc, reply_to_id=mesh_id)
            return
        query = text[len(PREFIX):].strip()
        if is_dm:
            # Direct message to the AI node: reply privately to the sender. wantAck=True gets
            # firmware-level retransmits (up to 3) if the sender doesn't confirm receipt —
            # fire-and-forget DMs silently lost chunk 2 of multi-chunk replies on a busy mesh.
            send_raw = lambda c, rid=None: _send_and_log(
                lambda: interface.sendText(c, destinationId=sender, wantAck=True, replyId=rid),
                sender, node_name, ch, True, True, c, reply_to_id=rid)
        else:
            if ch not in ALLOWED:
                return
            # Broadcast wantAck uses the implicit-ack (a neighbor rebroadcasting counts);
            # firmware retransmits if nobody is heard repeating it.
            send_raw = lambda c, rid=None: _send_and_log(
                lambda: interface.sendText(c, channelIndex=ch, wantAck=True, replyId=rid),
                sender, node_name, ch, False, True, c, reply_to_id=rid)
        send = make_quoted_send(send_raw, mesh_id)
        if not query:
            return
        # DEDUP BEFORE the rate-limit (C1): a radio retransmit must not burn a rate slot or
        # re-arm the sender cooldown — drop it before allowed_now() is ever consulted.
        if _is_duplicate(packet.get("id")):
            log("dropping duplicate retransmit from {} (pkt_id={}, query={!r})".format(
                sender, packet.get("id"), query))
            return
        # Radio checks are answered deterministically and immediately — never queued,
        # never sent to the LLM, exempt from the cooldown (an unanswered radio check
        # is indistinguishable from a dead node, which defeats its purpose).
        _nodes = getattr(interface, "nodes", None) or {}
        rc = radio_check_reply(query, packet, len(_nodes), live_online_count(_nodes, time.time()))
        if rc is not None:
            send(rc)   # the quoted-send wrapper threads it to the ping via replyId
            return
        # "more" is a user-pulled continuation of an already-generated reply — exempting it
        # from the cooldown lets the next installment come immediately (dedup still applies).
        if query.lower() not in ("more", "continue") and not allowed_now(sender):
            log("rate-limited {}".format(sender))
            return
        log("query from {} {} : {}".format(sender, "DM" if is_dm else "ch{}".format(ch), repr(query)))
        # BLOCKING enqueue (B2): never drops. If the queue is momentarily full, hold the radio
        # thread and re-log every 5s so a genuine wedge is distinguishable from brief busyness.
        waited = 0.0
        while True:
            try:
                work_q.put((sender, ch, query, send, is_dm), timeout=5); break
            except _queue.Full:
                waited += 5
                log("queue still full after {:.0f}s — holding radio thread for {} (queue_depth={})".format(
                    waited, sender, work_q.qsize()))
    except Exception as e:
        log("handler error: {}".format(e))

def on_neighbor(packet=None, interface=None):
    """v9: capture NEIGHBORINFO packets into the neighbors table. Fully isolated —
    a failure here must never affect text handling (co-subscriber to on_receive)."""
    try:
        dec = (packet or {}).get("decoded", {}) or {}
        if dec.get("portnum") != "NEIGHBORINFO_APP":
            return
        ni = dec.get("neighborinfo") or {}
        node_id = packet.get("fromId") or ("!{:08x}".format(packet.get("from", 0)))
        rows = []
        now = time.time()
        for nb in (ni.get("neighbors") or []):
            num = nb.get("nodeId")
            if num is None:
                continue
            nbid = num if isinstance(num, str) else "!{:08x}".format(int(num))
            rows.append((now, node_id, nbid, nb.get("snr")))
        if not rows:
            return
        with db() as c:
            c.executemany("INSERT INTO neighbors(ts, node_id, neighbor_id, snr) VALUES(?,?,?,?)", rows)
    except Exception as e:
        log("neighbor handler error: {}".format(e))

_direct_seen = {}  # sender node_id -> last ts a direct-link sample was recorded (throttle)

def on_direct_neighbor(packet=None, interface=None):
    """v10: derive the BASE's direct RF neighbors from received traffic. A packet whose
    hopStart == hopLimit reached us with no rebroadcast, so its sender is a direct neighbor;
    record a base->sender edge (rxSnr) into the same neighbors table the NEIGHBORINFO path
    feeds, throttled per sender. Fully isolated (co-subscriber to on_receive) so it can never
    disturb text handling."""
    try:
        if my_num is None:
            return
        frm = packet.get("from")
        if frm is None or frm == my_num:
            return
        if packet.get("viaMqtt"):
            return  # heard via MQTT, not a real RF neighbor
        hs, hl = packet.get("hopStart"), packet.get("hopLimit")
        if hs is None or hl is None or hs != hl:
            return  # not a direct (0-hop) reception, or hop info missing
        sender = packet.get("fromId") or ("!{:08x}".format(frm))
        now = time.time()
        if now - _direct_seen.get(sender, 0) < DIRECT_NEIGHBOR_THROTTLE_S:
            return
        _direct_seen[sender] = now
        base_id = "!{:08x}".format(my_num)
        with db() as c:
            c.execute("INSERT INTO neighbors(ts, node_id, neighbor_id, snr) VALUES(?,?,?,?)",
                      (now, base_id, sender, packet.get("rxSnr")))
    except Exception as e:
        log("direct-neighbor handler error: {}".format(e))

def node_display(interface, node_id):
    try:
        n = (getattr(interface, "nodes", None) or {}).get(node_id, {})
        u = n.get("user", {}) or {}
        return u.get("longName") or u.get("shortName") or node_id
    except Exception:
        return node_id

def on_routing(packet=None, interface=None):
    """5a: correlate ROUTING_APP ACK/NAK packets to an outstanding outbound send by
    exact requestId, recency-fenced to 300s. Fully isolated co-subscriber to
    meshtastic.receive — a failure here must never touch text handling. ACKs are
    unauthenticated RF: state is display-only and drives NO automation."""
    global acks_seen, acks_matched, ack_orphans, ack_db_errors, last_ack_ts, _ack_confirmed
    try:
        dec = (packet or {}).get("decoded", {}) or {}
        if dec.get("portnum") != "ROUTING_APP":
            return
        req = dec.get("requestId")
        if req is None:
            return
        # ACKs for OUR sends are addressed to us — skip third-party routing
        # traffic before it costs a db() call on the radio-receive thread.
        to_num = packet.get("to")
        if to_num is not None and my_num is not None and to_num != my_num:
            return
        acks_seen += 1
        last_ack_ts = time.time()
        err = (dec.get("routing") or {}).get("errorReason")
        from_num = packet.get("from")
        with db() as c:
            # Terminal states (ack/failed) never re-match; weaker states may be
            # UPGRADED by a later, more definitive ROUTING packet (see ack_rank).
            row = c.execute(
                "SELECT id, node_id, is_dm, ack_state FROM msg_log WHERE mesh_id=? AND direction='out' "
                "AND (ack_state IS NULL OR ack_state IN ('radio-accepted','relayed')) "
                "AND ts > ? ORDER BY ts DESC LIMIT 1",
                (req, time.time() - 300)).fetchone()
            if row is None:
                ack_orphans += 1
                return
            row_id, dest_id, is_dm, cur_state = row[0], row[1], bool(row[2]), row[3]
            dest_num = None
            ds = str(dest_id) if dest_id else ""
            if ds.startswith("!") or ds.startswith("0x"):
                try:
                    dest_num = int(ds[1:] if ds.startswith("!") else ds, 16)
                except ValueError:
                    dest_num = None
            state = ack_state_for(err, from_num, dest_num, my_num, is_dm)
            if ack_rank(state) <= ack_rank(cur_state):
                return  # not an upgrade — keep the stronger existing state
            c.execute("UPDATE msg_log SET ack_state=? WHERE id=?", (state, row_id))
            acks_matched += 1
            if not _ack_confirmed:
                _ack_confirmed = True
                log("ACK TRACKING CONFIRMED — first ROUTING ack matched msg_log row {} -> {}".format(row_id, state))
    except Exception as e:
        ack_db_errors += 1   # increment BEFORE logging so a log-format throw still counts
        log("routing handler error: {}".format(e))

def on_traceroute(packet=None, interface=None):
    """v17: correlate TRACEROUTE_APP responses (and ROUTING_APP failures) to a
    pending traceroutes row by exact requestId. Fully isolated co-subscriber to
    meshtastic.receive — a failure here must never touch text handling. RF is
    unauthenticated: results are display-only and drive NO automation."""
    try:
        dec = (packet or {}).get("decoded", {}) or {}
        pn = dec.get("portnum")
        if pn not in ("TRACEROUTE_APP", "ROUTING_APP"):
            return
        to_num = packet.get("to")
        if to_num is not None and my_num is not None and to_num != my_num:
            return
        if pn == "TRACEROUTE_APP":
            req, result = parse_traceroute(packet)
            if req is None:
                return
            with db() as c:
                row = c.execute("SELECT id FROM traceroutes WHERE request_id=? AND status='pending' "
                                "AND ts > ? ORDER BY ts DESC LIMIT 1",
                                (req, time.time() - 600)).fetchone()
                if row is None:
                    return
                c.execute("UPDATE traceroutes SET status='ok', route=?, snr_towards=?, route_back=?, "
                          "snr_back=?, resp_ts=? WHERE id=?",
                          (json.dumps(result["route"]), json.dumps(result["snr_towards"]),
                           json.dumps(result["route_back"]), json.dumps(result["snr_back"]),
                           time.time(), row[0]))
            log("traceroute answered: request_id={} {} hop(s) towards".format(req, len(result["route"])))
        else:
            req = dec.get("requestId")
            if not isinstance(req, int) or isinstance(req, bool):
                return
            err = (dec.get("routing") or {}).get("errorReason")
            if not err or err == "NONE":
                return   # transit ack, not a verdict — the route may still arrive
            with db() as c:
                row = c.execute("SELECT id FROM traceroutes WHERE request_id=? AND status='pending' "
                                "AND ts > ? ORDER BY ts DESC LIMIT 1",
                                (req, time.time() - 600)).fetchone()
                if row is None:
                    return
                c.execute("UPDATE traceroutes SET status=?, resp_ts=? WHERE id=?",
                          ("failed:{}".format(err), time.time(), row[0]))
            log("traceroute failed: request_id={} {}".format(req, err))
    except Exception as e:
        log("traceroute handler error: {}".format(e))

def on_lost(interface=None):
    log("mesh link lost; exiting for supervisor restart")
    os._exit(1)

def selftest():
    print("selftest: library ...")
    load_books()
    ctx = library_context("hypothermia treatment")
    print("library context chars:", len(ctx))
    if not ctx:
        # The library (qdrant/Kiwix) is optional — a plain LLM-only setup has none, so this
        # is a skip, not a failure. The memory + LLM checks below still run.
        print("selftest: no offline library configured — skipping retrieval check")
    print("selftest: memory ...")
    add_fact("!selftest", "the selftest fact: the sky over the mesh is violet")
    add_msg("!selftest", "user", "hello")
    add_msg("!selftest", "assistant", "hi there")
    msgs = build_messages("!selftest", "What color did I say the sky was?")
    assert any("violet" in m["content"] for m in msgs), "fact not injected"
    assert msgs[-2]["content"] == "hi there", "history not injected"
    print("selftest: memory OK; asking LLM ...")
    print("LLM:", ask_llm(msgs)[:200])
    print("selftest passed")

def main():
    global iface, my_num
    if "--selftest" in sys.argv:
        return selftest()
    link = "tcp={}:{}".format(TCP_HOST, TCP_PORT) if TCP_HOST else "serial={}".format(SERIAL)
    log("mesh-ai-bridge v18 starting (net_backup={}): {} model={} prefix={} channels={} db={}".format(
        "on" if NET_BACKUP else "off",
        link, MODEL, repr(PREFIX), sorted(ALLOWED), MEM_DB))
    load_books()
    pub.subscribe(on_receive, "meshtastic.receive")
    pub.subscribe(on_telemetry, "meshtastic.receive.telemetry")   # v7: capture weather telemetry
    pub.subscribe(on_neighbor, "meshtastic.receive")              # v9: capture neighbor links
    pub.subscribe(on_direct_neighbor, "meshtastic.receive")       # v10: derive base's direct neighbors
    pub.subscribe(on_routing, "meshtastic.receive")               # v11/5a: ACK/NAK delivery tracking
    pub.subscribe(on_traceroute, "meshtastic.receive")            # v17: traceroute responses
    pub.subscribe(on_lost, "meshtastic.connection.lost")
    if TCP_HOST:
        iface = meshtastic.tcp_interface.TCPInterface(TCP_HOST, portNumber=TCP_PORT)
    else:
        iface = meshtastic.serial_interface.SerialInterface(devPath=SERIAL)
    info = iface.getMyNodeInfo() or {}
    my_num = info.get("num")
    if not hasattr(iface, "sendText"):
        log("CRITICAL: meshtastic interface has no sendText — sends + ACK tracking will fail")
    try:
        start_send_api()
    except Exception as e:
        log("send API failed to start (radio bridge continues without it): {}".format(e))
    try:
        start_worker()
    except Exception as e:
        log("CRITICAL: query worker failed to start — bridge cannot answer any queries: {}".format(e)); raise
    log("connected to node {} ({})".format(
        info.get("user", {}).get("longName"), info.get("user", {}).get("id")))
    last_prune = 0.0
    while True:
        n = snapshot_nodes(iface)
        log("node snapshot: {} nodes".format(n))
        if time.time() - last_prune > 86400:
            if prune_msg_log():
                last_prune = time.time()
        time.sleep(60)

if __name__ == "__main__":
    main()
