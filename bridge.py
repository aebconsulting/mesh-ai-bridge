#!/usr/bin/env python3
"""mesh-ai-bridge v6 - Meshtastic (serial or TCP) -> local LLM, with persistent memory.

v4 adds MESH_TCP_HOST: connect to the radio via TCP (ser2net/meshtasticd) for the
containerized deployment; unset = native serial mode (rollback path).
v2 added SQLite memory (per-sender conversation history + long-term facts).
"@ai remember <fact>" stores a fact; every query is answered with facts +
that sender recent turns in context. Env-configured; drops on LLM failure;
rate-limited; channel-scoped; LoRa-sized replies.
"""
import os, re, sys, time, threading, collections, sqlite3, json, hmac
import queue as _queue
import requests
from pubsub import pub
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import meshtastic.serial_interface
import meshtastic.tcp_interface

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
MAX_CHUNKS = int(os.environ.get("MAX_REPLY_CHUNKS", "2"))
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
QDRANT_TOP_K = int(os.environ.get("QDRANT_TOP_K", "8"))
QDRANT_MIN_SCORE = float(os.environ.get("QDRANT_MIN_SCORE", "0.65"))          # measured: relevant medical ~0.74-0.84, noise <=0.63
QDRANT_TIMEOUT_S = int(os.environ.get("QDRANT_TIMEOUT_S", "10"))
EMBED_TIMEOUT_S = int(os.environ.get("EMBED_TIMEOUT_S", "15"))
MIN_CHUNK_CHARS = int(os.environ.get("MIN_CHUNK_CHARS", "120"))               # F1: skip tiny fragments; a 2-word snippet is not usable medical context
RERANK_ENABLED = os.environ.get("RERANK_ENABLED", "").lower() in ("1", "true", "yes")  # DEFAULT OFF: future cross-encoder rerank hook, not wired in v1
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
    return c

def log_traffic(direction, node_id, node_name, channel, is_dm, is_ai, text):
    try:
        with db() as c:
            c.execute("INSERT INTO msg_log(ts, direction, node_id, node_name, channel, is_dm, is_ai, text) "
                      "VALUES(?,?,?,?,?,?,?,?)",
                      (time.time(), direction, node_id, node_name, channel, int(is_dm), int(is_ai), text))
    except Exception as e:
        log("msg_log write failed: {}".format(e))

def snapshot_nodes(interface):
    try:
        rows = []
        for nid, n in (getattr(interface, "nodes", None) or {}).items():
            u = n.get("user", {}) or {}
            p = n.get("position", {}) or {}
            m = n.get("deviceMetrics", {}) or {}
            rows.append((nid, u.get("shortName"), u.get("longName"), p.get("latitude"), p.get("longitude"),
                         m.get("batteryLevel"), n.get("snr"), n.get("hopsAway"), n.get("lastHeard"), time.time()))
        with db() as c:
            c.executemany("INSERT INTO nodes(node_id, short_name, long_name, lat, lon, battery, snr, hops, "
                          "last_heard, updated) VALUES(?,?,?,?,?,?,?,?,?,?) "
                          "ON CONFLICT(node_id) DO UPDATE SET short_name=excluded.short_name, "
                          "long_name=excluded.long_name, lat=excluded.lat, lon=excluded.lon, "
                          "battery=excluded.battery, snr=excluded.snr, hops=excluded.hops, "
                          "last_heard=excluded.last_heard, updated=excluded.updated", rows)
        return len(rows)
    except Exception as e:
        log("node snapshot failed: {}".format(e))
        return 0

def prune_msg_log():
    """v6: msg_log shares memory.db with the AI memory (backed up nightly) - cap growth.
    Returns True on success, False on failure so the caller can retry (A3)."""
    try:
        with db() as c:
            n = c.execute("DELETE FROM msg_log WHERE ts < ?", (time.time() - RETENTION_DAYS * 86400,)).rowcount
        if n:
            log("msg_log pruned {} rows older than {}d".format(n, RETENTION_DAYS))
        return True
    except Exception as e:
        log("msg_log prune failed: {}".format(e))
        return False

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

def build_messages(sender, query):
    sys_parts = [SYSPROMPT]
    lib = library_context(query)
    if lib:
        sys_parts.append("Offline library context (prefer this; cite the book briefly):")
        sys_parts.append(lib)
    facts = get_facts()
    if facts:
        sys_parts.append("Known facts (remembered from prior conversations):")
        sys_parts += ["- [{}] {}".format(s, t) for s, t in facts]
    msgs = [{"role": "system", "content": "\n".join(sys_parts)}]
    msgs += get_history(sender)
    msgs.append({"role": "user", "content": query})
    return msgs

# ---------- offline library retrieval (v6/2b-i: qdrant semantic search, replaces cross-encoder) ----------
_book_ids = {}
_rerank_warned = False   # F6: emit the "RERANK_ENABLED set but not wired" warning at most once

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
    """qdrant vector search. Returns list of {score, payload} (score-desc) or None on failure
    (logged). A returned hit is guaranteed to have a numeric (non-bool) `score` and a dict
    `payload` whose `text` field, if present, is a string — so callers can safely subscript
    hit["score"]/hit["payload"] and read payload["text"]. It does NOT guarantee every other
    payload field is well-typed; the assembly loop in library_context() still coerces
    defensively. Structurally-invalid hits are dropped and counted; a non-list `result` or a
    transport failure returns None so callers fall back."""
    try:
        r = requests.post("{}/collections/{}/points/search".format(QDRANT_URL, QDRANT_COLLECTION),
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
            log("qdrant_search: dropped {} malformed hit(s) of {}".format(dropped, len(raw)))
        out.sort(key=lambda h: h["score"], reverse=True)  # F3: defensive re-sort (medical stakes)
        return out
    except Exception as e:
        log("qdrant unavailable ({}): {}".format(type(e).__name__, e))
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

def library_context(query):
    """Semantic retrieval: embed the query, vector-search the offline library, inject the best
    chunks. Falls back to Kiwix first-wins if embed/qdrant are down. Every no-context outcome is
    logged DISTINCTLY (embed-down / qdrant-down / zero-hits / below-floor / no-usable-text) so a
    missing medical answer is diagnosable. The assembly loop coerces every payload field with
    str() and is wrapped per-hit in try/except, so a malformed hit is skipped-and-logged and can
    NEVER raise out of here (which would be swallowed by handle_query's LLM-unreachable catch and
    silently drop a medical query)."""
    global _rerank_warned
    t0 = time.time()
    vec = embed_query(query)
    if vec is None:
        log("library_context: embed down -> kiwix fallback")
        return _kiwix_fallback(query)
    hits = qdrant_search(vec, QDRANT_TOP_K)
    if hits is None:
        log("library_context: qdrant down -> kiwix fallback")
        return _kiwix_fallback(query)
    if RERANK_ENABLED and not _rerank_warned:
        log("RERANK_ENABLED set but rerank layer not wired in v1")  # F6: don't silently ignore the flag
        _rerank_warned = True
    if not hits:
        log("library_context: qdrant returned zero hits, no context injected ({:.2f}s)".format(time.time() - t0))
        return ""
    if hits[0]["score"] < QDRANT_MIN_SCORE:
        log("library_context: top score {:.3f} < floor {}, no context injected ({:.2f}s)".format(
            hits[0]["score"], QDRANT_MIN_SCORE, time.time() - t0))
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
        winners.append("{}({:.2f})".format(title[:40], h["score"]))
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

def chunk_reply(text):
    if not text:
        return []
    chunks = []
    while text and len(chunks) < MAX_CHUNKS:
        if len(text.encode()) <= CHUNK:
            chunks.append(text)
            break
        cut = _byte_prefix(text, CHUNK)                 # byte-bounded, not char-bounded
        m = re.search(r"^.*[.!?]\s", cut, re.S)
        piece = (m.group(0) if m and len(m.group(0)) > CHUNK // 3 else cut).rstrip() or cut
        chunks.append(piece)
        text = text[len(piece):].lstrip()
    if text and len(chunks) == MAX_CHUNKS:
        # Reserve 4 bytes for " ..." so the final chunk still fits the budget (the old
        # CHUNK-2 slice + 4-byte suffix overflowed by 2 bytes even in pure ASCII).
        chunks[-1] = _byte_prefix(chunks[-1], CHUNK - 4).rstrip() + " ..."
    return chunks

# ---------- dashboard send API (v6) ----------
# Token-gated HTTP endpoint so the dashboard can transmit THROUGH the bridge
# (bridge is the single radio owner). Port is NOT published to the LAN -
# reachable only on the Docker network. Empty SEND_TOKEN disables the API.
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
                          "worker_idle_s": round(time.time() - last_progress_ts, 1)})

    def do_POST(self):
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
        if not dashboard_allowed_now():
            return self._reply(429, {"error": "rate limited"})
        if iface is None:
            return self._reply(503, {"error": "radio not connected"})
        try:
            if to:
                iface.sendText(text, destinationId=to)
                log_traffic("out", to, node_display(iface, to), ch, True, False, text)
            else:
                iface.sendText(text, channelIndex=ch)
                log_traffic("out", "dashboard", "Dashboard", ch, False, False, text)
        except Exception as e:
            log("sendapi radio send failed: {}".format(e))
            return self._reply(502, {"error": "radio send failed"})
        log("sendapi TX {} {}B: {}".format("dm " + to if to else "ch{}".format(ch), len(text.encode()), repr(text)))
        self._reply(200, {"ok": True})

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
                sender, ch, query, send = item
            except Exception as e:
                log("CRITICAL: malformed queue item dropped (unpack failed): {} raw={!r}".format(e, item))
                try: work_q.task_done()
                except Exception: pass
                continue
            try:
                handle_query(sender, ch, query, send)
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

def handle_query(sender, ch, query, send):
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
        reply = ask_llm(build_messages(sender, query))
    except Exception as e:
        log("LLM unreachable, dropping message: {}".format(e))
        return
    add_msg(sender, "user", query)
    add_msg(sender, "assistant", reply)
    for i, c in enumerate(chunk_reply(reply)):
        send(c)
        log("reply {} ({}B): {}".format(i + 1, len(c.encode()), repr(c)))
        time.sleep(2)

def on_receive(packet=None, interface=None):
    try:
        dec = (packet or {}).get("decoded", {})
        if dec.get("portnum") != "TEXT_MESSAGE_APP":
            return
        if packet.get("from") == my_num:
            return
        ch = packet.get("channel", 0)
        text = dec.get("text", "").strip()
        sender = packet.get("fromId") or hex(packet.get("from", 0))
        is_dm = packet.get("to") == my_num
        is_ai = text.lower().startswith(PREFIX)
        node_name = node_display(interface, sender)
        # Log ALL inbound mesh text (the dashboard feed), not just @ai queries.
        log_traffic("in", sender, node_name, ch, is_dm, is_ai, text)
        if not is_ai:
            return
        query = text[len(PREFIX):].strip()
        if is_dm:
            # Direct message to the AI node: reply privately to the sender.
            send = lambda c: (log_traffic("out", sender, node_name, ch, True, True, c),
                              interface.sendText(c, destinationId=sender))[1]
        else:
            if ch not in ALLOWED:
                return
            send = lambda c: (log_traffic("out", sender, node_name, ch, False, True, c),
                              interface.sendText(c, channelIndex=ch))[1]
        if not query:
            return
        # DEDUP BEFORE the rate-limit (C1): a radio retransmit must not burn a rate slot or
        # re-arm the sender cooldown — drop it before allowed_now() is ever consulted.
        if _is_duplicate(packet.get("id")):
            log("dropping duplicate retransmit from {} (pkt_id={}, query={!r})".format(
                sender, packet.get("id"), query))
            return
        if not allowed_now(sender):
            log("rate-limited {}".format(sender))
            return
        log("query from {} {} : {}".format(sender, "DM" if is_dm else "ch{}".format(ch), repr(query)))
        # BLOCKING enqueue (B2): never drops. If the queue is momentarily full, hold the radio
        # thread and re-log every 5s so a genuine wedge is distinguishable from brief busyness.
        waited = 0.0
        while True:
            try:
                work_q.put((sender, ch, query, send), timeout=5); break
            except _queue.Full:
                waited += 5
                log("queue still full after {:.0f}s — holding radio thread for {} (queue_depth={})".format(
                    waited, sender, work_q.qsize()))
    except Exception as e:
        log("handler error: {}".format(e))

def node_display(interface, node_id):
    try:
        n = (getattr(interface, "nodes", None) or {}).get(node_id, {})
        u = n.get("user", {}) or {}
        return u.get("longName") or u.get("shortName") or node_id
    except Exception:
        return node_id

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
    log("mesh-ai-bridge v6 starting: {} model={} prefix={} channels={} db={}".format(
        link, MODEL, repr(PREFIX), sorted(ALLOWED), MEM_DB))
    load_books()
    pub.subscribe(on_receive, "meshtastic.receive")
    pub.subscribe(on_lost, "meshtastic.connection.lost")
    if TCP_HOST:
        iface = meshtastic.tcp_interface.TCPInterface(TCP_HOST, portNumber=TCP_PORT)
    else:
        iface = meshtastic.serial_interface.SerialInterface(devPath=SERIAL)
    info = iface.getMyNodeInfo() or {}
    my_num = info.get("num")
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
