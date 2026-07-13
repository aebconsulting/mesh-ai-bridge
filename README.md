# mesh-ai-bridge

Ask your mesh a question from any Meshtastic node — `@ai how do I purify creek water?` —
and get a real answer back over LoRa, from an LLM running on **your own hardware**,
grounded in **your offline reference library**. No internet. One container between your
radio and any OpenAI-compatible endpoint.

```
!node-42  @ai how do I treat a deep cut with no supplies?
bridge    (1/2) Apply firm direct pressure with the cleanest cloth you have and
          hold it. Do not remove a soaked cloth—add more on top. Elevate the limb.
bridge    (2/2) Keep pressure until bleeding stops. Watch for shock. Source: Where
          There Is No Doctor, wound care.
```

It chunks replies to LoRa's byte limits, rate-limits itself so it never floods your
channel, remembers facts you teach it from a handheld, and — when the vector DB or the
internet isn't there — degrades gracefully instead of going silent.

## How it compares (honestly)

There are already several Meshtastic → LLM bridges, and if you just want prompt → reply
over the mesh, some of them are great and simpler to stand up:

- **[meshing-around](https://github.com/SpudGunMan/meshing-around)** — the community
  default; huge feature set (BBS mail, weather, EAS alerts, Kiwix RAG, multi-radio).
- **[MESH-AI / mesh-api](https://github.com/mr-tbot/mesh-api)** — feature-maximalist,
  many providers, and it ships a full web dashboard + node map.
- **[SurvivalRAG](https://github.com/bdkoeh/survivalRAG)** — ships a *pre-built* survival/
  medical RAG corpus, so you don't have to build your own index.

**Where this one is different:** it's a small, container-first, single-purpose bridge for
people who run (or want to run) **semantic RAG over their *own* corpus**, and it's built
with reliability engineering the hobby tier usually skips:

- **Answers cite real manuals, not model memory.** Your query is embedded and vector-
  searched against your offline library (qdrant) before it reaches the model, so answers
  come from actual medical/repair/survival text you chose — not just what the model
  happened to memorize.
- **It still answers when things break.** If the vector DB or embedder is unreachable it
  falls back to keyword search over a Kiwix library, then to the bare model — and it
  **logs which path it took**, so a missing answer is diagnosable, not mysterious.
- **It won't flood your channel.** Replies are byte-chunked to the LoRa limit and rate-
  limited per-node and globally; one chatty node can't hog airtime.
- **It holds under load instead of dropping.** A bounded FIFO queue with radio-retransmit
  de-duplication; worker/API liveness is exposed on a health endpoint.
- **It owns the radio cleanly.** A token-gated HTTP send API lets a dashboard transmit
  *through* the bridge (the bridge is the single radio owner), and every message + node
  telemetry snapshot is logged to SQLite for a feed and map.
- **It tells you whether a message actually went anywhere.** Every send requests an ACK
  and a `ROUTING_APP` co-subscriber correlates them back per message: DMs upgrade to an
  end-to-end *acknowledged*, broadcasts to *relayed by a neighbor*, failures carry the
  radio's reason. States only ever upgrade — an early local ACK can't mask a lost DM —
  and the vocabulary never claims "delivered": a radio ACK is not a human receipt.
- **Replies and reactions are first-class.** The send API takes `reply_id` for quoted
  Discord-style replies and `react` for emoji tapbacks (hand-built packet — the Python
  API exposes `reply_id` but not the `emoji` flag), inbound reply targets and tapbacks
  are logged so a UI can thread and chip them, and the AI's answers quote the question
  they answer. A 👍 aimed at the AI never burns an LLM slot.
- **It collects everything the mesh says about itself.** Device metrics, environment
  sensors, power telemetry, GPS quality, and neighbor links all land in queryable SQLite
  tables alongside the message log — enough to drive a live map, per-node detail views,
  and mesh-weather answers with zero extra services.
- **You can teach it facts from the field.** `@ai remember The well pump breaker is in the
  north shed.` — stored forever, injected into future answers.

If your use case is "curated survival answers with zero setup," SurvivalRAG may fit better.
If you want a kitchen-sink bot, meshing-around does more. This is for when you want *your*
library, *your* model, and a bridge that stays up.

> **Note:** you supply the offline library. This repo is the bridge, not the corpus — it
> reads a qdrant collection / Kiwix server you point it at. Building and embedding that
> library is on you (a curated-corpus tool like SurvivalRAG, or your own ingestion, gets
> you one).

## How it works

```
  handheld ──@ai──▶ Meshtastic radio ──serial/TCP──▶ bridge ──┬─▶ embed + qdrant (library)
                                                              ├─▶ SQLite (memory + msg_log + nodes)
                                                              └─▶ LLM (/v1/chat/completions)
  handheld ◀──LoRa reply (chunked)──────────────────────────────┘
```

Connects to the radio over **serial** (`MESH_SERIAL`) or **TCP** (`MESH_TCP_HOST`, for
containerized setups where the radio is exposed via ser2net / meshtasticd). For inbound
text starting with the trigger prefix (`@ai`), it assembles context (library + memory),
calls the model, and sends the chunked reply back to the sender (DM) or channel.

## Live data (mesh sensors + optional internet backup)

**Mesh weather telemetry (v7, always on):** nodes carrying BME280/680 sensors broadcast
temperature/humidity/pressure; the bridge records every reading (`env_log`) and injects an
aggregate of the last hour's conditions into the AI context. Fully offline — the sensors ARE
the weather station.

**Internet backup (v8, `NET_BACKUP=true`, default off):** when the host happens to be online,
live-data questions get real answers. A cached connectivity probe gates every fetch; offline,
behavior is byte-identical to the offline-only build — no errors, no hangs, honest answers.

- **Live weather** — keyless APIs (Open-Meteo, zippopotam.us). Location comes from the query
  ("weather in 33040"), else the **asking node's GPS position**, else the bridge node's GPS,
  else `NET_DEFAULT_PLACE`. A GPS handheld in the field gets weather for where it stands.
- **Live web search** (`SEARX_URL`) — general questions via your own SearXNG instance:
  `@ai search <anything>` always searches; news/price/score/"latest"-shaped questions search
  automatically. Top snippets ride into the model's context.

## ⚠️ Safety

This forwards **AI-generated** text over radio, including for medical, survival, and
repair questions. LLMs can be confidently wrong, and RAG grounding reduces but does not
eliminate that. **Do not rely on its output as a substitute for professional medical,
legal, or safety advice.** Treat every answer as a starting point to verify, not an
authority — especially in an emergency. Provided "as is" (see LICENSE); you are
responsible for how you deploy and use it.

## Requirements

- A Meshtastic radio (serial-attached, or reachable over TCP)
- An **OpenAI-compatible chat endpoint** — e.g. [Ollama](https://ollama.com), LiteLLM,
  or any `/v1/chat/completions` server
- Enough compute to run a useful model: a **RAG-grounded answer wants an ~8B+ model**, so
  in practice a machine with a modern GPU (or an Apple-silicon box). It will run a small
  model on a Pi-class device, but expect slower, weaker answers.
- *(optional, for retrieval)* an embeddings endpoint (Ollama `/api/embeddings`) + a
  [qdrant](https://qdrant.tech) collection of your pre-embedded library
- *(optional, keyword fallback)* a [Kiwix](https://kiwix.org) server hosting `.zim` books

Only the radio + an LLM endpoint are required; retrieval and fallback degrade gracefully.

## Quick start (Docker)

```bash
docker build -t mesh-ai-bridge .

docker run -d --name mesh-ai-bridge --restart unless-stopped \
  -e MESH_TCP_HOST=192.168.1.50 \
  -e LLM_BASE_URL=http://ollama:11434/v1 \
  -e LLM_MODEL=qwen3:8b \
  -e ALLOWED_CHANNELS=0 \
  -e ADMIN_NODES='!abcd1234' \
  -v mesh-bridge-data:/data \
  -e MEM_DB=/data/memory.db \
  mesh-ai-bridge
```

For serial instead of TCP, drop `MESH_TCP_HOST`, pass `--device` for your radio, and set
`MESH_SERIAL`. Smoke-test the stack (memory + LLM round-trip; retrieval is checked if
configured):

```bash
docker run --rm -e LLM_BASE_URL=... mesh-ai-bridge python /app/bridge.py --selftest
```

## Reply size

Replies are capped at `MAX_REPLY_CHUNKS` × `CHUNK_BYTES` (default **2 messages × 190
bytes**) — so answers are roughly **two text messages**, terse by design. LoRa airtime is
scarce; the system prompt pushes the model to be brief. Raise the caps if your mesh can
afford the airtime.

## Configuration

All configuration is via environment variables — see [`.env.example`](.env.example) for
the full annotated list. The essentials:

| Variable | Default | Purpose |
|---|---|---|
| `MESH_TCP_HOST` / `MESH_TCP_PORT` | _(serial)_ / `4403` | Reach the radio over TCP (else serial) |
| `MESH_SERIAL` | CP2102 by-id path | Serial device path (serial mode) |
| `LLM_BASE_URL` | `http://127.0.0.1:11434/v1` | OpenAI-compatible chat endpoint |
| `LLM_MODEL` | `deepseek-r1:8b` | Model name |
| `TRIGGER_PREFIX` | `@ai` | Prefix that invokes the assistant |
| `ALLOWED_CHANNELS` | `0` | Channels the bridge answers on |
| `ADMIN_NODES` | _(none)_ | Node IDs allowed to teach facts |
| `SYSTEM_PROMPT` | off-grid assistant | System prompt |
| `MAX_REPLY_CHUNKS` / `CHUNK_BYTES` | `2` / `190` | LoRa reply sizing |
| `NODE_COOLDOWN_S` / `GLOBAL_PER_MIN` | `30` / `6` | Rate limiting |
| `EMBED_URL` / `EMBED_MODEL` | Ollama / `nomic-embed-text` | Query embeddings (retrieval) |
| `QDRANT_URL` / `QDRANT_COLLECTION` | `qdrant:6333` / `knowledge_base` | Vector library (retrieval) |
| `KIWIX_URL` / `LIBRARY_BOOKS` | — | Offline library (keyword fallback) |
| `SEND_TOKEN` / `SEND_PORT` | _(off)_ / `8700` | Dashboard send API (disabled if no token) |
| `MEM_DB` | `/opt/mesh-ai-bridge/memory.db` | SQLite memory + logs |

## Teach it facts from a handheld

Nodes listed in `ADMIN_NODES` can give the bridge durable, shared knowledge that's injected
into every future answer:

- `@ai remember The well pump breaker is in the north shed.`
- `@ai forget well pump`

Facts are operator-taught and shared to everyone on the mesh (intended for common
reference like locations and procedures), not per-user private notes.

## Connecting a dashboard / UI

> **Companion:** [**NOMAD Mesh Dashboard**](https://github.com/aebconsulting/nomad-mesh-dashboard)
> is a ready-made, offline-first web UI for exactly this — a live feed + send box, a
> MapLibre node map, a sortable telemetry table, a combined log, and a per-node detail
> view. It reads the bridge DB read-only and sends through `/api/send`. Or build your own.

The bridge doesn't ship a UI, but it exposes everything one needs:

- **`POST /api/send`** (token-gated) — `{"text": "...", "channel": 0}` or `{"text": "...",
  "to": "!abcd1234"}` with header `X-Send-Token: <token>`. Transmits through the bridge
  (the single radio owner), so a dashboard doesn't need its own radio connection.
- **`GET /api/health`** — radio/API/worker liveness, queue depth, worker idle time.
- **`msg_log`** and **`nodes`** tables in `MEM_DB` — a live message feed and node
  map/telemetry. Open the DB **read-only**; WAL mode keeps reads from blocking the bridge.

Bind `SEND_PORT` to a private/container network only — it is not meant to face the internet.

## Security notes

- The send API is **token-gated** (constant-time compare) and **disabled** unless
  `SEND_TOKEN` is set. `GET /api/health` is unauthenticated (returns node name + queue
  internals), so keep `SEND_PORT` off any shared/public network.
- Only `ADMIN_NODES` can write facts; other memory-write attempts are refused and logged.
- The bridge holds a **single** radio connection. Don't run a second Meshtastic client
  against the same radio — route dashboard sends through `/api/send` instead.

## License

MIT — see [LICENSE](LICENSE). Free to use, modify, and redistribute. Part of a larger
self-hosted off-grid AI system (Project NOMAD); this bridge stands alone.
