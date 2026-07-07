# mesh-ai-bridge

A **Meshtastic → local LLM bridge** for off-grid mesh networks. Send `@ai <question>`
from any node on your mesh and get an answer back over LoRa — generated entirely on
your own hardware, grounded in an offline reference library, with no internet required.

Built for [Project NOMAD](https://github.com/aebconsulting), a self-hosted off-grid AI
+ knowledge base. Runs as a single container; the radio is the only thing it needs.

## Why this over a minimal bridge

A basic mesh-LLM bridge forwards a prompt to a model and chunks the reply. This one is
built to actually be useful when the grid is down:

- **Retrieval-augmented answers.** Every query is embedded and semantically searched
  against an offline vector library (qdrant) before it reaches the model, so answers are
  grounded in real reference text — medical guides, repair manuals, survival docs — not
  just model memory. Sub-second retrieval against a multi-million-chunk index.
- **Graceful degradation.** If the vector DB or embedder is unreachable, it falls back to
  keyword search over a Kiwix library, then to the bare model — and logs *which* path it
  took, so a missing answer is always diagnosable.
- **Persistent memory.** Per-sender conversation history plus operator-taught facts
  (`@ai remember <fact>`), stored in SQLite (WAL mode for concurrent dashboard reads).
- **Dashboard-ready.** A token-gated HTTP send API lets a dashboard transmit *through*
  the bridge (the bridge is the single radio owner), and every message + node telemetry
  snapshot is logged to SQLite for a live feed and map.
- **Built to not drop your message.** A bounded no-drop FIFO queue holds under load
  rather than shedding, radio retransmits are de-duplicated, and worker/API liveness is
  exposed on a health endpoint. The only intentional drop is when the LLM itself is
  unreachable.
- **LoRa-aware.** Replies are size-chunked to the mesh's byte limits and rate-limited
  per-node and globally, so one chatty node can't flood the channel.

## How it works

```
  handheld ──@ai──▶ Meshtastic radio ──serial/TCP──▶ bridge ──┬─▶ embed + qdrant (library)
                                                              ├─▶ SQLite (memory + msg_log + nodes)
                                                              └─▶ LLM (/v1/chat/completions)
  handheld ◀──LoRa reply (chunked)──────────────────────────────┘
```

The bridge connects to the radio over **serial** (`MESH_SERIAL`) or **TCP**
(`MESH_TCP_HOST` — for containerized deployments where the radio is exposed via
ser2net / meshtasticd). It subscribes to inbound text, and for messages starting with
the trigger prefix (`@ai` by default) it assembles context (library + memory), calls the
model, and sends the chunked reply back to the sender (DM) or channel.

## Requirements

- A Meshtastic radio (serial-attached, or reachable over TCP)
- An **OpenAI-compatible chat endpoint** — e.g. [Ollama](https://ollama.com),
  LiteLLM, or any `/v1/chat/completions` server
- *(optional, for retrieval)* an embeddings endpoint (Ollama `/api/embeddings`) +
  a [qdrant](https://qdrant.tech) collection of your pre-embedded library
- *(optional, for keyword fallback)* a [Kiwix](https://kiwix.org) server hosting `.zim` books

Only the radio + an LLM endpoint are required; everything else degrades gracefully.

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
`MESH_SERIAL` to its path.

Run the built-in self-test (checks library retrieval, memory, and the LLM round-trip):

```bash
docker run --rm -e LLM_BASE_URL=... mesh-ai-bridge python /app/bridge.py --selftest
```

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
| `QDRANT_URL` / `QDRANT_COLLECTION` | — | Vector library (retrieval) |
| `KIWIX_URL` / `LIBRARY_BOOKS` | — | Offline library (keyword fallback) |
| `SEND_TOKEN` / `SEND_PORT` | _(off)_ / `8700` | Dashboard send API (disabled if no token) |
| `MEM_DB` | `/opt/mesh-ai-bridge/memory.db` | SQLite memory + logs |

## Operator commands

Nodes listed in `ADMIN_NODES` can teach the assistant durable facts:

- `@ai remember The well pump breaker is in the north shed.`
- `@ai forget well pump`

Facts are injected into the context of every future answer.

## Dashboard integration

With `SEND_TOKEN` set, the bridge exposes a small HTTP API on `SEND_PORT` (bind it to a
private network only — it is not meant to face the internet):

- `POST /api/send` — `{"text": "...", "channel": 0}` or `{"text": "...", "to": "!abcd1234"}`,
  header `X-Send-Token: <token>`. Transmits through the bridge (the single radio owner).
- `GET /api/health` — radio/API/worker liveness, queue depth, worker idle time.

The `msg_log` and `nodes` tables in `MEM_DB` give a dashboard a live message feed and a
node map/telemetry view (open the DB read-only; WAL mode keeps reads from blocking the
bridge's writes).

## Security notes

- The send API is **token-gated** (constant-time compare) and **disabled** unless
  `SEND_TOKEN` is set. Bind `SEND_PORT` to a private/container network, never the public
  internet or a shared LAN.
- Only `ADMIN_NODES` can write facts; all other memory-write attempts are refused and logged.
- The bridge holds a **single** radio connection. Don't run a second Meshtastic client
  against the same radio — route dashboard sends through `/api/send` instead.

## License

MIT — see [LICENSE](LICENSE).
