#!/usr/bin/env python3
"""Ingest SurvivalRAG pre-embedded chunks into qdrant collection `survivalrag`.

Chunks ship with 768-dim nomic-embed-text vectors (same embedder the bridge
queries with). Payload fields are shaped for bridge.py library_context():
text / article_title / section_title / source — plus full provenance.
Deterministic UUIDv5 ids make re-runs idempotent (upsert, not duplicate)."""
import json, glob, uuid, urllib.request, sys, time

QDRANT = "http://localhost:6333"
COLL = "survivalrag"
BATCH = 500
NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/bdkoeh/survivalRAG")

def req(method, path, body=None):
    r = urllib.request.Request(QDRANT + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=120) as resp:
        return json.loads(resp.read())

# Create collection if missing (768 cosine, matching the shipped vectors)
try:
    req("GET", f"/collections/{COLL}")
    print("collection exists")
except Exception:
    print(req("PUT", f"/collections/{COLL}", {"vectors": {"size": 768, "distance": "Cosine"}}))

files = sorted(glob.glob("/home/ai_box/survivalRAG/processed/chunks/*.jsonl"))
total = skipped = 0
batch = []
t0 = time.time()

def flush():
    global batch
    if batch:
        req("PUT", f"/collections/{COLL}/points?wait=true", {"points": batch})
        batch = []

for fi, f in enumerate(files):
    for line in open(f, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        emb = r.get("embedding") or []
        if len(emb) != 768:
            skipped += 1
            continue
        md = r.get("metadata", {})
        key = "{}|{}|{}|{}".format(md.get("source_document",""), md.get("section_header",""),
                                   md.get("chunk_index",0), md.get("page_number",0))
        payload = {
            "text": r.get("text",""),
            "article_title": md.get("source_title") or md.get("source_document") or "SurvivalRAG",
            "section_title": md.get("section_header") or "",
            "source": md.get("source_document",""),
            "archive_title": "SurvivalRAG",
            "categories": md.get("categories") or [],
            "content_type": md.get("content_type",""),
            "page_number": md.get("page_number"),
            "chunk_index": md.get("chunk_index"),
            "license": md.get("license",""),
            "source_url": md.get("source_url",""),
        }
        if md.get("warning_level"):
            payload["warning_level"] = md["warning_level"]
            payload["warning_text"] = md.get("warning_text") or ""
        batch.append({"id": str(uuid.uuid5(NS, key)), "vector": emb, "payload": payload})
        total += 1
        if len(batch) >= BATCH:
            flush()
    if (fi + 1) % 50 == 0:
        print(f"{fi+1}/{len(files)} files, {total} points, {time.time()-t0:.0f}s", flush=True)
flush()
info = req("GET", "/collections/" + COLL)
count = info["result"]["points_count"]
print("DONE: {} upserted, {} skipped (no vector), collection count={}, {:.0f}s".format(total, skipped, count, time.time()-t0))
