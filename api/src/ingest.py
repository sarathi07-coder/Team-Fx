"""
ingest.py — POST /ingest and GET /status
Accepts a CSV upload, publishes one Kafka message per row,
and reports real row counts directly from Neo4j.
"""

import os
import csv
import io
import uuid
import json
import hashlib
import logging

from fastapi import APIRouter, UploadFile, File, HTTPException
from kafka import KafkaProducer
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    try:
        from kafka.errors import BrokerNotAvailableError as NoBrokersAvailable
    except ImportError:
        class NoBrokersAvailable(Exception): pass

from neo4j import GraphDatabase

router  = APIRouter()
# In-memory job store (sufficient for a single-instance hackathon)
jobs: dict = {}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )


def _get_driver():
    return GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    )


# ─────────────────────────────────────────────────────────────
# POST /ingest
# ─────────────────────────────────────────────────────────────
@router.post("/ingest", status_code=202)
async def ingest(file: UploadFile = File(...)):
    # ── 1. Validate file type ──
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(400, detail="Only CSV files are accepted")

    # ── 2. Read with a 50 MB safety cap ──
    content = await file.read(50 * 1024 * 1024)
    if not content.strip():
        raise HTTPException(400, detail="File is empty")

    # ── 3. Decode and parse ──
    try:
        text   = content.decode("utf-8", errors="replace")
        reader = list(csv.DictReader(io.StringIO(text)))
    except Exception as exc:
        raise HTTPException(400, detail=f"Invalid CSV: {exc}")

    if not reader:
        raise HTTPException(400, detail="CSV has a header row but no data rows")

    raw_headers = list(reader[0].keys())
    if not raw_headers or not any(h and str(h).strip() for h in raw_headers):
        raise HTTPException(400, detail="CSV header row is empty")

    headers = []
    header_map = {}
    unnamed_c = 1
    for h in raw_headers:
        if h is None or not str(h).strip():
            clean_h = f"unnamed_{unnamed_c}"
            unnamed_c += 1
        else:
            clean_h = str(h).strip().replace("\x00", "").replace("`", "")
        headers.append(clean_h)
        header_map[h] = clean_h

    # ── 4. Generate stable, reproducible IDs ──
    # dataset_id is deterministic: same file name + same row count = same ID
    # This guarantees idempotency across docker compose restarts
    dataset_id = hashlib.sha256(
        f"{file.filename}:{len(reader)}".encode()
    ).hexdigest()[:16]
    job_id = dataset_id[:8]

    # ── 5. Publish to Kafka — one message per row ──
    try:
        producer = _get_producer()
        for idx, row in enumerate(reader):
            clean_data = {
                header_map[orig_k]: str(row.get(orig_k, "")).strip()
                for orig_k in raw_headers
                if orig_k in header_map
            }
            producer.send("csv-rows", value={
                "job_id":     job_id,
                "dataset_id": dataset_id,
                "filename":   file.filename,
                "row_index":  idx,
                "data":       clean_data,
            })
        producer.flush()
        producer.close()
    except NoBrokersAvailable:
        raise HTTPException(503, detail="Kafka is not available")

    # ── 6. Store job metadata in memory ──
    jobs[job_id] = {
        "job_id":     job_id,
        "dataset_id": dataset_id,
        "filename":   file.filename,
        "rows_total": len(reader),
        "headers":    headers,
        "status":     "queued",
    }

    logging.info(
        f"📤 Ingested job={job_id} dataset={dataset_id} "
        f"file={file.filename} rows={len(reader)}"
    )

    return {
        "job_id":        job_id,
        "rows_received": len(reader),
        "status":        "queued",
    }


# ─────────────────────────────────────────────────────────────
# GET /status
# ─────────────────────────────────────────────────────────────
@router.get("/status")
def status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, detail="Job not found")

    job        = jobs[job_id]
    dataset_id = job["dataset_id"]
    total      = job["rows_total"]

    # Query real row count directly from Neo4j — never hardcode
    driver = _get_driver()
    try:
        with driver.session(database=os.environ["NEO4J_DB"]) as session:
            result  = session.run(
                "MATCH (:Dataset {id: $id})-[:HAS_ROW]->(r:Row) RETURN count(r) AS loaded",
                id=dataset_id,
            )
            loaded = result.single()["loaded"]
    except Exception:
        loaded = 0
    finally:
        driver.close()

    # Determine status from real counts
    failed = max(0, total - loaded) if loaded < total else 0
    if loaded == 0:
        current_status = "queued"
    elif loaded < total:
        current_status = "loading"
    else:
        current_status = "complete"
        failed         = 0

    return {
        "job_id":      job_id,
        "status":      current_status,
        "rows_total":  total,
        "rows_loaded": loaded,
        "rows_failed": failed,
    }
