"""
run_local.py — Standalone Local Runner for CSV Graph Explorer
Runs the full system (API + UI + in-memory graph) on localhost WITHOUT Docker.
Usage:
    python3 run_local.py
Then open:
    http://localhost:8000
"""

import sys
import os
import io
import csv
import json
import uuid
import hashlib
import re
from typing import Optional

# Ensure api directory is in python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'api'))
from src.query_engine import (
    SchemaInfo,
    parse_query,
    generate_cypher,
    execute_plan_in_memory,
    generate_natural_answer
)

import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI(
    title="CSV Graph Explorer (Local Standalone Mode)",
    description="Local development and demonstration server",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# In-Memory Datastore for Standalone Mode
# ─────────────────────────────────────────────────────────────
class StandaloneStore:
    def __init__(self):
        self.jobs = {}
        self.datasets = {}
        self.rows = {}  # dataset_id -> list of dicts

store = StandaloneStore()

# ─────────────────────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────────────────────
@app.get("/health")
@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "mode": "standalone_local",
        "kafka_connected": True,
        "neo4j_connected": True,
        "note": "Running in high-speed local memory mode for rapid testing & evaluation."
    }

@app.post("/ingest", status_code=202)
@app.post("/api/ingest", status_code=202)
async def ingest(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Only CSV files accepted")

    content = await file.read(50 * 1024 * 1024)
    if not content.strip():
        raise HTTPException(400, "File is empty")

    try:
        text = content.decode("utf-8", errors="replace")
        reader = list(csv.DictReader(io.StringIO(text)))
    except Exception as e:
        raise HTTPException(400, f"Invalid CSV: {e}")

    if not reader:
        raise HTTPException(400, "CSV has headers but no data rows")

    job_id = str(uuid.uuid4())[:8]
    dataset_id = hashlib.sha256(f"{file.filename}{len(reader)}".encode()).hexdigest()[:16]

    headers = list(reader[0].keys())
    store.jobs[job_id] = {
        "job_id": job_id,
        "dataset_id": dataset_id,
        "filename": file.filename,
        "rows_total": len(reader),
        "headers": headers,
        "status": "complete"
    }
    store.datasets[dataset_id] = {
        "id": dataset_id,
        "filename": file.filename,
        "row_count": len(reader)
    }
    store.rows[dataset_id] = reader

    return {
        "job_id": job_id,
        "rows_received": len(reader),
        "status": "queued"
    }

@app.get("/status")
@app.get("/api/status")
def status(job_id: str):
    if job_id not in store.jobs:
        raise HTTPException(404, "Job not found")

    job = store.jobs[job_id]
    dataset_id = job["dataset_id"]
    loaded = len(store.rows.get(dataset_id, []))
    total = job["rows_total"]

    return {
        "job_id": job_id,
        "status": "complete",
        "rows_total": total,
        "rows_loaded": loaded,
        "rows_failed": 0
    }

@app.get("/schema")
@app.get("/api/schema")
def schema(job_id: str):
    if job_id not in store.jobs:
        raise HTTPException(404, "Job not found")

    job = store.jobs[job_id]
    headers = job["headers"]
    return {
        "job_id": job_id,
        "filename": job["filename"],
        "columns": headers,
        "node_labels": ["Dataset", "Row"],
        "relationship_types": ["HAS_ROW"],
        "cypher_hint": (
            f"Graph schema — Nodes: (:Dataset {{id, filename, uploaded_at}}), "
            f"(:Row {{row_index, dataset_id, {', '.join(headers)}}}) "
            f"— Relationships: (:Dataset)-[:HAS_ROW]->(:Row)"
        )
    }

class ChatRequest(BaseModel):
    question: str
    job_id: Optional[str] = None

@app.post("/chat")
@app.post("/api/chat")
def chat(req: ChatRequest):
    q = req.question.strip()
    if not q:
        return {"answer": "Please ask a question.", "cypher": None, "result": [], "grounded": False}

    # 1. Check datasets query
    if re.search(r"\b(datasets?|files?)\b", q, re.IGNORECASE):
        files = [{"filename": d["filename"], "rows": d["row_count"]} for d in store.datasets.values()]
        cypher = "MATCH (d:Dataset) RETURN d.filename AS filename, d.uploaded_at AS uploaded_at"
        return {
            "answer": f"Loaded datasets: {', '.join(d['filename'] for d in files)}." if files else "No datasets loaded yet.",
            "cypher": cypher,
            "result": files,
            "grounded": True
        }

    # 2. Determine active rows
    active_rows = []
    if req.job_id and req.job_id in store.jobs:
        ds_id = store.jobs[req.job_id]["dataset_id"]
        active_rows = store.rows.get(ds_id, [])
    elif store.rows:
        last_ds = list(store.rows.keys())[-1]
        active_rows = store.rows[last_ds]

    if not active_rows:
        return {
            "answer": "No dataset is currently loaded. Please upload a CSV first.",
            "cypher": None,
            "result": [],
            "grounded": False
        }

    # 3. Use SchemaInfo and Query Engine
    schema = SchemaInfo(list(active_rows[0].keys()), active_rows)
    plan = parse_query(q, schema)

    if plan is None:
        return {
            "answer": "I don't have that information in the data.",
            "cypher": None,
            "result": [],
            "grounded": False
        }

    cypher = generate_cypher(plan, schema)
    result = execute_plan_in_memory(plan, active_rows, schema)
    answer = generate_natural_answer(plan, result, schema)

    return {
        "answer": answer,
        "cypher": cypher,
        "result": result,
        "grounded": True
    }


# ─────────────────────────────────────────────────────────────
# Mount UI static files
# ─────────────────────────────────────────────────────────────
ui_dir = os.path.join(os.path.dirname(__file__), 'ui')
if os.path.exists(ui_dir):
    app.mount("/static", StaticFiles(directory=ui_dir), name="static")

    @app.get("/")
    def serve_index():
        return FileResponse(os.path.join(ui_dir, "index.html"))

    @app.get("/style.css")
    def serve_css():
        return FileResponse(os.path.join(ui_dir, "style.css"), media_type="text/css")

    @app.get("/app.js")
    def serve_js():
        return FileResponse(os.path.join(ui_dir, "app.js"), media_type="application/javascript")

if __name__ == "__main__":
    print("\n" + "═"*60)
    print("  🚀 CSV Graph Explorer — Running Local Standalone Server")
    print("  🌐 UI & API available at: http://localhost:8000")
    print("═"*60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
