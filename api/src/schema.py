"""
schema.py — GET /schema
Returns discovered column names from an uploaded CSV.
Used by the UI to display column info and to power the SLM prompt.
"""

import os
from fastapi import APIRouter, HTTPException
from src.ingest import jobs

router = APIRouter()


@router.get("/schema")
def get_schema(job_id: str):
    """Returns the column structure discovered from the uploaded CSV."""
    if job_id not in jobs:
        raise HTTPException(404, detail="Job not found — upload a CSV first")

    job = jobs[job_id]
    cols = job["headers"]

    return {
        "job_id":             job_id,
        "filename":           job["filename"],
        "columns":            cols,
        "node_labels":        ["Dataset", "Row"],
        "relationship_types": ["HAS_ROW"],
        # Human-readable hint injected into the SLM system prompt
        "cypher_hint": (
            "Graph schema — "
            "Nodes: (:Dataset {id, filename, uploaded_at}), "
            f"(:Row {{row_index, dataset_id, {', '.join(cols[:20])}}}) "
            "— Relationships: (:Dataset)-[:HAS_ROW]->(:Row)"
        ),
    }
