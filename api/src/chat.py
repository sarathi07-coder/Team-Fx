"""
chat.py — POST /chat
Dual-mode chatbot:
  CHATBOT_MODE=slm      → LangChain + Qwen2.5-Coder:1.5B via Ollama
  CHATBOT_MODE=template → Pure Cypher template map (no model needed)

Both modes ALWAYS return: answer, cypher, result, grounded
A grounded:false answer NEVER fabricates — it says so plainly.
"""

import os
import re
import logging

from fastapi import APIRouter
from pydantic import BaseModel
from neo4j import GraphDatabase

from src.query_engine import (
    SchemaInfo,
    parse_query,
    generate_cypher,
    generate_natural_answer
)

router = APIRouter()


class ChatRequest(BaseModel):
    question: str
    job_id: str | None = None


# ─────────────────────────────────────────────────────────────
# Neo4j helper
# ─────────────────────────────────────────────────────────────
def _get_driver():
    return GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    )


def _run_cypher(cypher: str, params: dict = {}) -> list:
    driver = _get_driver()
    try:
        with driver.session(database=os.environ["NEO4J_DB"]) as session:
            result = session.run(cypher, **params)
            return [dict(r) for r in result]
    finally:
        driver.close()


# ─────────────────────────────────────────────────────────────
# MODE 1: Cypher Template Map (always grounded, no model)
# ─────────────────────────────────────────────────────────────
TEMPLATES = [
    # Count where column = value (MUST be before generic row count)
    (r"how many (?:rows? )?(?:where|with|have?|are|is) (\w[\w\s]*?)\s*(?:=|:)\s*['\"]?([^'\"?\n]+?)['\"]?$",
     "MATCH (r:Row) WHERE r.`{0}` = '{1}' RETURN count(r) AS count"),

    # Total row count
    (r"^how many rows\??$|^total rows\??$|^count rows\??$",
     "MATCH (r:Row) RETURN count(r) AS total_rows"),

    # Count by column value grouping
    (r"count (?:by|per|group by) (\w+)",
     "MATCH (r:Row) RETURN r.`{0}` AS `{0}`, count(r) AS count ORDER BY count DESC"),

    # List distinct values
    (r"(?:list|show|what are) (?:all |unique |distinct )?(\w[\w\s]*?)(?:\s*values?)?$",
     "MATCH (r:Row) RETURN DISTINCT r.`{0}` AS value ORDER BY value LIMIT 25"),

    # Average
    (r"(?:(?:tell|show|what is|find)?\s*(?:the\s+)?)?(?:average|avg|mean|averge|avrage) (?:of )?(\w+)",
     "MATCH (r:Row) WHERE r.`{0}` IS NOT NULL "
     "RETURN avg(toFloat(r.`{0}`)) AS average_`{0}`"),

    # Maximum
    (r"(?:(?:tell|show|what is|find)?\s*(?:the\s+)?)?(?:max(?:imum)?|highest|hihgest|higest|heighest|largest|biggest|peak) (?:of )?(\w+)",
     "MATCH (r:Row) WHERE r.`{0}` IS NOT NULL RETURN max(toFloat(r.`{0}`)) AS max_`{0}`"),

    # Minimum
    (r"(?:(?:tell|show|what is|find)?\s*(?:the\s+)?)?(?:min(?:imum)?|lowest|lowset|smallest|least) (?:of )?(\w+)",
     "MATCH (r:Row) WHERE r.`{0}` IS NOT NULL RETURN min(toFloat(r.`{0}`)) AS min_`{0}`"),

    # Datasets loaded
    (r"(?:what|which) (?:files?|datasets?|csv) (?:are|have been|were) (?:loaded|uploaded)",
     "MATCH (d:Dataset) RETURN d.filename AS filename, d.uploaded_at AS uploaded_at"),

    # All columns / schema
    (r"(?:what|which) columns?|show (?:me )?(?:the )?columns?",
     "MATCH (r:Row) RETURN keys(r) AS columns LIMIT 1"),

    # Sample rows
    (r"(?:show|list|give) (?:me )?(?:some |a few |sample )?rows?",
     "MATCH (r:Row) RETURN r LIMIT 10"),
]


def _get_active_schema(dataset_id: str | None = None) -> SchemaInfo:
    try:
        driver = _get_driver()
        with driver.session(database=os.environ.get("NEO4J_DB", "neo4j")) as session:
            if dataset_id:
                result = session.run("MATCH (r:Row {dataset_id: $did}) RETURN r LIMIT 50", did=dataset_id)
            else:
                result = session.run("MATCH (r:Row) RETURN r LIMIT 50")
            nodes = [dict(record["r"]) for record in result if "r" in record.keys()]
            if nodes:
                cols = []
                seen_cols = set()
                for n in nodes:
                    for k in n.keys():
                        if k not in ["dataset_id", "row_index"] and k not in seen_cols:
                            cols.append(k)
                            seen_cols.add(k)
                return SchemaInfo(cols, nodes)
    except Exception as e:
        logging.warning(f"Failed to load schema from Neo4j: {e}")
    try:
        from src.ingest import jobs
        if jobs:
            last_job = list(jobs.values())[-1]
            return SchemaInfo(last_job.get("headers", []))
    except Exception:
        pass
    return SchemaInfo(["name", "department", "salary", "city"])


def template_chat(question: str, dataset_id: str | None = None) -> dict:
    # 1. First attempt the dynamic query engine
    schema = _get_active_schema(dataset_id)
    plan = parse_query(question, schema)

    if plan is not None:
        if dataset_id:
            plan["dataset_id"] = dataset_id
        cypher = generate_cypher(plan, schema)
        try:
            records = _run_cypher(cypher)
            if records:
                answer = generate_natural_answer(plan, records, schema)
                return {
                    "answer":   answer,
                    "cypher":   cypher,
                    "result":   records,
                    "grounded": True,
                }
        except Exception as e:
            logging.warning(f"Engine Cypher failed: {e}")

    # 2. Secondary fallback: regex TEMPLATES
    q = question.lower().strip()
    for pattern, cypher_template in TEMPLATES:
        match = re.search(pattern, q, re.IGNORECASE)
        if match:
            groups = match.groups()
            cypher = cypher_template
            for i, g in enumerate(groups):
                cypher = cypher.replace(f"{{{i}}}", g.strip())
            if dataset_id:
                cypher = cypher.replace("(r:Row)", f"(r:Row {{dataset_id: '{dataset_id}'}})")

            try:
                records = _run_cypher(cypher)
                if records:
                    return {
                        "answer":   f"{records}",
                        "cypher":   cypher,
                        "result":   records,
                        "grounded": True,
                    }
            except Exception as e:
                logging.warning(f"Template Cypher failed: {e}")
                continue

    # 3. Tertiary fallback: Ollama SLM Natural Language Cypher synthesis
    slm_cypher = _generate_cypher_with_ollama(question, schema, dataset_id)
    if slm_cypher:
        try:
            records = _run_cypher(slm_cypher)
            if records:
                plan = parse_query(question, schema) or {"target_cols": schema.columns[:2]}
                ans = generate_natural_answer(plan, records, schema)
                return {
                    "answer":   ans,
                    "cypher":   slm_cypher,
                    "result":   records,
                    "grounded": True,
                }
        except Exception as e:
            logging.warning(f"SLM generated Cypher failed execution: {e}")

    # 4. No match / Out of domain — honest response
    return {
        "answer":   "I don't have that information in the loaded data.",
        "cypher":   None,
        "result":   [],
        "grounded": False,
    }


# ─────────────────────────────────────────────────────────────
# MODE 2: Direct Ollama Qwen2.5-Coder:1.5B NL-to-Cypher Synthesis
# ─────────────────────────────────────────────────────────────
def _generate_cypher_with_ollama(question: str, schema: SchemaInfo, dataset_id: str | None = None) -> str | None:
    import urllib.request
    import json

    ollama_url = os.environ.get("OLLAMA_URL", "http://ollama:11434")
    model = os.environ.get("OLLAMA_MODEL", "qwen2.5-coder:1.5b")

    cols_str = ", ".join(schema.columns)
    dataset_filter = f'WHERE r.dataset_id = "{dataset_id}"' if dataset_id else ""

    prompt = f"""You are an expert Neo4j Cypher generator.
Graph schema: Node label (:Row) has properties: {cols_str}, dataset_id.
Question: {question}

Rules:
1. Return ONLY the raw Cypher query. No explanations, no markdown ticks, no commentary.
2. The query must start with MATCH (r:Row).
3. Always include {dataset_filter} in WHERE clause.
4. For numerical aggregations or sorting, use toFloat(r.prop).
5. If the question cannot be answered from these properties, respond ONLY with "OUT_OF_DOMAIN".
"""
    try:
        payload = json.dumps({
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_predict": 70,
                "num_ctx": 1024
            }
        }).encode("utf-8")

        req = urllib.request.Request(f"{ollama_url}/api/generate", data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode())
            raw = data.get("response", "").strip()
            clean_cypher = re.sub(r"```(?:cypher)?\s*", "", raw).replace("```", "").strip()
            if clean_cypher.upper().startswith("MATCH"):
                return clean_cypher
    except Exception as e:
        logging.warning(f"Ollama SLM call skipped/timed out: {e}")
    return None


def slm_chat(question: str, schema_hint: str = "", dataset_id: str | None = None) -> dict:
    schema = _get_active_schema(dataset_id)
    cypher = _generate_cypher_with_ollama(question, schema, dataset_id)
    if cypher:
        try:
            records = _run_cypher(cypher)
            if records:
                plan = parse_query(question, schema) or {"target_cols": schema.columns[:2]}
                ans = generate_natural_answer(plan, records, schema)
                return {
                    "answer":   ans,
                    "cypher":   cypher,
                    "result":   records,
                    "grounded": True,
                }
        except Exception as e:
            logging.warning(f"SLM query execution failed: {e}")
    return template_chat(question, dataset_id=dataset_id)


# ─────────────────────────────────────────────────────────────
# Endpoint
# ─────────────────────────────────────────────────────────────
@router.post("/chat")
def chat(req: ChatRequest):
    """
    Answers questions grounded in the Neo4j graph.
    Always returns: answer, cypher, result, grounded.
    Never fabricates an answer when grounded is false.
    """
    if not req.question or not req.question.strip():
        return {
            "answer":   "Please ask a question.",
            "cypher":   None,
            "result":   [],
            "grounded": False,
        }

    dataset_id = None
    if req.job_id and req.job_id.strip():
        from src.ingest import jobs
        if req.job_id in jobs:
            dataset_id = jobs[req.job_id].get("dataset_id")
        else:
            # Check if req.job_id is a dataset_id or id in Neo4j
            try:
                driver = _get_driver()
                with driver.session(database=os.environ.get("NEO4J_DB", "neo4j")) as session:
                    res = session.run("MATCH (d:Dataset) WHERE d.id = $id OR d.id STARTS WITH $id RETURN d.id LIMIT 1", id=req.job_id).single()
                    if res:
                        dataset_id = res["d.id"]
                    else:
                        res_row = session.run("MATCH (r:Row) WHERE r.dataset_id = $id OR r.dataset_id STARTS WITH $id RETURN r.dataset_id LIMIT 1", id=req.job_id).single()
                        if res_row:
                            dataset_id = res_row["r.dataset_id"]
                driver.close()
            except Exception as e:
                logging.warning(f"Error checking dataset_id in Neo4j: {e}")

        # If user explicitly passed a job_id but it does not exist:
        if not dataset_id:
            return {
                "answer":   "Job not found — please upload a valid CSV first.",
                "cypher":   None,
                "result":   [],
                "grounded": False,
            }
    else:
        # Fallback to latest dataset in Neo4j if no job_id was provided
        try:
            driver = _get_driver()
            with driver.session(database=os.environ.get("NEO4J_DB", "neo4j")) as session:
                res = session.run("MATCH (d:Dataset) RETURN d.id ORDER BY d.uploaded_at DESC LIMIT 1").single()
                if res:
                    dataset_id = res["d.id"]
                else:
                    res_row = session.run("MATCH (r:Row) RETURN r.dataset_id LIMIT 1").single()
                    if res_row:
                        dataset_id = res_row["r.dataset_id"]
            driver.close()
        except Exception as e:
            logging.warning(f"Error finding fallback dataset in Neo4j: {e}")

    mode = os.environ.get("CHATBOT_MODE", "template")

    if mode == "slm":
        return slm_chat(req.question, dataset_id=dataset_id)
    else:
        return template_chat(req.question, dataset_id=dataset_id)
