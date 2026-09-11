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
    (r"(?:average|avg|mean) (?:of )?(\w+)",
     "MATCH (r:Row) WHERE r.`{0}` IS NOT NULL "
     "RETURN avg(toFloat(r.`{0}`)) AS average_`{0}`"),

    # Maximum
    (r"(?:max(?:imum)?|highest|largest) (?:of )?(\w+)",
     "MATCH (r:Row) RETURN max(r.`{0}`) AS max_`{0}`"),

    # Minimum
    (r"(?:min(?:imum)?|lowest|smallest) (?:of )?(\w+)",
     "MATCH (r:Row) RETURN min(r.`{0}`) AS min_`{0}`"),

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


def _get_active_schema() -> SchemaInfo:
    try:
        driver = _get_driver()
        with driver.session(database=os.environ.get("NEO4J_DB", "CSV_Graph_DB")) as session:
            result = session.run("MATCH (r:Row) RETURN r LIMIT 50")
            nodes = [dict(record["r"]) for record in result if "r" in record]
            if nodes:
                cols = [k for k in nodes[0].keys() if k not in ["dataset_id", "row_index"]]
                return SchemaInfo(cols, nodes)
    except Exception:
        pass
    try:
        from src.ingest import jobs
        if jobs:
            last_job = list(jobs.values())[-1]
            return SchemaInfo(last_job.get("headers", []))
    except Exception:
        pass
    return SchemaInfo(["name", "department", "salary", "city"])


def template_chat(question: str) -> dict:
    # 1. First attempt the dynamic query engine
    schema = _get_active_schema()
    plan = parse_query(question, schema)

    if plan is not None:
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

    # 3. No match / Out of domain — honest response
    return {
        "answer":   "I don't have that information in the loaded data.",
        "cypher":   None,
        "result":   [],
        "grounded": False,
    }



# ─────────────────────────────────────────────────────────────
# MODE 2: LangChain + Qwen2.5-Coder:1.5B via Ollama
# ─────────────────────────────────────────────────────────────
def slm_chat(question: str, schema_hint: str = "") -> dict:
    try:
        from langchain_community.llms import Ollama
        from langchain_community.graphs import Neo4jGraph
        from langchain.chains import GraphCypherQAChain

        slm = Ollama(
            model=os.environ.get("OLLAMA_MODEL", "qwen2.5-coder:1.5b"),
            base_url=os.environ.get("OLLAMA_URL", "http://ollama:11434"),
            temperature=0.0,   # Deterministic — critical for Cypher
            num_predict=300,
        )

        graph = Neo4jGraph(
            url=os.environ["NEO4J_URI"],
            username=os.environ["NEO4J_USER"],
            password=os.environ["NEO4J_PASSWORD"],
            database=os.environ["NEO4J_DB"],
        )
        graph.refresh_schema()  # Always get latest schema after CSV loads

        chain = GraphCypherQAChain.from_llm(
            llm=slm,
            graph=graph,
            validate_cypher=True,          # Rejects hallucinated queries
            return_intermediate_steps=True,
            verbose=False,
            allow_dangerous_requests=True,
        )

        response = chain.invoke({"query": question})
        steps    = response.get("intermediate_steps", [])
        cypher   = steps[0].get("query", "")   if len(steps) > 0 else ""
        raw      = steps[1].get("context", []) if len(steps) > 1 else []

        # Detect ungroundable responses
        if not raw or "UNGROUNDABLE" in cypher.upper():
            return {
                "answer":   "I don't have that information in the loaded data.",
                "cypher":   cypher or None,
                "result":   [],
                "grounded": False,
            }

        return {
            "answer":   response.get("result", str(raw)),
            "cypher":   cypher,
            "result":   raw,
            "grounded": True,
        }

    except Exception as e:
        logging.warning(f"SLM failed ({e}), falling back to templates")
        return template_chat(question)


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

    mode = os.environ.get("CHATBOT_MODE", "template")

    if mode == "slm":
        return slm_chat(req.question)
    else:
        return template_chat(req.question)
