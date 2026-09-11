# 🚀 RISE @ RST #5 Hackathon — Full Project Breakdown

> **Event**: RISE @ RST #5 | **Theme**: *Data In, Answers Out*
> **Duration**: ~3 hours (5:00 PM – 8:00 PM)

---

## 🧠 1. Problem Statement — What Are We Building?

### The Core Challenge

Build a **dynamic, real-time CSV-to-Graph chatbot pipeline** that works like this:

```
User uploads ANY CSV file → Kafka streams each row → Neo4j graph DB → Chatbot answers questions from that data
```

The pipeline must work with **any CSV** — you don't know the columns ahead of time. A user drags in a file and within seconds can ask questions about that data via a chat interface — all backed by real graph data, not AI hallucinations.

### The Two Classic Failures They Warn About

| Failure | What it looks like | Why it's bad |
|---|---|---|
| **Pipeline with no chatbot** | Data goes in, nobody can query it | "A filing cabinet nobody can open" |
| **Chatbot with no real data** | Confident-sounding but hallucinated answers | "A puppet — worse than useless, does damage" |

**You are marked on the WHOLE path** — front door to back door.

---

## 🔧 2. What Concepts This Tests

| Concept | What You're Learning |
|---|---|
| **Message Brokers (Kafka)** | Decoupling ingestion from storage — industry standard pattern |
| **Graph Databases (Neo4j)** | Storing connected data as nodes + relationships, not tables |
| **Containerization (Docker)** | One-command reproducibility on any machine |
| **API Design** | Strict interface contracts that other teams can test |
| **Chatbot Grounding** | RAG-style: answers ONLY from real data, never from model memory |
| **Idempotency** | Running twice shouldn't duplicate data — a critical prod pattern |
| **Streaming Architecture** | Real-time progress tracking while data is still loading |

### Why Graph Instead of a Table?
A CSV might have a column like `customer_id` that appears across thousands of rows — that's a **relationship**, not just a value. Neo4j is built for exactly this shape: nodes (entities) connected by relationships. Cypher queries traverse these connections in ways SQL cannot easily express.

### Why Kafka in the Middle?
- Upload handler **returns immediately** — no blocking while the whole file loads
- If Neo4j is down, messages **queue safely** in Kafka instead of being lost
- You can **replay** the topic to reload the graph without re-uploading the file
- This is how **real production pipelines** are actually built

---

## 🏗️ 3. Architecture — Five Moving Parts

```
Browser (User)
   ↓ drag & drop CSV / ask question
   ↕  POST /ingest  |  POST /chat
  [UI] ─────────────────────→ [API] → [Kafka topic: csv-rows]
   ↑ preview rows / chat answer        ↓ one message per row
                                    [Loader] → MERGE into
                                     [Neo4j: CSV_Graph_DB]
                              ↑ reads for /chat & /status
```

### The 5 Services (all in `docker-compose.yml`)

| Service | Role |
|---|---|
| **ui** | Single-page: browse, upload CSV, preview rows, chat box |
| **api** | Backend: `/ingest`, `/status`, `/chat`, `/health` |
| **kafka** | Single broker, KRaft mode — topic `csv-rows`, one message per CSV row |
| **loader** | Consumes Kafka topic, MERGEs each row into Neo4j |
| **neo4j** | Graph DB — instance `CSV_Graph_DB`, queried by loader (write) + api (read) |

---

## 📋 4. Instructions You MUST Follow

### ⚠️ CRITICAL: Chatbot Honesty Rule (Part 1)
> If the question cannot be answered from what is actually in Neo4j, the correct answer is **"I don't have that in the data"** — NOT a plausible-sounding guess.
- Must return: `answer`, `cypher` (the actual query run), `result` (raw DB result), `grounded: true/false`
- **A confident sentence with nothing behind it = ZERO marks on grounding**

### ⚠️ CRITICAL: Idempotency Rule (Part 5)
- Use `MERGE`, **never** `CREATE`
- Key on `dataset_id + row_index`
- Two runs of the same CSV must produce **identical node and relationship counts**
- **No partial credit** — duplicates = zero on this component

### 🔐 Credentials (Fixed for event)
```
Database Name: CSV_Graph_DB
Password: csvgraphdb
```
> Store in `.env` / docker-compose env vars — NEVER hard-code in source code

### Graph Model (Keep It Simple First)
```cypher
(:Dataset {id, filename, uploaded_at})
  -[:HAS_ROW]->
(:Row {row_index, col_1, col_2, ...})
```
One property per CSV column on the Row node. Keep it boring — the system is what's graded.

### API Contract (Judges Will Test These Exact Shapes)

**POST /ingest** (multipart/form-data, field: `file`)
```json
→ 202 { "job_id": "b3f1", "rows_received": 1000, "status": "queued" }
```

**GET /status?job_id=b3f1**
```json
{ "job_id": "b3f1", "status": "loading", "rows_total": 1000, "rows_loaded": 640, "rows_failed": 3 }
// status: queued | loading | complete | failed
```

**GET /health**
```json
{ "status": "ok", "kafka_connected": true, "neo4j_connected": true }
// Must be NOT "ok" until BOTH are genuinely reachable — not just "container started"
```

**POST /chat**
```json
→ { "answer": "There are 128 rows where group = 'Billing'.",
    "cypher": "MATCH (r:Row {group: 'Billing'}) RETURN count(r)",
    "result": [{"count(r)": 128}],
    "grounded": true }
```

### Pre-Pull Docker Images (Do This NOW Before Hackathon!)
```bash
docker pull apache/kafka:3.7.0
docker pull neo4j:5.24-community
docker pull python:3.11-slim
```

---

## 📅 5. Timeline (3 Hours!)

| Time | Phase | Done When |
|---|---|---|
| **5:00–5:10** | Read + agree architecture + split work | Everyone knows what they own |
| **5:10–5:35** | Skeleton — 5 services with dummy/fake logic | `docker compose up` runs all 5 |
| **5:35–6:00** | Real UI — upload, preview, chat box | Real CSV can be dragged in + rows previewed |
| **6:00–6:30** | Real ingest + loader → rows reach Neo4j | `MATCH` in Neo4j Browser shows real rows |
| **6:30–7:00** | Real API — `/status` honest, `/health` honest | `curl /status` matches graph |
| **7:00–7:20** | Real chatbot — grounded answers + Cypher shown | Real question → correct grounded answer |
| **7:20–7:25** | Hardening — non-root, pinned tags, hostile input | Checklist passes |
| **7:25** | **⛔ BUILD FREEZE** | Nothing edited after this |
| **7:30–8:00** | REPORT.md written + committed | Report committed |

---

## 🎯 6. Marking Scheme (100 marks total)

| Component | Marks |
|---|---|
| `docker compose up` works clean, first try | 15 |
| Correct pipeline — Kafka decouples ingest from load | 10 |
| Healthcheck + correct startup ordering | 8 |
| Container hygiene (pinned tags, non-root, env creds, small images) | 7 |
| UI works end-to-end (upload, preview, chat) | 5 |
| API correctness + input handling | 10 |
| Idempotent load (no duplicates on rerun) | 10 |
| **Chatbot groundedness** (only from graph, honest "I don't know") | **10** |
| Report: methods and decisions | 12 |
| Report: results interpretation | 8 |
| Report: process and honesty | 5 |
| **Total** | **100** |

> 💡 **Key insight from PDF**: *"A team with a template-only chatbot, a clean pipeline and a sharp report will beat a team with an LLM-powered chatbot and a broken compose file."* Engineering is 90 marks. Chatbot cleverness is 10 marks.

---

## 🌍 7. Existing Tools in the Market (What Already Exists)

### CSV Ingestion Tools
| Tool | What It Does | Limitation |
|---|---|---|
| **Neo4j Data Importer** (GUI) | Point-and-click CSV to Neo4j | Not real-time, no API |
| **LOAD CSV (Cypher)** | Built-in Neo4j batch import | Static schema needed, not streaming |
| **neo4j-admin import** | Bulk cold import | Database must be offline |
| **Apache NiFi** | Visual dataflow for CSV → Kafka → DB | Complex setup, heavyweight |

### Chatbot / Q&A on Data Tools
| Tool | What It Does | Limitation |
|---|---|---|
| **LangChain CypherQAChain** | LLM → Cypher → Neo4j → Answer | Requires LLM API key |
| **LlamaIndex** | Graph + vector retrieval | Complex, not lightweight |
| **Neo4j Bloom** | Visual graph exploration | Not a chatbot, no NL interface |
| **Tableau / PowerBI** | Dashboard Q&A on tabular data | SQL-based, not graph |
| **ChatCSV / AskCSV** | LLM chat over CSV (in-memory) | No streaming, no graph, no idempotency |

### Pipeline / Streaming Tools
| Tool | What It Does | Limitation |
|---|---|---|
| **Apache Kafka + Connect** | Stream data into Neo4j | Pre-configured connectors, not dynamic CSV |
| **Airbyte / Fivetran** | Managed data ingestion | Cloud-based, not self-hosted 5-minute setup |
| **Debezium** | CDC streaming | Designed for DB-to-DB, not file uploads |

---

## 🆕 8. What WE Are Building Differently

This project is **uniquely different** from existing tools because:

| Feature | Existing Market | Our Project |
|---|---|---|
| **Schema discovery** | Requires schema defined upfront | Auto-discovers columns from any CSV |
| **End-to-end in one compose** | Separate tools, separate setup | 5 services, 1 command: `docker compose up` |
| **Real-time grounded chatbot** | Either static dashboards OR hallucinating LLMs | Chatbot that ONLY answers from graph + shows the Cypher proof |
| **Live progress tracking** | Batch "done or not done" | Real `rows_loaded` / `rows_failed` streaming while loading |
| **Idempotent by design** | Most tools duplicate on re-run | MERGE-keyed — safe to replay |
| **Hostile input handling** | Crash on bad CSV | Graceful errors on empty/broken/non-CSV files |

The **key innovation** is the combination:
> Dynamic schema discovery + Kafka streaming + Graph storage + Grounded chatbot + Reproducible Docker pipeline = a **full production-grade data platform in one evening**

---

## 🛠️ 9. Tech Stack We Will Use

| Layer | Tool | Why |
|---|---|---|
| **Language** | Python 3.11 | Mature Kafka + Neo4j clients, fast to write |
| **API** | FastAPI | Async, auto-validates request bodies, fast |
| **UI** | Plain HTML + vanilla JS (fetch) | Simple, fast, works — ugly UI > broken pretty one |
| **Message Broker** | Apache Kafka 3.7.0, single broker, KRaft mode | No ZooKeeper needed, event queue with replay |
| **Graph DB** | Neo4j 5.24 Community | Cypher, free browser UI at `:7474`, official Python driver |
| **Neo4j Driver** | `neo4j` Python driver | Official, never hand-roll Bolt connections |
| **Kafka Client** | `confluent-kafka` or `kafka-python` | Mature, handles retries |
| **Containers** | Docker + Docker Compose | One-command startup on any machine |
| **Chatbot Strategy** | Question → Cypher template map | Doesn't require LLM API key, always grounded |

---

## ✅ 10. Pre-Freeze Checklist (Must Pass ALL Before 7:25!)

- [ ] `docker compose down -v` then `docker compose up` works from clean
- [ ] No `latest` tag anywhere — all images pinned to versions
- [ ] Neo4j password comes from environment variable, NOT hard-coded
- [ ] Containers run as non-root user
- [ ] `/health` reports not-ok before Kafka + Neo4j are genuinely reachable
- [ ] `/status` reports real row counts including failures — no hardcoded values
- [ ] API survives: empty CSV, non-CSV file, chat question before any upload
- [ ] Second load of same CSV does NOT duplicate nodes
- [ ] Chat answers include Cypher query + result, says "I don't know" when ungrounded
- [ ] `REPORT.md` committed
- [ ] Everything pushed to repo

---

## 📝 11. The Report (REPORT.md) — 30 Marks!

Required sections in this order:
1. **What we built** — 5 sentences + architecture diagram. Honest about what works and what doesn't
2. **The data and the graph model** — CSVs tested, row counts, exact node labels / relationships / properties
3. **Methods** — Decision table: Ingest path / Idempotency key / Chatbot approach / API readiness strategy
4. **Results** — Table of ≥8 questions asked to chatbot, correct?, grounded?, and WHY failures failed
5. **How we worked** — Who owned what, 2 decisions with Options/Chosen/Cost/Revisit format, 1 dead end
6. **Limitations and next steps** — Specific production weaknesses (not vague "add monitoring")
7. **How to run it** — Exact commands. Judge must run it from this section alone

---

## 🏆 12. Stretch Goals (Extra Ways to Win)

- API image under 400 MB
- p95 `/ingest` response time under 200ms for 10,000-row file (with measurement method stated)
- UI shows live progress bar while large file is still loading (not just a spinner)
- Chatbot detects foreign-key-style columns and turns them into real Neo4j relationships
- Second `docker compose up` on same file skips already-MERGEd rows
- Multi-stage Docker builds (build tools don't ship in runtime image)
