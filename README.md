# Team-Fx — Graph-Grounded CSV Knowledge Pipeline & Conversational Intelligence

[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Ready-blue?logo=docker)](docker-compose.yml)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-009688?logo=fastapi)](https://fastapi.tiangolo.com)
[![Neo4j](https://img.shields.io/badge/Neo4j-5-008CC1?logo=neo4j)](https://neo4j.com)
[![Apache Kafka](https://img.shields.io/badge/Kafka-KRaft%20Mode-231F20?logo=apachekafka)](https://kafka.apache.org)
[![Ollama](https://img.shields.io/badge/Ollama-Qwen2.5--Coder-black?logo=ollama)](https://ollama.ai)

> Built for the **RISE @ RST Hackathon** by **Team-Fx**.  
> An end-to-end distributed system that transforms raw CSV datasets into queryable Neo4j knowledge graphs via an event-driven Apache Kafka stream, backed by a dual-mode conversational query engine grounded in graph truth.

---

## 🌟 Architecture Overview

```
 Browser / Client (HTTP:3000)
         │
         ▼
 ┌──────────────┐
 │  Nginx / UI  │─── Static Web Interface & Interactive Dashboard
 └──────────────┘
         │
         ▼
 ┌──────────────┐
 │ FastAPI API  │─── Ingestion, Validation & Chat Gateway (:8000)
 └──────────────┘
    │        ▲
    │        └───────────────────────┐
    ▼                                │
 ┌──────────────┐                    │ Cypher Query &
 │ Apache Kafka │ (Topic: csv-rows)  │ Graph Retrieval
 └──────────────┘                    │
    │                                │
    ▼                                │
 ┌──────────────┐             ┌──────────────┐
 │ Loader Daemon│────────────▶│    Neo4j     │ (:7474, :7687)
 └──────────────┘ (MERGE)     └──────────────┘
                                     ▲
 ┌──────────────┐                    │
 │ Ollama (SLM) │────────────────────┘
 └──────────────┘ (Qwen2.5-Coder:1.5B via NL-to-Cypher)
```

---

## 🚀 Key Features

1. **Deterministic & Idempotent Ingestion**:
   - Computes SHA-256 dataset hash for reproducible dataset identification.
   - Streams CSV rows into Kafka topic `csv-rows`.
   - Neo4j loader applies atomic `MERGE` statements with unique constraints `(dataset_id, row_index)`.
   - Re-uploading identical datasets is strictly idempotent with zero duplication.

2. **Graph Knowledge Model**:
   - Nodes: `(:Dataset)` and `(:Row)`
   - Relationships: `(:Dataset)-[:HAS_ROW]->(:Row)`
   - Dynamic schema detection adapts to arbitrary CSV headers.

3. **Dual-Mode Grounded Chat Engine**:
   - **Template & Heuristic Cypher Engine**: Fast, deterministic regex-based Cypher generation with instant execution.
   - **Small Language Model (SLM)**: Integrates local Ollama `qwen2.5-coder:1.5b` for NL-to-Cypher translation.
   - **Anti-Hallucination Guardrail**: Queries return the executed Cypher statement, raw records, and `grounded: true`. If no graph data matches, the system explicitly reports lack of grounding rather than hallucinating.

4. **Hostile Input Defense**:
   - Rejects empty files, non-CSV files, binary payloads, and header-only files with clear HTTP 400 responses.

5. **Modern Cyberpunk / Glassmorphism UI**:
   - Interactive drag-and-drop file upload with live progress tracking.
   - Graph statistics overview (node counts, row counts, relationship stats).
   - Conversational chat interface with Cypher query inspector, collapsible record viewers, and sample query chips.

---

## 📦 Service Topology (Docker Compose)

| Service | Port | Description |
|---|---|---|
| `ui` | `3000` | Nginx serving modern front-end application |
| `api` | `8000` | FastAPI application server |
| `loader` | — | Python background daemon consuming Kafka and writing to Neo4j |
| `kafka` | `9092` | Apache Kafka in KRaft mode (no Zookeeper required) |
| `neo4j` | `7474`, `7687` | Neo4j Graph Database |
| `ollama` | `11434` | Local SLM inference engine (`qwen2.5-coder:1.5b`) |

---

## 🛠️ Quick Start

### 1. Run with Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/sarathi07-coder/Team-Fx.git
cd Team-Fx

# Launch all services
docker compose up -d --build
```

Access the application:
- **Web UI**: [http://localhost:3000](http://localhost:3000)
- **API Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Neo4j Browser**: [http://localhost:7474](http://localhost:7474) (Username: `neo4j`, Password: `password`)

### 2. Standalone / Local Development

A local standalone runner is provided for offline testing and evaluation without Docker dependencies:

```bash
# Run local standalone server
python3 run_local.py
```
Open [http://localhost:8000](http://localhost:8000) in your browser.

---

## 🧪 Testing & Verification

Run the local API test suite against standard, volume, and hostile CSVs:

```bash
python3 test_api_local.py
```

Test fixtures provided:
- `small.csv`: Basic tabular data sanity test (10 rows).
- `large.csv`: Scalability and bulk volume test (5,000 rows).
- `broken.csv`: Hostile format test (malformed rows, empty values).

---

## 📋 API Endpoints

- `POST /ingest` — Upload and ingest CSV files into Kafka.
- `GET /status/{job_id}` — Query real-time ingestion status and row counts.
- `GET /health` — Check health and connectivity of downstream services.
- `GET /schema` — Fetch dynamically discovered graph schema & column labels.
- `POST /chat` — Ask natural language questions grounded in the graph data.

---

## 📄 Hackathon Report & Documentation

For detailed architecture analysis, constraints, benchmarks, and evaluation criteria, see:
- [REPORT.md](REPORT.md)
- [hackathon_analysis.md](hackathon_analysis.md)

---

## 👥 Authors

**Team-Fx**  
RISE @ RST #5 Hackathon
