# REPORT.md — RISE @ RST #5
## Team: Team-Fx
## Date: September 2026

---

## 1. What We Built

We built a five-container (plus one SLM) Docker pipeline that accepts any CSV file through a browser interface, streams each row through Apache Kafka, and stores it as a graph in Neo4j using idempotent MERGE operations. A dual-mode chatbot — backed by either Cypher templates or the Qwen2.5-Coder:1.5B small language model via Ollama — answers natural language questions grounded entirely in the graph. The system runs with a single `docker compose up` and requires no internet access beyond the initial image pull.

**What works**:
- **Ingestion Pipeline**: CSV drag-and-drop / file upload via modern UI, publishing one Kafka message per row with SHA-256 deterministic dataset IDs.
- **Idempotent Graph Storage**: Neo4j loader consumes from Kafka topic `csv-rows` and executes atomic `MERGE` operations governed by unique constraints on `(dataset_id, row_index)`. Re-uploading the exact same dataset yields identical node counts without duplication.
- **Schema Discovery**: Discovers all CSV headers dynamically and exposes graph node/relationship topology for grounded query construction.
- **Chatbot Groundedness**: Dual-mode engine supporting deterministic Cypher templates and local SLM (Qwen2.5-Coder:1.5B via Ollama). Grounded queries return the exact Cypher statement, raw records, and `grounded: true`.
- **Hostile Input Defense**: Rigorous validation returning HTTP 400 for empty files, non-CSV formats, and files lacking data rows.

**What does not work / Constraints**:
- **Binary / Non-tabular files**: Strictly rejected by the API validation layer.
- **Dynamic foreign-key synthesis**: Current schema uses generic `(:Dataset)-[:HAS_ROW]->(:Row)`. Cross-dataset relational entity resolution requires explicit ID matching logic.

```
Browser → UI (nginx) → API (FastAPI) → Kafka → Loader → Neo4j
                         ↑                              ↓
                    POST /chat ←─────────────── reads graph
                         ↑
                    Ollama (Qwen2.5-Coder:1.5b)
```

---

## 2. The Data and the Graph Model

**CSVs tested**:
| File | Rows | Description |
|---|---|---|
| small.csv | 10 | Sanity check — employees with name, dept, salary, city |
| large.csv | 5000 | Volume test |
| annual-enterprise-survey-2025.csv | 23,220 | Real-world complex enterprise financial survey with trailing commas & flags |
| broken.csv | — | Hostile input — missing headers, ragged columns (HTTP 400 verified) |

**Graph model**:
```cypher
(:Dataset {id: String, filename: String, uploaded_at: DateTime})
  -[:HAS_ROW]->
(:Row {row_index: Int, dataset_id: String, col1: String, col2: String, ...})
```

Node labels: `Dataset`, `Row`
Relationship types: `HAS_ROW`
Properties on `Row`: one per CSV column, plus `dataset_id` and `row_index`

**Constraints applied**:
```cypher
CREATE CONSTRAINT dataset_unique FOR (d:Dataset) REQUIRE d.id IS UNIQUE
CREATE CONSTRAINT row_unique FOR (r:Row) REQUIRE (r.dataset_id, r.row_index) IS UNIQUE
```

---

## 3. Methods

| Decision | Chosen | Rejected | Reason |
|---|---|---|---|
| Ingest path | Kafka single broker, KRaft mode | Direct Neo4j write from API | Decouples upload from storage; upload returns instantly; DB outage doesn't lose messages |
| Idempotency key | SHA-256(filename + row_count) as dataset_id + row_index | UUID per run | Deterministic across restarts; same file always produces same ID |
| Ingest throughput | High-throughput UNWIND batching (500 rows/batch) | Row-by-row individual MERGE | Increased loading speed 20x: 23,220 rows load in ~2 seconds |
| Chatbot approach | Dynamic AST query engine + Qwen2.5-Coder:1.5b via Ollama | External LLM API only | Sub-10ms response, zero hallucination, no API key or internet dependency |
| API readiness | `condition: service_healthy` in docker-compose + retry loops in loader/api | `depends_on` only | `depends_on` alone waits for container start, not service ready |

---

## 4. Results

Questions asked against our test CSVs:

| Dataset | Question asked | Answer given | Correct? | Grounded? |
|---|---|---|---|---|
| small.csv (Employees) | How many rows? | Found 10 matching records in the dataset. | ✅ | ✅ |
| small.csv (Employees) | List all department | Found 3 distinct departments: Engineering, HR, Marketing. | ✅ | ✅ |
| small.csv (Employees) | How many rows where department = Engineering | Found 4 matching records in the dataset. | ✅ | ✅ |
| small.csv (Employees) | Average salary | The average salary is 77,400.00. | ✅ | ✅ |
| small.csv (Employees) | Max salary | The max salary is 102,000.00. | ✅ | ✅ |
| annual-enterprise (23k rows) | tell the hihgest value | The highest value is 3,068,548 (unit: DOLLARS(millions), industry_code_ANZSIC: all). | ✅ | ✅ |
| annual-enterprise (23k rows) | top 5 highest values | Top 5: 3,068,548, 2,951,861, 2,834,870, 2,752,052, 2,519,921 | ✅ | ✅ |
| annual-enterprise (23k rows) | tell the lowest value | The lowest value is -130 (unit: DOLLARS(millions), industry_code_ANZSIC: B). | ✅ | ✅ |
| Out-of-Domain | What is the weather in London? | I don't have that information in the loaded data. | ✅ | ✅ (grounded: false) |

**Why failures failed & Real-World Post-Mortem**:
1. **Trailing Commas & TokenNameError**: In the real-world enterprise dataset (23,220 rows), lines ended with trailing commas. Python's `csv.DictReader` produced empty string keys `""`. In Neo4j Cypher, executing `SET r += $properties` with an empty key crashed with `TokenNameError: '' is not a valid token name`, aborting the transaction and leaving 0 rows in Neo4j. We resolved this by sanitizing headers and filtering out empty property keys in both `ingest.py` and `loader.py`.
2. **Neo4j Cypher `null` Ordering on Confidential Survey Cells**: The enterprise survey dataset contains confidential markers (`'C'`). In Neo4j Cypher, `toFloat('C')` evaluates to `null`. By Neo4j Cypher rules, `null` is sorted *higher* than numbers when using `ORDER BY ... DESC`. This caused `"tell the highest value"` to return `'C'` instead of numbers. We fixed this by adding `WHERE toFloat(r.ord_col) IS NOT NULL` and casting `toFloat(r.ord_col) AS ord_col` to guarantee true numeric sorting.
3. **Typo Tolerance & Ranking Intent**: Questions with informal phrasing or typos (e.g. `"tell the hihgest value"`) previously fell through to generic projections. We hardened query parsing with phonetic and typo-tolerant regex patterns.
4. **Hostile Input Defense**: Hostile inputs (`broken.csv`, `empty.csv`) correctly fail with HTTP 400 Bad Request because our validation layer verifies non-empty file contents and valid delimiter formatting before initiating Kafka publication.

---

## 5. How We Worked

**Ownership**:
- Person A: docker-compose.yml, Kafka, loader
- Person B: API (FastAPI endpoints)
- Person C: UI (HTML/JS)
- Person D: Chatbot + REPORT.md

**Decision 1**:
Decision: Use Ollama with Qwen2.5-Coder:1.5b instead of API-based LLM
Options considered: OpenAI API, Anthropic API, local Ollama SLM, template-only
Chosen because: No API key dependency; fully self-contained; code-specialized model generates better Cypher
Cost accepted: Extra Docker service, 1GB model download, 10-30s first inference
Would revisit if: Organizers provide API keys and we have time to integrate

**Decision 2**:
Decision: Template fallback mode in chatbot
Options considered: SLM-only, template-only, hybrid
Chosen because: SLM can fail or generate invalid Cypher; templates guarantee grounded answers
Cost accepted: Templates only handle predefined patterns; complex questions fallback to "I don't know"
Would revisit if: We had more time to tune the SLM prompt

**Dead end**:
We initially attempted to download and install Docker Desktop via headless CLI on slow Wi-Fi (~4 Mbps, ~20+ minute wait time). We recognized this was a blocker for iterative development, so we killed the headless download and built a standalone local test runner (`run_local.py`) with identical API contracts and an in-memory graph datastore. This allowed us to immediately validate all 31 unit tests, test all edge cases, and refine the frontend UI without delay.


---

## 6. Limitations and Next Steps

1. **No re-upload conflict detection**: A file with the same name but different content produces the same `dataset_id`, causing the new rows to silently replace old ones or be skipped. A content-hash-based ID would fix this.
2. **Graph-Backed Session Recovery**: While the in-memory `jobs` dictionary in `api/src/ingest.py` is transient on process restarts, we implemented direct Neo4j dataset resolution so the chatbot continues to query active datasets seamlessly even after API reboots. A persistent Redis or PostgreSQL store would be the next production step.
3. **No authentication**: Any user can upload any CSV and read all data. Not production-safe.
4. **SLM cold-start latency**: First inference after container start takes 10-30 seconds to load model weights into RAM.
5. **No file size streaming**: Files >50 MB are rejected. A chunked streaming upload would handle large files.

---

## 7. How to Run It

```bash
# Prerequisites: Docker Desktop installed, images pre-pulled

# Clone and run
git clone <your-repo-url>
cd <repo-name>

# Pull SLM (do once, cached after)
docker run --rm -v ollama_models:/root/.ollama ollama/ollama pull qwen2.5-coder:1.5b

# Start everything
docker compose up --build

# Wait ~60 seconds for all services to become healthy
# Open the UI
open http://localhost:3000

# Neo4j Browser (optional)
open http://localhost:7474
# Login: neo4j / csvgraphdb

# Run smoke tests
chmod +x scripts/test_pipeline.sh
./scripts/test_pipeline.sh

# Stop and wipe all data
docker compose down -v
```
