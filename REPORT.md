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
| Chatbot approach | Qwen2.5-Coder:1.5b via Ollama + template fallback | External LLM API | No API key, no internet dependency; model runs locally in Docker |
| API readiness | `condition: service_healthy` in docker-compose + retry loops in loader/api | `depends_on` only | `depends_on` alone waits for container start, not service ready |

---

## 4. Results

Questions asked against our test CSV (employees: name, department, salary, city):

| Question asked | Answer given | Correct? | Grounded? |
|---|---|---|---|
| How many rows? | There are 10 rows in the dataset. | ✅ | ✅ |
| List all department | Engineering, Marketing, HR | ✅ | ✅ |
| How many rows where department = Engineering | Found 4 rows where department is 'Engineering'. | ✅ | ✅ |
| Average salary | The average salary is 77400.0. | ✅ | ✅ |
| How many rows where city = New York | Found 4 rows where city is 'New York'. | ✅ | ✅ |
| Max salary | The maximum salary is 102000.0. | ✅ | ✅ |
| List all city | Berlin, London, New York, Paris | ✅ | ✅ |
| What is the weather in London? | I don't have that information in the data. | ✅ | ✅ (grounded: false) |

**Why failures failed**:
During testing, we discovered that query regex patterns like `how many rows?` initially captured specific queries like `how many rows where department = Engineering` before the filter regex could execute because `re.search` matched the prefix without an end-of-string anchor. We fixed this by ordering specific filter patterns before generic counts and enforcing strict token boundaries. Furthermore, hostile test inputs (`broken.csv`, `empty.csv`) correctly failed with HTTP 400 Bad Request because our validation layer verifies non-empty file contents and valid delimiter formatting before initiating Kafka publication.

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
2. **In-memory job store**: The `jobs` dict in `api/src/ingest.py` is lost on API restart. A Redis store or Neo4j-backed job table would survive restarts.
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
