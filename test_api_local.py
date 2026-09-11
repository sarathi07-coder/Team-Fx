"""
test_api_local.py — Tests the API locally WITHOUT Docker/Kafka/Neo4j
Run with:  python3 test_api_local.py
"""

import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'api'))

# ── Env vars FIRST ───────────────────────────────────────────────────────────
os.environ.update({
    "KAFKA_BOOTSTRAP": "localhost:9092",
    "NEO4J_URI":       "bolt://localhost:7687",
    "NEO4J_USER":      "neo4j",
    "NEO4J_PASSWORD":  "csvgraphdb",
    "NEO4J_DB":        "CSV_Graph_DB",
    "CHATBOT_MODE":    "template",
    "OLLAMA_URL":      "http://localhost:11434",
    "OLLAMA_MODEL":    "qwen2.5-coder:1.5b",
})

# ── Import src modules FIRST so patch() can find them ───────────────────────
import src.ingest
import src.health
import src.chat
import src.schema

from unittest.mock import MagicMock, patch

# ── Fake classes ─────────────────────────────────────────────────────────────
class FakeProducer:
    def send(self, topic, value=None): return MagicMock()
    def flush(self): pass
    def close(self): pass

class FakeResult:
    def __init__(self, data): self._data = data
    def single(self): return MagicMock(**{"__getitem__": lambda s, k: self._data.get(k, 10)})
    def __iter__(self): return iter([self._data])

class FakeSession:
    def __enter__(self): return self
    def __exit__(self, *a): pass
    def run(self, query, **params):
        return FakeResult({"loaded": 10, "total_rows": 10, "count": 5})

class FakeDriver:
    def verify_connectivity(self): pass
    def session(self, database=None): return FakeSession()
    def close(self): pass

_fake_driver = FakeDriver()
_fake_producer = FakeProducer()

# ── Apply patches ─────────────────────────────────────────────────────────────
with patch.object(src.ingest, 'KafkaProducer', return_value=_fake_producer), \
     patch.object(src.ingest, 'GraphDatabase', **{'driver.return_value': _fake_driver}), \
     patch.object(src.health, '_check_kafka', return_value=True), \
     patch.object(src.health, '_check_neo4j', return_value=True), \
     patch.object(src.chat,   '_get_driver',  return_value=_fake_driver), \
     patch.object(src.chat,   '_run_cypher',  return_value=[{"total_rows": 10}]):

    from fastapi.testclient import TestClient
    from src.main import app
    client = TestClient(app)

    results = []

    def test(name, condition, detail=""):
        icon = "✅" if condition else "❌"
        results.append((icon, name, detail))
        print(f"  {icon} {name}" + (f"  [{detail}]" if detail else ""))

    print("\n" + "═"*55)
    print("  CSV Graph API — Local Test Suite (no Docker)")
    print("═"*55)

    # ── 1. Health ──────────────────────────────────────────────
    print("\n📍 GET /health")
    r = client.get("/health")
    d = r.json()
    test("Returns 200",      r.status_code == 200,    f"got {r.status_code}")
    test("status=ok",        d.get("status") == "ok", str(d))
    test("kafka_connected",  d.get("kafka_connected") == True)
    test("neo4j_connected",  d.get("neo4j_connected") == True)

    # ── 2. Valid CSV upload ────────────────────────────────────
    print("\n📍 POST /ingest (valid CSV — 3 rows)")
    csv_bytes = b"name,department,salary\nAlice,Engineering,95000\nBob,Marketing,72000\nCarol,HR,61000"
    r = client.post("/ingest", files={"file": ("employees.csv", csv_bytes, "text/csv")})
    d = r.json()
    test("Returns 202",      r.status_code == 202,   f"got {r.status_code}: {d}")
    test("Has job_id",       "job_id" in d,           str(d))
    test("rows_received=3",  d.get("rows_received") == 3, str(d))
    test("status=queued",    d.get("status") == "queued")
    job_id = d.get("job_id", "noop")

    # ── 3. Status ──────────────────────────────────────────────
    print("\n📍 GET /status")
    r = client.get(f"/status?job_id={job_id}")
    d = r.json()
    test("Returns 200",      r.status_code == 200,  f"got {r.status_code}")
    test("Has rows_total",   "rows_total"  in d)
    test("Has rows_loaded",  "rows_loaded" in d)
    test("Has rows_failed",  "rows_failed" in d)
    test("Has status field", "status" in d)

    # ── 4. Schema ──────────────────────────────────────────────
    print("\n📍 GET /schema")
    r = client.get(f"/schema?job_id={job_id}")
    d = r.json()
    test("Returns 200",      r.status_code == 200,  f"got {r.status_code}: {d}")
    test("Has columns",      "columns" in d,         str(d.get("columns")))
    test("Columns correct",  set(d.get("columns",[])) == {"name","department","salary"})
    test("Has cypher_hint",  "cypher_hint" in d)

    # ── 5. Chat (grounded) ────────────────────────────────────
    print("\n📍 POST /chat (grounded question)")
    r = client.post("/chat", json={"question": "how many rows", "job_id": job_id})
    d = r.json()
    test("Returns 200",      r.status_code == 200)
    test("Has answer",       "answer"   in d)
    test("Has cypher",       "cypher"   in d)
    test("Has result",       "result"   in d)
    test("Has grounded",     "grounded" in d)

    # ── 6. Chat (ungrounded) ──────────────────────────────────
    print("\n📍 POST /chat (ungrounded — no matching template)")
    r = client.post("/chat", json={"question": "what is the weather in London"})
    d = r.json()
    test("Returns 200",      r.status_code == 200)
    test("Has grounded",     "grounded" in d)
    test("Has answer",       bool(d.get("answer")))

    # ── 7. Hostile: empty file ─────────────────────────────────
    print("\n📍 POST /ingest (empty file)")
    r = client.post("/ingest", files={"file": ("empty.csv", b"", "text/csv")})
    test("Returns 400",      r.status_code == 400, f"got {r.status_code}")
    test("Has detail",       "detail" in r.json())

    # ── 8. Hostile: non-CSV ───────────────────────────────────
    print("\n📍 POST /ingest (non-CSV file)")
    r = client.post("/ingest", files={"file": ("image.png", b"\x89PNG", "image/png")})
    test("Returns 400",      r.status_code == 400, f"got {r.status_code}")

    # ── 9. Hostile: header-only CSV ───────────────────────────
    print("\n📍 POST /ingest (header row only — no data)")
    r = client.post("/ingest", files={"file": ("nodata.csv", b"col1,col2\n", "text/csv")})
    test("Returns 400",      r.status_code == 400, f"got {r.status_code}")

    # ── 10. Hostile: bad job_id ────────────────────────────────
    print("\n📍 GET /status (nonexistent job_id)")
    r = client.get("/status?job_id=doesnotexist999")
    test("Returns 404",      r.status_code == 404, f"got {r.status_code}")

    # ── 11. Hostile: schema bad job_id ────────────────────────
    print("\n📍 GET /schema (nonexistent job_id)")
    r = client.get("/schema?job_id=doesnotexist999")
    test("Returns 404",      r.status_code == 404, f"got {r.status_code}")

    # ── SUMMARY ───────────────────────────────────────────────
    passed = sum(1 for r in results if r[0] == "✅")
    total  = len(results)
    print(f"\n{'═'*55}")
    print(f"  Results: {passed}/{total} passed  {'🎉' if passed==total else '⚠️'}")
    print(f"{'═'*55}")
    if passed == total:
        print("  ✅ ALL PASS — API logic verified without Docker!\n")
    else:
        for icon, name, detail in results:
            if icon == "❌":
                print(f"  ❌ FAIL: {name}  {detail}")
        print()
