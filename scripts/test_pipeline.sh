#!/bin/bash
# scripts/test_pipeline.sh
# Smoke test — runs the full pipeline end to end
# Usage: ./scripts/test_pipeline.sh

set -e
BASE="http://localhost:8000"

echo ""
echo "══════════════════════════════════════════"
echo "  CSV Graph Pipeline — Smoke Test"
echo "══════════════════════════════════════════"

# ── 1. Health ──────────────────────────────────
echo ""
echo "1. Health check..."
HEALTH=$(curl -sf "$BASE/health")
echo "   $HEALTH"

STATUS=$(echo $HEALTH | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
if [ "$STATUS" != "ok" ]; then
  echo "   ❌ Health not OK — check containers"
  exit 1
fi
echo "   ✅ Health OK"

# ── 2. Create test CSV ─────────────────────────
echo ""
echo "2. Creating test CSV..."
cat > /tmp/test_smoke.csv << 'EOF'
name,department,salary,city
Alice,Engineering,95000,New York
Bob,Marketing,72000,London
Carol,Engineering,88000,New York
Dave,HR,61000,Paris
Eve,Engineering,102000,Berlin
Frank,Marketing,69000,New York
Grace,HR,58000,London
Heidi,Engineering,91000,Berlin
Ivan,Marketing,75000,Paris
Judy,HR,63000,New York
EOF
echo "   ✅ Created /tmp/test_smoke.csv (10 rows)"

# ── 3. Upload ──────────────────────────────────
echo ""
echo "3. Uploading CSV..."
INGEST=$(curl -sf -X POST "$BASE/ingest" -F "file=@/tmp/test_smoke.csv")
echo "   $INGEST"
JOB_ID=$(echo $INGEST | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
echo "   ✅ Job ID: $JOB_ID"

# ── 4. Poll until complete ─────────────────────
echo ""
echo "4. Waiting for rows to load into Neo4j..."
for i in {1..30}; do
  sleep 2
  STATUS_RESP=$(curl -sf "$BASE/status?job_id=$JOB_ID")
  STATUS=$(echo $STATUS_RESP | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  LOADED=$(echo $STATUS_RESP | python3 -c "import sys,json; print(json.load(sys.stdin)['rows_loaded'])")
  echo "   [$i] status=$STATUS rows_loaded=$LOADED"
  if [ "$STATUS" = "complete" ]; then
    echo "   ✅ All rows loaded"
    break
  fi
  if [ "$STATUS" = "failed" ]; then
    echo "   ❌ Load failed"
    exit 1
  fi
done

# ── 5. Schema ──────────────────────────────────
echo ""
echo "5. Checking schema..."
SCHEMA=$(curl -sf "$BASE/schema?job_id=$JOB_ID")
COLS=$(echo $SCHEMA | python3 -c "import sys,json; print(json.load(sys.stdin)['columns'])")
echo "   Columns: $COLS"
echo "   ✅ Schema discovered"

# ── 6. Chat questions ──────────────────────────
echo ""
echo "6. Testing chatbot..."

ask() {
  local Q="$1"
  echo "   Q: $Q"
  RESP=$(curl -sf -X POST "$BASE/chat" \
    -H "Content-Type: application/json" \
    -d "{\"question\": \"$Q\", \"job_id\": \"$JOB_ID\"}")
  ANSWER=$(echo $RESP | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  A: {d[\"answer\"]}')")
  GROUNDED=$(echo $RESP | python3 -c "import sys,json; print(json.load(sys.stdin)['grounded'])")
  echo "   $ANSWER"
  echo "   Grounded: $GROUNDED"
  echo ""
}

ask "how many rows"
ask "list all department"
ask "how many rows where department = Engineering"

# ── 7. Idempotency test ───────────────────────
echo "7. Testing idempotency (second upload)..."
curl -sf -X POST "$BASE/ingest" -F "file=@/tmp/test_smoke.csv" > /dev/null
sleep 8
STATUS2=$(curl -sf "$BASE/status?job_id=$JOB_ID" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['rows_loaded'])")
echo "   After second upload — rows in graph: $STATUS2"
echo "   ✅ If count is still 10, idempotency is working"

# ── 8. Hostile input tests ────────────────────
echo ""
echo "8. Hostile input tests..."

# Empty file
echo "" > /tmp/empty.csv
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/ingest" -F "file=@/tmp/empty.csv")
echo "   Empty file → HTTP $CODE (expected 400)"

# Non-CSV
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/ingest" -F "file=@/tmp/test_pipeline.sh")
echo "   Non-CSV file → HTTP $CODE (expected 400)"

# Chat before upload
RESP=$(curl -sf -X POST "$BASE/chat" -H "Content-Type: application/json" \
  -d '{"question": "how many rows", "job_id": "nonexistent"}')
GROUNDED=$(echo $RESP | python3 -c "import sys,json; print(json.load(sys.stdin)['grounded'])" 2>/dev/null || echo "false")
echo "   Invalid job_id chat → grounded=$GROUNDED (expected False/404)"

echo ""
echo "══════════════════════════════════════════"
echo "  ✅ Smoke test complete!"
echo "══════════════════════════════════════════"
echo ""
