"""
loader.py — Kafka consumer → Neo4j MERGE writer
Reads one message per CSV row from topic 'csv-rows'
Idempotently MERGEs each row into Neo4j using MERGE (never CREATE)
"""

import os
import json
import time
import logging

from kafka import KafkaConsumer
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    try:
        from kafka.errors import BrokerNotAvailableError as NoBrokersAvailable
    except ImportError:
        class NoBrokersAvailable(Exception): pass

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LOADER] %(levelname)s — %(message)s"
)

# ─────────────────────────────────────────────────────────────
# Neo4j Cypher — idempotent MERGE, never CREATE
# ─────────────────────────────────────────────────────────────
MERGE_QUERY = """
MERGE (d:Dataset {id: $dataset_id})
  ON CREATE SET
    d.filename    = $filename,
    d.uploaded_at = datetime()
MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
  ON CREATE SET r += $properties
MERGE (d)-[:HAS_ROW]->(r)
"""

BATCH_MERGE_QUERY = """
UNWIND $batch AS item
MERGE (d:Dataset {id: item.dataset_id})
  ON CREATE SET
    d.filename    = item.filename,
    d.uploaded_at = datetime()
MERGE (r:Row {dataset_id: item.dataset_id, row_index: item.row_index})
  ON CREATE SET r += item.properties
MERGE (d)-[:HAS_ROW]->(r)
"""

# Constraints ensure MERGE is atomic and duplicates are impossible
CONSTRAINTS = [
    "CREATE CONSTRAINT dataset_unique IF NOT EXISTS "
    "FOR (d:Dataset) REQUIRE d.id IS UNIQUE",
    "CREATE CONSTRAINT row_unique IF NOT EXISTS "
    "FOR (r:Row) REQUIRE (r.dataset_id, r.row_index) IS UNIQUE",
    "CREATE INDEX row_dataset_idx IF NOT EXISTS "
    "FOR (r:Row) ON (r.dataset_id)",
]


# ─────────────────────────────────────────────────────────────
# Connection helpers with exponential backoff
# ─────────────────────────────────────────────────────────────
def connect_kafka(retries: int = 15) -> KafkaConsumer:
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                "csv-rows",
                bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="loader-group-v1",
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logging.info("✅ Kafka connected")
            return consumer
        except NoBrokersAvailable:
            wait = 2 ** min(i, 5)
            logging.warning(f"Kafka not ready — retry {i+1}/{retries} in {wait}s")
            time.sleep(wait)
    raise RuntimeError("❌ Cannot connect to Kafka after all retries")


def connect_neo4j() -> GraphDatabase:
    uri      = os.environ["NEO4J_URI"]
    user     = os.environ["NEO4J_USER"]
    password = os.environ["NEO4J_PASSWORD"]
    db       = os.environ["NEO4J_DB"]

    for i in range(15):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()

            # Apply constraints and indexes on first start
            with driver.session(database=db) as session:
                for cql in CONSTRAINTS:
                    session.run(cql)
            logging.info("✅ Neo4j connected — constraints applied")
            return driver
        except (ServiceUnavailable, Exception) as e:
            wait = 2 ** min(i, 5)
            logging.warning(f"Neo4j not ready — retry {i+1}/15 in {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError("❌ Cannot connect to Neo4j after all retries")


# ─────────────────────────────────────────────────────────────
# Main consumer loop
# ─────────────────────────────────────────────────────────────
def sanitize_properties(raw_data: dict, dataset_id: str, row_index: int) -> dict:
    """Sanitizes property keys so Neo4j never encounters empty or null tokens."""
    clean = {}
    unnamed_idx = 1
    for k, v in raw_data.items():
        if k is None or not str(k).strip():
            k_name = f"unnamed_col_{unnamed_idx}"
            unnamed_idx += 1
        else:
            k_name = str(k).strip()
        # Remove null bytes and backticks
        k_name = k_name.replace("\x00", "").replace("`", "")
        if k_name:
            clean[k_name] = str(v).strip() if v is not None else ""

    clean["dataset_id"] = dataset_id
    clean["row_index"]  = int(row_index)
    return clean


def main():
    logging.info("🚀 Loader starting up...")

    consumer = connect_kafka()
    driver   = connect_neo4j()
    db       = os.environ["NEO4J_DB"]

    logging.info("📥 Consuming from topic: csv-rows (High-throughput batching)")

    while True:
        records_dict = consumer.poll(timeout_ms=300, max_records=250)
        if not records_dict:
            continue

        batch = []
        for tp, messages in records_dict.items():
            for message in messages:
                msg = message.value
                try:
                    props = sanitize_properties(
                        msg.get("data", {}),
                        msg["dataset_id"],
                        msg["row_index"],
                    )
                    batch.append({
                        "dataset_id": msg["dataset_id"],
                        "filename":   msg["filename"],
                        "row_index":  int(msg["row_index"]),
                        "properties": props,
                        "job_id":     msg.get("job_id"),
                    })
                except Exception as e:
                    logging.error(f"❌ Error preparing message: {e}")

        if batch:
            try:
                with driver.session(database=db) as session:
                    session.run(BATCH_MERGE_QUERY, {"batch": batch})
                logging.info(
                    f"✅ Batch merged {len(batch)} rows "
                    f"(dataset={batch[0]['dataset_id'][:8]} job={batch[0].get('job_id')})"
                )
            except Exception as e:
                logging.warning(f"⚠️ Batch merge failed ({e}) — falling back to row-by-row")
                with driver.session(database=db) as session:
                    for item in batch:
                        try:
                            session.run(MERGE_QUERY, item)
                        except Exception as row_err:
                            logging.error(f"❌ Row failed: {row_err}")


if __name__ == "__main__":
    main()
