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
from kafka.errors import NoBrokersAvailable
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
def main():
    logging.info("🚀 Loader starting up...")

    consumer = connect_kafka()
    driver   = connect_neo4j()
    db       = os.environ["NEO4J_DB"]

    logging.info("📥 Consuming from topic: csv-rows")

    for message in consumer:
        msg = message.value
        try:
            properties = {
                **msg["data"],
                "dataset_id": msg["dataset_id"],
                "row_index":  int(msg["row_index"]),
            }
            with driver.session(database=db) as session:
                session.run(MERGE_QUERY, {
                    "dataset_id": msg["dataset_id"],
                    "filename":   msg["filename"],
                    "row_index":  int(msg["row_index"]),
                    "properties": properties,
                })
            logging.info(
                f"✅ Merged job={msg['job_id']} "
                f"row={msg['row_index']} "
                f"dataset={msg['dataset_id'][:8]}"
            )
        except Exception as e:
            logging.error(
                f"❌ Failed job={msg.get('job_id')} "
                f"row={msg.get('row_index')}: {e}"
            )


if __name__ == "__main__":
    main()
