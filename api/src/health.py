"""
health.py — GET /health
Reports kafka_connected and neo4j_connected genuinely.
Must return status != "ok" until BOTH are truly reachable.
"""

import os
import logging
from fastapi import APIRouter
from kafka import KafkaProducer
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    try:
        from kafka.errors import BrokerNotAvailableError as NoBrokersAvailable
    except ImportError:
        class NoBrokersAvailable(Exception): pass

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

router = APIRouter()


def _check_kafka() -> bool:
    try:
        p = KafkaProducer(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
            request_timeout_ms=3000,
            max_block_ms=3000,
        )
        p.close()
        return True
    except Exception:
        return False


def _check_neo4j() -> bool:
    try:
        driver = GraphDatabase.driver(
            os.environ["NEO4J_URI"],
            auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
            connection_timeout=3.0,
        )
        driver.verify_connectivity()
        driver.close()
        return True
    except Exception:
        return False


@router.get("/health")
def health():
    """
    Returns ok only when both Kafka and Neo4j are genuinely reachable.
    A container being 'started' is NOT the same as being ready.
    """
    kafka_ok = _check_kafka()
    neo4j_ok = _check_neo4j()
    status   = "ok" if (kafka_ok and neo4j_ok) else "degraded"

    return {
        "status":          status,
        "kafka_connected": kafka_ok,
        "neo4j_connected": neo4j_ok,
    }
