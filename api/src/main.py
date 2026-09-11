"""
main.py — FastAPI application entry point
Wires all routers and warms up the Ollama SLM on startup
"""

import os
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.ingest import router as ingest_router
from src.health import router as health_router
from src.chat   import router as chat_router
from src.schema import router as schema_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API] %(levelname)s — %(message)s"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Warm up the Ollama SLM so first chat isn't slow."""
    if os.getenv("CHATBOT_MODE") == "slm":
        logging.info("🔥 Warming up Ollama SLM...")
        try:
            async with httpx.AsyncClient(timeout=90.0) as client:
                await client.post(
                    f"{os.getenv('OLLAMA_URL')}/api/generate",
                    json={
                        "model":  os.getenv("OLLAMA_MODEL"),
                        "prompt": "Return 1",
                        "stream": False,
                    },
                )
            logging.info("✅ Ollama SLM warm")
        except Exception as e:
            logging.warning(f"⚠️  Ollama warmup failed (non-fatal): {e}")
    yield


app = FastAPI(
    title="CSV Graph API",
    description="Dynamic CSV ingestion → Kafka → Neo4j → Grounded chatbot",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingest_router)
app.include_router(health_router)
app.include_router(chat_router)
app.include_router(schema_router)
