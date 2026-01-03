import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.db import init_db, Base, engine
from app.events import start_consumer, stop_producer
from app.logging import get_logger
from prometheus_client import make_asgi_app

logger = get_logger()

@asynccontextmanager
async def lifespan(_app: FastAPI):
    # --- Startup ---
    logger.info("Starting Fraud Payment Service...")
    Base.metadata.create_all(bind=engine)
    init_db()

    # Start Kafka consuming - handles Fraud Check, Payment, and Refunds
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("Kafka consumer started")
    yield
    # --- Shutdown ---
    logger.info("Shutting down Fraud Payment Service...")
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    await stop_producer()
    logger.info("Kafka producer stopped")

# --- Create FastAPI app ---
app = FastAPI(title="Fraud & Payment Service", lifespan=lifespan)

# Mount Prometheus metrics endpoint at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}