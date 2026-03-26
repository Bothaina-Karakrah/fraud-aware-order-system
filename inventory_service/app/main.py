"""
INVENTORY SERVICE
=================
Manages stock levels and inventory reservations for products.

Responsibilities:
  - Reserve inventory when orders are created
  - Update stock levels after order fulfillment
  - Handle stock availability checks
  - Manage inventory releases (cancellations/returns)

Event Flow:
  1. Subscribes to "OrderCreated" events from Order Service
  2. Checks product availability and reserves stock
  3. Publishes "InventoryReserved" event back to Kafka
  4. Updates inventory on order completion/cancellation

API Endpoints:
  GET    /inventory/{product_id}  Get product stock level
  POST   /inventory/reserve        Reserve items for order (via Kafka)
  POST   /inventory/release        Release reserved items (via Kafka)

Dependencies:
  - Kafka: Event consumption and publishing
  - PostgreSQL (inventory-db): Store product stock data
  - Prometheus: Metrics collection
"""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api import router
from app.db import init_db, Base, engine
from app.events import stop_producer, start_consumer
from app.logging import get_logger
from prometheus_client import make_asgi_app

logger = get_logger()

@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    Application lifecycle management:
    - Startup: Initialize DB, start Kafka consumer
    - Shutdown: Gracefully stop consumer and producer
    """
    # --- Startup ---
    logger.info("Starting Inventory Service...")
    Base.metadata.create_all(bind=engine)
    init_db()

    # Start Kafka consumer in background to listen for orders
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("Kafka consumer started")

    yield

    # --- Shutdown ---
    logger.info("Shutting down Inventory Service...")
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    await stop_producer()
    logger.info("Kafka producer stopped")


# --- Create FastAPI app ---
app = FastAPI(title="Inventory Service", lifespan=lifespan)

# Mount Prometheus metrics endpoint at /metrics for monitoring
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Include REST API routes for inventory endpoints
app.include_router(router)