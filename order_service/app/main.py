"""
ORDER SERVICE
=============
Main entry point for customer order processing.

Responsibilities:
  - Create new orders
  - Retrieve order details
  - Update order status based on fraud/inventory results
  - Orchestrate communication between Fraud & Inventory services

Event Flow:
  1. API: Customer submits order → "OrderCreated" event published to Kafka
  2. Listens for "FraudCheckResult" from Fraud Service
  3. Listens for "InventoryReserved" from Inventory Service
  4. Updates order status once both checks complete

API Endpoints:
  POST   /orders              Create new order
  GET    /orders/{order_id}   Get order details
  GET    /orders              List all orders

Dependencies:
  - Kafka: Event communication with other services
  - PostgreSQL (order-db): Store order records
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
    - Startup: Initialize DB, start Kafka consumer for event listening
    - Shutdown: Gracefully stop consumer and producer
    """
    # --- Startup ---
    logger.info("Starting Order Service...")
    Base.metadata.create_all(bind=engine)
    init_db()

    # Start Kafka consumer in background to listen for fraud/inventory results
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("Kafka consumer started")

    yield

    # --- Shutdown ---
    logger.info("Shutting down Order Service...")
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    await stop_producer()
    logger.info("Kafka producer stopped")


# --- Create FastAPI app ---
app = FastAPI(title="Order Service", lifespan=lifespan)

# Mount Prometheus metrics endpoint at /metrics for monitoring
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Include REST API routes for order management
app.include_router(router)