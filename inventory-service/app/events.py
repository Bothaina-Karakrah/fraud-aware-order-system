import os
import json
import uuid
import time
from typing import Optional
from uuid import UUID

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from app.db import SessionLocal
from app.models import Inventory, ProcessedEvent
from app.inventory import reserve_stock
from app.logging import get_logger
from app.metrics import (
    inventory_reservation_duration_seconds,
    inventory_reservation_failures,
    inventory_reservations_total,
    kafka_consumer_lag,
    kafka_processing_errors,
    kafka_messages_processed
)

logger = get_logger()
_KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
_producer: Optional[AIOKafkaProducer] = None

# ----------------------
# Producer
# ----------------------
async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=_KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        await _producer.start()
    return _producer

async def publish_event(topic: str, event_type: str, payload: dict, trace_id: str):
    event = {
        "event_id": str(uuid.uuid4()),
        "trace_id": trace_id,
        "event_type": event_type,
        "payload": payload,
    }
    producer = await get_producer()
    await producer.send(topic, value=event)

async def stop_producer():
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None

# ----------------------
# Event Handler
# ----------------------
async def handle_event(event: dict):
    event_id = event.get("event_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    order_id = payload.get("order_id")
    trace_id = payload.get("trace_id")

    if not event_id or not order_id:
        logger.warning("Invalid Inputs", extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type})
        kafka_processing_errors.labels(service="inventory-service").inc()
        return

    with SessionLocal() as db:
        # Validate UUID
        try:
            event_uuid = UUID(event_id)
        except (ValueError, TypeError):
            logger.warning(f"Invalid event_id format: {event_id}", extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type})
            kafka_processing_errors.labels(service="inventory-service").inc()
            return

        # Idempotency
        if db.query(ProcessedEvent).filter_by(event_id=event_uuid).first():
            logger.info("Event already processed", extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type})
            kafka_messages_processed.labels(service="inventory-service").inc()
            return

        # Process PaymentSucceeded
        if event_type == "PaymentSucceeded":
            start = time.perf_counter()
            inventory_reservations_total.inc()
            try:
                success, message = reserve_stock(
                    product_id=payload.get("product_id"),
                    quantity=payload.get("quantity"),
                    db=db
                )
                if success:
                    await publish_event(topic="order-events", event_type="StockReserved", payload={"order_id": order_id}, trace_id=trace_id)
                    logger.info(f"Stock reserved - order {order_id}", extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type})
                else:
                    inventory_reservation_failures.labels(reason="StockReservationFailed").inc()
                    await publish_event(topic="order-events", event_type="StockReservationFailed", payload={"order_id": order_id, "reason": message}, trace_id=trace_id)
                    logger.info(f"Stock reservation failed - order {order_id}", extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type})
            except Exception as e:
                inventory_reservation_failures.labels(reason="exception").inc()
                kafka_processing_errors.labels(service="inventory-service").inc()
                logger.error(f"Exception during stock reservation: {e}", extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type})
                raise
            finally:
                duration = time.perf_counter() - start
                inventory_reservation_duration_seconds.observe(duration)
                logger.info(f"Order processing duration: {duration} seconds")

        # Mark as processed
        db.add(ProcessedEvent(event_id=event_uuid, event_type=event_type))
        db.commit()
        kafka_messages_processed.labels(service="inventory-service").inc()

# ----------------------
# Consumer
# ----------------------
async def start_consumer():
    consumer = AIOKafkaConsumer(
        "order-events",
        bootstrap_servers=_KAFKA_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode()),
        group_id="inventory-service-group",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            tp = TopicPartition(msg.topic, msg.partition)
            committed = await consumer.committed(tp)
            lag = msg.offset - committed if committed is not None else 0
            kafka_consumer_lag.labels(service="inventory-service").set(lag)
            await handle_event(msg.value)
    finally:
        await consumer.stop()