import os
import json
import uuid
from datetime import datetime, timezone
from typing import Optional, List

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Order, OrderStatus, ProcessedEvent
from app.logging import get_logger
from app.metrics import (
    order_processing_duration_seconds,
    order_status_transitions_total,
    orders_confirmed_total,
    orders_canceled_total,
    kafka_consumer_lag,
    kafka_processing_errors,
    kafka_messages_processed
)

logger = get_logger()

# ======================
# Kafka Config
# ======================

_KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
_producer: Optional[AIOKafkaProducer] = None

async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=_KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        await _producer.start()
    return _producer

async def publish_event(*, topic: str, event_type: str, payload: dict, trace_id: str) -> None:
    event = {
        "event_id": str(uuid.uuid4()),
        "trace_id": trace_id,
        "event_type": event_type,
        "payload": payload,
    }
    producer = await get_producer()
    await producer.send(topic, value=event)
    logger.info(
        "Event published",
        extra={
            "service": "order_service",
            "trace_id": trace_id,
            "order_id": payload.get("order_id"),
            "event_type": event_type,
        },
    )

async def stop_producer() -> None:
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None

# ======================
# Event Handling Logic
# ======================

EVENT_STATE_MAP = {
    "OrderApproved": OrderStatus.APPROVED,
    "OrderBlocked": OrderStatus.CANCELED,
    "PaymentSucceeded": OrderStatus.PAID,
    "PaymentFailed": OrderStatus.CANCELED,
    "StockReserved": OrderStatus.CONFIRMED,
    "StockReservationFailed": OrderStatus.CANCELED,
    "RefundSucceeded": OrderStatus.REFUNDED,
}

FINAL_STATUSES = [OrderStatus.CONFIRMED, OrderStatus.CANCELED, OrderStatus.REFUNDED]

async def handle_event(event: dict, db: Optional[Session] = None) -> None:
    event_id = event.get("event_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    order_id = payload.get("order_id")
    trace_id = event.get("trace_id", str(uuid.uuid4()))

    if not event_id or not order_id:
        logger.warning(
            "Invalid Inputs",
            extra={"service": "order_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
        )
        kafka_processing_errors.labels(service="order_service").inc()
        return

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        # Convert event_id to UUID (matches DB schema)
        try:
            event_uuid = uuid.UUID(event_id)
        except (ValueError, TypeError, AttributeError):
            logger.warning(
                f"Invalid event_id format: {event_id}",
                extra={"service": "order_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )
            kafka_processing_errors.labels(service="order_service").inc()
            return

        # Idempotency Check
        if db.query(ProcessedEvent).filter_by(event_id=event_uuid).first():
            logger.info(
                "Event already processed",
                extra={"service": "order_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )
            kafka_messages_processed.labels(service="order_service").inc()
            return

        # Check if order exists
        order = db.query(Order).filter_by(order_id=order_id).first()
        if not order:
            logger.warning(
                "Order not found",
                extra={"service": "order_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )
            kafka_processing_errors.labels(service="order_service").inc()
            return

        # Update Status
        prev_status = order.status
        new_status = EVENT_STATE_MAP.get(event_type)
        if new_status:
            order.status = new_status
            logger.info(
                f"Order {order_id} status changed from {prev_status} to {new_status}",
                extra={"service": "order_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )
            # Metrics
            order_status_transitions_total.labels(status=str(new_status.value)).inc()
            if new_status == OrderStatus.APPROVED:
                orders_confirmed_total.inc()
            if new_status == OrderStatus.CANCELED:
                orders_canceled_total.labels(reason=event_type).inc()

            if order.status in FINAL_STATUSES:
                duration_seconds = (datetime.now(timezone.utc) - order.created_at).total_seconds()
                order_processing_duration_seconds.observe(duration_seconds)
                logger.info(f"Order processing duration: {duration_seconds} seconds")

        # Handle refund requests
        if event_type == "StockReservationFailed" and prev_status == OrderStatus.PAID:
            await publish_event(
                topic="payment-events",
                event_type="RefundRequested",
                payload={
                    "order_id": str(order.order_id),
                    "amount": float(order.amount)
                },
                trace_id=trace_id
            )
            logger.info(
                f"RefundRequested for order {order_id}",
                extra={"service": "order_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )

        # Mark as processed
        db.add(ProcessedEvent(event_id=event_uuid, event_type=event_type))
        db.commit()
        kafka_messages_processed.labels(service="order_service").inc()

    except Exception as e:
        db.rollback()
        logger.error(
            f"Error handling event: {e}",
            extra={"service": "order_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
        )
        kafka_processing_errors.labels(service="order_service").inc()
    finally:
        if close_db:
            db.close()

# ======================
# Kafka Consumer
# ======================

async def start_consumer() -> None:
    consumer = AIOKafkaConsumer(
        "order-events",
        "payment-events",  # Added to hear about payment/refund results
        "inventory-events",  # Added to hear about stock results,
        bootstrap_servers=_KAFKA_SERVERS,
        group_id="order_service_group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True
    )
    await consumer.start()

    try:
        async for msg in consumer:
            # Compute lag per partition
            tp = TopicPartition(msg.topic, msg.partition)
            committed = await consumer.committed(tp)
            lag = msg.offset - committed if committed is not None else 0
            kafka_consumer_lag.labels(service="order_service").set(lag)

            await handle_event(msg.value)
    finally:
        await consumer.stop()