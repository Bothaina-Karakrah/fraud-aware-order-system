import os
import json
import uuid
import time
from typing import Optional
from uuid import UUID
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Transaction, PaymentStatus, ProcessedEvent
from app.fraud import evaluate_fraud
from app.logging import get_logger
from app.metrics import (
    fraud_check_duration_seconds,
    fraud_decisions,
    payment_failures,
    payment_refunds_total,
    kafka_consumer_lag,
    kafka_processing_errors,
    kafka_messages_processed
)
from app.payment import process_payment, process_refund

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
            "service": "fraud_payment_service",
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
# Consumer Logic
# ======================

EVENT_STATE_MAP = {
    "OrderCreated": PaymentStatus.PENDING,
    "RefundRequested": PaymentStatus.REFUNDED,
}


async def handle_event(event: dict, db: Optional[Session] = None) -> None:
    event_id_str = event.get("event_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    order_id = payload.get("order_id")
    trace_id = event.get("trace_id", str(uuid.uuid4()))

    if not event_id_str or not order_id:
        logger.warning(
            "Invalid Inputs",
            extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
        )
        kafka_processing_errors.labels(service="fraud_payment_service").inc()
        return

    # Convert event_id string to UUID for idempotency
    try:
        event_id = UUID(event_id_str)
    except (ValueError, TypeError):
        logger.warning(
            f"Invalid event_id format: {event_id_str}",
            extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
        )
        kafka_processing_errors.labels(service="fraud_payment_service").inc()
        return

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        # Idempotency check
        if db.query(ProcessedEvent).filter_by(event_id=event_id).first():
            logger.info(
                "Event already processed",
                extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )
            return

        if event_type == "OrderCreated":
            logger.info(
                f"Processing OrderCreated: {order_id}",
                extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )

            # Fraud check
            start = time.perf_counter()
            fraud_result = evaluate_fraud(db, payload)
            duration_seconds = time.perf_counter() - start
            fraud_check_duration_seconds.observe(duration_seconds)
            fraud_decisions.labels(decision=fraud_result["decision"]).inc()
            logger.info(f"Fraud check duration: {duration_seconds} seconds")

            # Create transaction
            transaction = Transaction(
                order_id=UUID(order_id),
                user_id=UUID(payload.get("user_id")),
                amount=payload.get("amount"),
                payment_method=payload.get("payment_method"),
                fraud_decision=fraud_result["decision"],
                fraud_score=fraud_result["score"],
                status=PaymentStatus.PENDING,
                idempotency_key=event_id
            )
            db.add(transaction)
            db.commit()

            # Publish result
            if fraud_result["decision"].value == "BLOCK":
                await publish_event(
                    topic="order-events",
                    event_type="OrderBlocked",
                    payload={"order_id": order_id, "reason": fraud_result["reason"]},
                    trace_id=trace_id,
                )
                logger.info(
                    f"Order blocked: {order_id}",
                    extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type},
                )
            else:
                await publish_event(
                    topic="order-events",
                    event_type="OrderApproved",
                    payload={"order_id": order_id},
                    trace_id=trace_id,
                )
                # Process payment
                try:
                    await process_payment(db, payload, trace_id)
                    db.commit()
                except Exception:
                    db.rollback()
                    payment_failures.labels(reason="PROCESSING_ERROR").inc()
                    logger.error(f"Payment processing failed for order {order_id}",
                                 extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id})

        elif event_type == "RefundRequested":
            payment_refunds_total.inc()
            logger.info(f"Processing RefundRequested: {order_id}", extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id})
            try:
                await process_refund(db, order_id, trace_id)
                db.commit()
            except Exception:
                db.rollback()
                kafka_processing_errors.labels(service="fraud_payment_service").inc()
                logger.error(f"Refund processing failed for order {order_id}", extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id})

        # Mark event as processed
        db.add(ProcessedEvent(event_id=event_id, event_type=event_type))
        db.commit()
        kafka_messages_processed.labels(service="fraud_payment_service").inc()

    except Exception:
        db.rollback()
        logger.error(f"Error handling event {event_id}", extra={"service": "fraud_payment_service", "trace_id": trace_id, "order_id": order_id})
    finally:
        if close_db:
            db.close()


async def start_consumer() -> None:
    consumer = AIOKafkaConsumer(
        "order-events",
        "payment-events",
        bootstrap_servers=_KAFKA_SERVERS,
        group_id="fraud_payment_service-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            tp = TopicPartition(msg.topic, msg.partition)
            committed = await consumer.committed(tp)
            lag = msg.offset - committed if committed is not None else 0
            kafka_consumer_lag.labels(service="fraud_payment_service").set(lag)
            await handle_event(msg.value)
    finally:
        await consumer.stop()