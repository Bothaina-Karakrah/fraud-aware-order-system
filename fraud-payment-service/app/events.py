import os
import json
import uuid
from typing import Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Transaction, PaymentStatus, ProcessedEvent
from app.fraud import evaluate_fraud
from app.logging import get_logger

logger = get_logger()

# ======================
# Publisher
# ======================

_KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)
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


async def stop_producer() -> None:
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None


# ======================
# Consumer
# ======================

EVENT_STATE_MAP = {
    "OrderCreated": PaymentStatus.PENDING,
    "RefundRequested": PaymentStatus.REFUNDED,
}

async def handle_event(event: dict, db: Optional[Session] = None) -> None:
    import traceback
    from app.payment import process_payment, process_refund

    event_id = event.get("event_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    order_id = payload.get("order_id")
    trace_id = event.get("trace_id")

    if not event_id or not order_id:
        logger.warning(
            f"Invalid Inputs",
            extra={"service": "fraud-payment-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
        )
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
                extra={"service": "fraud-payment-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )
            return

        if event_type == "OrderCreated":
            logger.info(
                f"Processing OrderCreated: {order_id}",
                extra={"service": "fraud-payment-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )
            # Fraud check
            fraud_result = evaluate_fraud(db, payload)

            # Create transaction
            transaction = Transaction(
                order_id=uuid.UUID(order_id),
                user_id=uuid.UUID(payload.get("user_id")),
                amount=payload.get("amount"),
                payment_method=payload.get("payment_method"),
                fraud_decision=fraud_result["decision"],
                fraud_score=fraud_result["score"],
                status=PaymentStatus.PENDING,
                idempotency_key=event_id
            )
            db.add(transaction)
            db.commit()  # Commit immediately so transaction exists before payment

            # Publish result
            if fraud_result["decision"].value == "BLOCK":
                await publish_event(
                    topic="order-events",
                    event_type="OrderBlocked",
                    payload={"order_id": order_id, "reason": fraud_result["reason"]},
                    trace_id=trace_id,
                )
                logger.info(
                    f"Block order - {order_id}",
                    extra={
                        "service": "fraud-payment-service",
                        "trace_id": trace_id,
                        "order_id": order_id,
                        "event_type": event_type,
                    },
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
                    db.commit()  # Commit payment changes
                except Exception:
                    db.rollback()
                    logger.info(
                        f"Payment processing failed for order {order_id}",
                        extra={
                            "service": "fraud-payment-service",
                            "trace_id": trace_id,
                            "order_id": order_id,
                            "event_type": event_type,
                        },
                    )

        elif event_type == "RefundRequested":
            logger.info(
                f"Processing RefundRequested: {order_id}",
                extra={
                    "service": "fraud-payment-service",
                    "trace_id": trace_id,
                    "order_id": order_id,
                    "event_type": event_type,
                },
            )
            try:
                await process_refund(db, order_id, trace_id)
                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(
                    f"Error handling event: {e}",
                    extra={"service": "fraud-payment-service", "trace_id": trace_id, "order_id": order_id,
                           "event_type": event_type}
                )

        # Mark event processed
        db.add(ProcessedEvent(event_id=event_id, event_type=event_type))
        db.commit()

    except Exception:
        db.rollback()
        logger.info(
            f"Error handling event {event_id}",
            extra={
                "service": "fraud-payment-service",
                "trace_id": trace_id,
                "order_id": order_id,
                "event_type": event_type,
            },
        )
    finally:
        if close_db:
            db.close()

async def start_consumer() -> None:
    consumer = AIOKafkaConsumer(
        "order-events",
        "payment-events",
        bootstrap_servers=_KAFKA_SERVERS,
        group_id="fraud-payment-service-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await handle_event(msg.value)
    finally:
        await consumer.stop()