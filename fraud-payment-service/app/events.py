import os
import json
import uuid
from typing import Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from sqlalchemy.orm import Session

from db import SessionLocal
from models import Transaction, PaymentStatus, ProcessedEvent
from fraud import evaluate_fraud # Import your logic
from payment import process_payment, process_refund

# ======================
# Publisher
# ======================

_KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
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


async def publish_event(*, topic: str, event_type: str, payload: dict) -> None:
    event = {
        "event_id": str(uuid.uuid4()),
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
    event_id = event.get("event_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    order_id = payload.get("order_id")

    if not event_id or not order_id:
        return

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        # 1. Idempotency Check
        if db.query(ProcessedEvent).filter_by(event_id=event_id).first():
            return

        # 2. Handle OrderCreated (The Start of this service's job)
        if event_type == "OrderCreated":
            # Run Fraud Logic
            fraud_result = evaluate_fraud(db, payload)

            # Create the transaction record
            transaction = Transaction(
                order_id=uuid.UUID(order_id),
                user_id=uuid.UUID(payload.get("user_id")),
                amount=payload.get("amount"),
                payment_method=payload.get("payment_method"),
                fraud_decision=fraud_result["decision"],
                fraud_score=fraud_result["score"],
                status=PaymentStatus.PENDING,
                idempotency_key=event_id  # Use event_id as idempotency key
            )
            db.add(transaction)

            # 3. Emit Result to Order Service
            if fraud_result["decision"].value == "BLOCK":
                await publish_event(
                    topic="order-events",
                    event_type="OrderBlocked",
                    payload={"order_id": order_id, "reason": fraud_result["reason"]}
                )
            else:
                # Approved!
                await publish_event(
                    topic="order-events",
                    event_type="OrderApproved",
                    payload={"order_id": order_id}
                )
                # Execute Payment
                success = await process_payment(db, payload)

        # 5. Handle Compensation (RefundRequested)
        elif event_type == "RefundRequested":
            await process_refund(db, order_id)

        db.add(ProcessedEvent(event_id=event_id, event_type=event_type))
        db.commit()

    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
    finally:
        if close_db:
            db.close()


# Update consumer to use a unique group_id for this service
async def start_consumer() -> None:
    consumer = AIOKafkaConsumer(
        "order-events",  # Listen for OrderCreated
        "payment-events",  # Listen for RefundRequested
        bootstrap_servers=_KAFKA_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode()),
        group_id="payment-service-group",
    )

    await consumer.start()
    try:
        async for msg in consumer:
            await handle_event(msg.value)
    finally:
        await consumer.stop()