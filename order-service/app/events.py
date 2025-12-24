import os
import json
import uuid
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Order, OrderStatus, ProcessedEvent


# ======================
# Kafka Config
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
# State Machine & Logic
# ======================

EVENT_STATE_MAP = {
    "OrderApproved": OrderStatus.APPROVED,
    "OrderBlocked": OrderStatus.CANCELED,
    "PaymentSucceeded": OrderStatus.PAID,
    "PaymentFailed": OrderStatus.CANCELED,
    "StockReserved": OrderStatus.CONFIRMED,
    "StockReservationFailed": OrderStatus.CANCELED, # Trigger compensation
    "RefundSucceeded": OrderStatus.REFUNDED,
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

        # 2. Find Order
        order = db.query(Order).filter_by(order_id=order_id).first()
        if not order:
            return

        # 3. Update Status
        new_status = EVENT_STATE_MAP.get(event_type)
        if new_status:
            order.status = new_status

        # 4. COMPENSATION LOGIC: If stock fails but we were already PAID, request refund
        if event_type == "StockReservationFailed" and order.status == OrderStatus.PAID:
            await publish_event(
                topic="payment-events",
                event_type="RefundRequested",
                payload={"order_id": str(order.order_id), "amount": float(order.amount)}
            )

        db.add(ProcessedEvent(event_id=event_id, event_type=event_type))
        db.commit()

    except Exception as e:
        db.rollback()
        print(f"Error handling event: {e}")
    finally:
        if close_db:
            db.close()


async def start_consumer() -> None:
    consumer = AIOKafkaConsumer(
        "order-events",
        "payment-events", # Added to hear about payment/refund results
        "inventory-events", # Added to hear about stock results
        bootstrap_servers=_KAFKA_SERVERS,
        group_id="order-service-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await handle_event(msg.value)
    finally:
        await consumer.stop()