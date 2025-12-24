import os
import json
import uuid
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from sqlalchemy.orm import Session

from db import SessionLocal
from models import Inventory, ProcessedEvent


# ======================
# Kafka
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

async def handle_event(event: dict, db: Optional[Session] = None) -> None:
    event_id = event.get("event_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})

    product_id = payload.get("product_id")
    if not event_id or not product_id:
        return

    try:
        product_uuid = uuid.UUID(product_id)
    except ValueError:
        return

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        if db.query(ProcessedEvent).filter_by(event_id=event_id).first():
            return

            # Lock the row during this transaction
        inv = db.query(Inventory).filter(
                Inventory.product_id == product_id
            ).with_for_update().first()

        quantity = payload.get("quantity")

        if not inv or not quantity:
            return

        if inv.available_quantity < quantity:
            return

        # Update stock
        inv.available_quantity -= quantity
        inv.reserved_quantity += quantity
        db.commit()

        db.add(ProcessedEvent(event_id=event_id, event_type=event_type))
        db.commit()

    except Exception:
        db.rollback()
        raise
    finally:
        if close_db:
            db.close()


async def start_consumer() -> None:
    consumer = AIOKafkaConsumer(
        "fraud-payment-events",
        bootstrap_servers=_KAFKA_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode()),
        group_id="inventory-group",
    )

    await consumer.start()
    try:
        async for msg in consumer:
            await handle_event(msg.value)
    finally:
        await consumer.stop()