import os
import json
import uuid
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from app.db import SessionLocal
from app.models import Inventory, ProcessedEvent
from app.inventory import reserve_stock # Import your logic

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

async def publish_event(*, topic: str, event_type: str, payload: dict) -> None:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "payload": payload,
    }
    producer = await get_producer()
    await producer.send(topic, value=event)

async def handle_event(event: dict) -> None:
    event_id = event.get("event_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    order_id = payload.get("order_id")

    if not event_id or not order_id:
        return

    db = SessionLocal()
    try:
        # 1. Idempotency Check
        if db.query(ProcessedEvent).filter_by(event_id=event_id).first():
            return

        # 2. React to Successful Payment
        if event_type == "PaymentSucceeded":
            # Attempt to reserve stock using your with_for_update logic
            success, message = reserve_stock(
                product_id=payload.get("product_id"),
                quantity=payload.get("quantity"),
                db=db
            )

            if success:
                # Saga Success Path
                await publish_event(
                    topic="order-events",
                    event_type="StockReserved",
                    payload={"order_id": order_id}
                )
            else:
                # Saga Failure Path -> Trigger Refund in Payment Service
                await publish_event(
                    topic="order-events",
                    event_type="StockReservationFailed",
                    payload={"order_id": order_id, "reason": message}
                )

        db.add(ProcessedEvent(event_id=event_id, event_type=event_type))
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Inventory Error: {e}")
    finally:
        db.close()

async def start_consumer() -> None:
    consumer = AIOKafkaConsumer(
        "order-events",
        bootstrap_servers=_KAFKA_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode()),
        group_id="inventory-service-group",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await handle_event(msg.value)
    finally:
        await consumer.stop()