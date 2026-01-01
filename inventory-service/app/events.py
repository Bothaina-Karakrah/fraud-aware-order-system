import os
import json
import uuid
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from app.db import SessionLocal
from app.models import Inventory, ProcessedEvent
from app.inventory import reserve_stock
from app.logging import get_logger

logger = get_logger()

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
        "event_type": event_type,
        "payload": payload,
        "trace_id": trace_id
    }
    producer = await get_producer()
    await producer.send(topic, value=event)

from uuid import UUID

async def handle_event(event: dict) -> None:
    event_id = event.get("event_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    order_id = payload.get("order_id")
    trace_id = payload.get("trace_id")

    if not event_id or not order_id:
        logger.warning(
            "Invalid Inputs",
            extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
        )
        return

    db = SessionLocal()
    try:
        # Convert event_id string to UUID
        try:
            event_uuid = UUID(event_id)
        except (ValueError, AttributeError, TypeError):
            logger.warning(
                f"Invalid event_id format: {event_id}",
                extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id,
                       "event_type": event_type}
            )
            return

        # 1. Idempotency Check
        if db.query(ProcessedEvent).filter_by(event_id=event_uuid).first():
            logger.info(
                "Event already processed",
                extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
            )
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
                    payload={"order_id": order_id},
                    trace_id=trace_id
                )
                logger.info(
                    f"Stock reserved - order {order_id}",
                    extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id,
                           "event_type": event_type}
                )
            else:
                # Saga Failure Path -> Trigger Refund in Payment Service
                await publish_event(
                    topic="order-events",
                    event_type="StockReservationFailed",
                    payload={"order_id": order_id, "reason": message},
                    trace_id=trace_id
                )
                logger.info(
                    f"Stock reservation failed - order {order_id}",
                    extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id,
                           "event_type": event_type}
                )

        # Use UUID object here
        db.add(ProcessedEvent(event_id=event_uuid, event_type=event_type))
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(
            f"Error handling event: {e}",
            extra={"service": "inventory-service", "trace_id": trace_id, "order_id": order_id, "event_type": event_type}
        )
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