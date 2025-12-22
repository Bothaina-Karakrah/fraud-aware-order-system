import os
import json
import uuid
from typing import Optional, Type, Dict

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from sqlalchemy.orm import Session

from .db import SessionLocal
from .models import ProcessedEvent

# Producer singleton
producer: Optional[AIOKafkaProducer] = None


def get_kafka_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


async def get_producer() -> AIOKafkaProducer:
    """
    Return a singleton Kafka producer, start it if not started.
    """
    global producer
    if producer is None:
        producer = AIOKafkaProducer(
            bootstrap_servers=get_kafka_servers(),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()
    return producer


async def stop_producer() -> None:
    """
    Stop Kafka producer gracefully.
    """
    global producer
    if producer:
        await producer.stop()
        producer = None
        print("🛑 Kafka producer stopped")


async def publish_event(event_type: str, data: dict, topic: str) -> None:
    """
    Publish an event to the given Kafka topic.
    """
    p = await get_producer()
    data["event_type"] = event_type
    if "event_id" not in data:
        data["event_id"] = str(uuid.uuid4())
    await p.send(topic, value=data)
    print(f"✅ Published {event_type} with event_id {data['event_id']} to {topic}")


async def handle_event(
        event: dict,
        db: Optional[Session] = None,
        model: Optional[Type] = None,
        state_map: Optional[Dict[str, enum.Enum]] = None,
        idempotency_model: Type = ProcessedEvent,
) -> None:
    """
    Generic event handler for any service.

    Parameters:
    - event: the incoming event dict
    - db: optional SQLAlchemy session (for testing)
    - model: the SQLAlchemy model to update (e.g., Order, Transaction)
    - state_map: dict mapping event_type -> enum status
    - idempotency_model: table to track processed events
    """
    if not model or not state_map:
        raise ValueError("model and state_map must be provided")

    event_type = event.get("event_type")
    event_id = event.get("event_id")
    entity_id_str = event.get("id") or event.get("order_id") or event.get("transaction_id")

    # Validate IDs
    if not event_id or not entity_id_str:
        print("❌ Missing event_id or entity_id in event")
        return

    try:
        entity_uuid = uuid.UUID(entity_id_str)
    except (ValueError, TypeError, AttributeError):
        print(f"❌ Invalid entity_id {entity_id_str}")
        return

    # Determine session
    close_session = False
    if db is None:
        db = SessionLocal()
        close_session = True

    try:
        # Idempotency check
        if db.query(idempotency_model).filter_by(event_id=event_id).first():
            print(f"⚠️ Duplicate event {event_id}, skipping")
            return

        # Fetch entity
        entity = db.query(model).filter_by(id=entity_uuid).first()
        if not entity:
            print(f"❌ Entity {entity_uuid} not found in {model.__tablename__}")
            return

        # Apply state transition
        if event_type in state_map:
            new_status = state_map[event_type]
            old_status = getattr(entity, "status", None)
            setattr(entity, "status", new_status)
            print(f"🔄 {model.__name__} {entity_uuid} status: {old_status} -> {new_status}")
        else:
            print(f"⚠️ Unknown event_type {event_type}, skipping state update")

        # Mark event as processed
        db.add(idempotency_model(event_id=event_id, event_type=event_type))
        db.commit()
        print(f"✅ Event {event_type} processed for {entity_uuid}")

    except Exception as e:
        db.rollback()
        print(f"❌ Error processing event {event_id}: {e}")
    finally:
        if close_session:
            db.close()


async def start_consumer(
        topic: str,
        group_id: str,
        model: Type,
        state_map: Dict[str, enum.Enum],
        idempotency_model: Type = ProcessedEvent,
) -> None:
    """
    Start Kafka consumer and handle events indefinitely.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=get_kafka_servers(),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=group_id,
    )
    await consumer.start()
    print(f"🟢 Kafka consumer started for topic {topic}")

    try:
        async for msg in consumer:
            event = msg.value
            await handle_event(event, model=model, state_map=state_map, idempotency_model=idempotency_model)
    except Exception as e:
        print(f"❌ Consumer error: {e}")
    finally:
        await consumer.stop()
        print(f"🛑 Kafka consumer stopped for topic {topic}")