import asyncio
import random
import uuid

from sqlalchemy.orm import Session
from app.models import Transaction, PaymentStatus
from app.metrics import (payment_failures, payment_attempts_total)

async def process_payment(db: Session, order_data: dict, trace_id:str) -> bool:
    """
    Simulates a payment gateway charge and records it in the DB.
    """
    from app.events import publish_event
    order_id = order_data.get("order_id")
    amount = order_data.get("amount")

    # Simulate External Gateway Latency (Requirement: < 2s)
    await asyncio.sleep(random.uniform(0.5, 1.5))

    # Simulate Success/Failure (e.g., 90% success)
    payment_successful = random.random() < 0.90

    status = PaymentStatus.SUCCESS if payment_successful else PaymentStatus.FAILED

    # Update/Create Transaction Record
    transaction = db.query(Transaction).filter_by(order_id=uuid.UUID(order_id)).first()

    if transaction:
        transaction.status = status
    else:
        # Fallback if fraud step didn't create it
        transaction = Transaction(
            order_id=uuid.UUID(order_id),
            user_id=uuid.UUID(order_data.get("user_id")),
            amount=amount,
            payment_method=order_data.get("payment_method"),
            status=status,
            idempotency_key=str(uuid.uuid4())
        )
        db.add(transaction)

    db.commit()

    payment_attempts_total.inc()
    # Emit the Result
    if payment_successful:
        await publish_event(
            topic="order-events",
            event_type="PaymentSucceeded",
            payload={"order_id": order_id, "product_id": order_data.get("product_id"), "quantity": order_data.get("quantity")},
            trace_id=trace_id
        )
    else:
        payment_failures.labels(reason="insufficient_funds").inc()
        await publish_event(
            topic="order-events",
            event_type="PaymentFailed",
            payload={"order_id": order_id, "reason": "insufficient_funds"},
            trace_id=trace_id
        )

    return payment_successful


async def process_refund(db: Session, order_id: str, trace_id: str):
    """
    Handles the compensating transaction if inventory fails later.
    """
    transaction = db.query(Transaction).filter_by(order_id=uuid.UUID(order_id)).first()
    if transaction and transaction.status == PaymentStatus.SUCCESS:
        # Simulate refund delay
        await asyncio.sleep(0.5)
        transaction.status = PaymentStatus.REFUNDED
        db.commit()

        await publish_event(
            topic="order-events",
            event_type="RefundSucceeded",
            payload={"order_id": order_id},
            trace_id=trace_id
        )