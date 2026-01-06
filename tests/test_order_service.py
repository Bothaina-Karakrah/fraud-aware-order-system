import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

import uuid
from datetime import datetime, timezone

from order_service.app.models import Order, OrderStatus, ProcessedEvent
from order_service.app.order_service import handle_event

# ----------------------------
# Fixtures
# ----------------------------

@pytest.fixture
def mock_db():
    """Mock SQLAlchemy session."""
    db = MagicMock()
    db.query().filter_by().first.return_value = None  # Default: no processed event, no order
    return db

@pytest.fixture
def existing_order(mock_db):
    """Return a mock order existing in DB."""
    order = Order(
        order_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        amount=100.0,
        status=OrderStatus.CREATED,
        created_at=datetime.now(timezone.utc)
    )
    # Mock the query to return this order
    mock_db.query().filter_by().first.return_value = order
    return order

# ----------------------------
# Tests
# ----------------------------

@pytest.mark.asyncio
async def test_order_approved_status(existing_order, mock_db, monkeypatch):
    """Test handling of OrderApproved event changes status correctly."""
    # Mock publish_event so no actual Kafka call
    fake_publish = AsyncMock()
    monkeypatch.setattr("app.order_service.publish_event", fake_publish)

    # Create a sample event
    trace_id = str(uuid.uuid4())
    event = {
        "event_id": str(uuid.uuid4()),
        "trace_id": trace_id,
        "event_type": "OrderApproved",
        "payload": {"order_id": str(existing_order.order_id)}
    }

    # Run handler
    await handle_event(event, db=mock_db)

    # Assertions
    assert existing_order.status == OrderStatus.APPROVED
    mock_db.commit.assert_called()  # Make sure DB commit happened
    # ProcessedEvent should be added
    mock_db.add.assert_any_call(existing_order)  # Not strictly necessary, depends on how you add ProcessedEvent

@pytest.mark.asyncio
async def test_stock_reservation_failed_triggers_refund(existing_order, mock_db, monkeypatch):
    """Test StockReservationFailed triggers RefundRequested if order was PAID."""
    # Set order to PAID
    existing_order.status = OrderStatus.PAID

    # Mock publish_event
    fake_publish = AsyncMock()
    monkeypatch.setattr("app.order_service.publish_event", fake_publish)

    # Create event
    event = {
        "event_id": str(uuid.uuid4()),
        "trace_id": str(uuid.uuid4()),
        "event_type": "StockReservationFailed",
        "payload": {"order_id": str(existing_order.order_id)}
    }

    await handle_event(event, db=mock_db)

    # Order status should now be CANCELED
    assert existing_order.status == OrderStatus.CANCELED

    # RefundRequested event should be published
    fake_publish.assert_awaited_with(
        topic="payment-events",
        event_type="RefundRequested",
        payload={
            "order_id": str(existing_order.order_id),
            "amount": float(existing_order.amount),
            "reason": getattr(existing_order, "reason", None)
        },
        trace_id=event["trace_id"]
    )
    mock_db.commit.assert_called()