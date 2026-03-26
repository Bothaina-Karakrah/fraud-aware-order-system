"""
DATABASE MODELS - Order Service
===============================
SQLAlchemy ORM models defining the database schema for the Order Service.

Tables:
- orders: Core order records with status tracking
- products: Available products for ordering
- processed_events: Idempotency key to prevent duplicate event processing
"""

from sqlalchemy import Column, String, Integer, Numeric, DateTime, Enum
from sqlalchemy.dialects.postgresql import UUID
from app.db import Base
import uuid
from datetime import datetime, timezone
import enum


class OrderStatus(enum.Enum):
    """Order lifecycle states"""
    CREATED = "CREATED"          # Initial state after order creation
    APPROVED = "APPROVED"        # Fraud check passed
    PAID = "PAID"                # Payment processed successfully
    CONFIRMED = "CONFIRMED"      # Inventory reserved
    CANCELED = "CANCELED"        # Order failed (fraud/payment/inventory)
    REFUNDED = "REFUNDED"        # Refund issued


class Order(Base):
    """
    Order record - represents a customer purchase request
    
    Status Flow: CREATED → APPROVED → PAID → CONFIRMED
                                ↓ (fraud/payment fails)
                             CANCELED → REFUNDED
    """
    __tablename__ = "orders"

    order_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False)              # Customer ID
    product_id = Column(UUID(as_uuid=True), nullable=False)           # Product being ordered
    quantity = Column(Integer, nullable=False)                        # Number of units
    payment_method = Column(String, nullable=False)                   # e.g., "credit_card"
    amount = Column(Numeric(10, 2), nullable=False)                   # Total order value
    status = Column(Enum(OrderStatus), default=OrderStatus.CREATED)   # Current order state
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class Product(Base):
    """Product catalog - items available for order"""
    __tablename__ = "products"

    product_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)                             # Product name
    price = Column(Numeric(10, 2), nullable=False)                    # Unit price


class ProcessedEvent(Base):
    """
    Idempotency tracking - prevents duplicate processing of events
    If the same event_id is processed twice, we skip it
    """
    __tablename__ = "processed_events"

    event_id = Column(UUID(as_uuid=True), primary_key=True)  # Unique event identifier
    event_type = Column(String, nullable=False)              # Type of event processed
    processed_at = Column(DateTime, default=datetime.now(timezone.utc))