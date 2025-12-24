from sqlalchemy import Column, String, Numeric, DateTime, Enum
from sqlalchemy.dialects.postgresql import UUID
from db import Base
import uuid
from datetime import datetime, UTC
import enum


class PaymentStatus(enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


class FraudDecision(enum.Enum):
    APPROVE = "APPROVE"
    BLOCK = "BLOCK"
    REVIEW = "REVIEW"


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    status = Column(Enum(PaymentStatus), default=PaymentStatus.PENDING)
    payment_method = Column(String, nullable=False)
    fraud_decision = Column(Enum(FraudDecision))
    fraud_score = Column(Numeric(3, 2))
    idempotency_key = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now(UTC))
    updated_at = Column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id = Column(UUID(as_uuid=True), primary_key=True)
    event_type = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.now(UTC))