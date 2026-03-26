"""
DATABASE MODELS - Fraud & Payment Service
==========================================
SQLAlchemy ORM models for payment transactions and fraud analysis.

Tables:
- transactions: Payment records for each order
- fraud_analysis: Fraud risk assessments and decisions
"""

from sqlalchemy import Column, String, Numeric, DateTime, Enum
from sqlalchemy.dialects.postgresql import UUID
from app.db import Base
import uuid
from datetime import datetime, timezone
import enum


class PaymentStatus(enum.Enum):
    """Payment lifecycle states"""
    PENDING = "PENDING"      # Awaiting processing
    SUCCESS = "SUCCESS"      # Payment accepted
    FAILED = "FAILED"        # Payment declined
    REFUNDED = "REFUNDED"    # Money returned to customer


class FraudDecision(enum.Enum):
    """Fraud detection outcomes"""
    APPROVE = "APPROVE"      # Order approved, low fraud risk
    BLOCK = "BLOCK"          # Order blocked, high fraud risk
    REVIEW = "REVIEW"        # Order flagged for manual review


class Transaction(Base):
    """
    Payment transaction record
    Links orders to payment attempts with status tracking
    """
    __tablename__ = "transactions"

    transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), nullable=False)           # Associated order
    user_id = Column(UUID(as_uuid=True), nullable=False)            # Customer
    amount = Column(Numeric(10, 2), nullable=False)                 # Payment amount
    status = Column(Enum(PaymentStatus), default=PaymentStatus.PENDING)  # Payment state
    payment_method = Column(String, nullable=False)                 # e.g., "credit_card"
    fraud_decision = Column(Enum(FraudDecision))
    fraud_score = Column(Numeric(3, 2))
    idempotency_key = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id = Column(UUID(as_uuid=True), primary_key=True)
    event_type = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.now(timezone.utc))