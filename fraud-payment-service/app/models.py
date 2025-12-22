from sqlalchemy import Column, String, Integer, Numeric, DateTime, Enum, CheckConstraint
from sqlalchemy.dialects.postgresql import UUID
from common.db import Base
import uuid
from datetime import datetime, UTC
import enum
from sqlalchemy import Column


class PaymentStatus(enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"

class FraudDecision(enum.Enum):
    APPROVE = "APPROVE"
    BLOCK = "BLOCK"
    REVIEW = "REVIEW"

class Payment(Base):
    __tablename__ = "payments"
    transaction_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    amount = Column(Numeric(10, 2), nullable=False)
    status = Column(Enum(PaymentStatus), default=PaymentStatus.PENDING)
    payment_method = Column(String, nullable=False)
    fraud_decision = Column(Enum(FraudDecision), default=FraudDecision.REVIEW)
    fraud_score = Column(Numeric(10, 2), default=0.0, nullable=False)
    idempotency_key = Column(String, unique=True, nullable=True)
    created_at = Column(DateTime, default=datetime.now(UTC))
    updated_at = Column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))
    __table_args__ = (
        CheckConstraint('fraud_score >= 0.0 AND fraud_score <= 1.0', name='fraud_score_range'),
    )