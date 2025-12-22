from sqlalchemy import Column, String, Integer, Numeric, DateTime, Enum
from sqlalchemy.dialects.postgresql import UUID
from common.db import Base
import uuid
from datetime import datetime, UTC
import enum


class OrderStatus(enum.Enum):
    CREATED = "CREATED"
    APPROVED = "APPROVED"
    PAID = "PAID"
    CONFIRMED = "CONFIRMED"
    CANCELED = "CANCELED"


class Order(Base):
    __tablename__ = "orders"

    order_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    product_id = Column(UUID(as_uuid=True), nullable=False)
    quantity = Column(Integer, nullable=False)
    payment_method = Column(String, nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    status = Column(Enum(OrderStatus), default=OrderStatus.CREATED)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))


class Product(Base):
    __tablename__ = "products"

    product_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    price = Column(Numeric(10, 2), nullable=False)