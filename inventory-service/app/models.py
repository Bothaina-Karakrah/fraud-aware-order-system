from sqlalchemy import Column, Integer, DateTime, String
from sqlalchemy.dialects.postgresql import UUID
from db import Base
from datetime import datetime, UTC

class Inventory(Base):
    __tablename__ = "inventory"

    product_id = Column(UUID(as_uuid=True), primary_key=True)
    available_quantity = Column(Integer, default=0)
    reserved_quantity = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id = Column(UUID(as_uuid=True), primary_key=True)
    event_type = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.now(UTC))