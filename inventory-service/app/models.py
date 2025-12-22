from sqlalchemy import Column, Integer, DateTime
from sqlalchemy.dialects.postgresql import UUID
from common.db import Base
from datetime import datetime, UTC


class Inventory(Base):
    __tablename__ = 'inventory'
    product_id = Column(UUID(as_uuid=True), primary_key=True)
    available_quantity = Column(Integer, nullable=False)
    reserved_quantity = Column(Integer, nullable=False)
    updated_at = Column(DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))