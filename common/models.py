import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
from .db import Base  # shared Base

class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow)