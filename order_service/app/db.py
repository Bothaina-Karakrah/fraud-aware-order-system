"""
DATABASE CONFIGURATION - Order Service
======================================
Manages PostgreSQL connection pool and SQLAlchemy session lifecycle.

Environment Variables:
  DATABASE_URL: PostgreSQL connection string
                Default: postgresql://user:pass@order-db:5432/orders
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# Database connection string from environment or Docker default
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:pass@order-db:5432/orders"
)

# Create database engine with connection pooling
engine = create_engine(DATABASE_URL)

# Session factory for creating new DB sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for all ORM models
Base = declarative_base()

def init_db():
    """Initialize database tables if they don't exist"""
    Base.metadata.create_all(bind=engine)

def get_db():
    """
    Dependency injection for FastAPI endpoints
    Provides a database session and ensures cleanup
    
    Usage in endpoints:
        @app.get("/orders")
        def get_orders(db: Session = Depends(get_db)):
            return db.query(Order).all()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()