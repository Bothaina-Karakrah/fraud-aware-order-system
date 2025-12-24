from datetime import datetime, timedelta, UTC
from sqlalchemy.orm import Session
from .models import Transaction, FraudDecision

def evaluate_fraud(db: Session, order_data: dict) -> dict:
    user_id = order_data.get("user_id")
    amount = order_data.get("amount", 0)

    # --- Rule 1: High Amount Check ---
    if amount > 10000:
        return {"decision": FraudDecision.BLOCK, "score": 0.95, "reason": "high_amount"}

    # --- Rule 2: Velocity Check (Design Requirement) ---
    # Check how many successful/pending transactions this user has in the last hour
    one_hour_ago = datetime.now(UTC) - timedelta(hours=1)
    recent_order_count = db.query(Transaction).filter(
        Transaction.user_id == user_id,
        Transaction.created_at >= one_hour_ago
    ).count()

    if recent_order_count >= 3:
        return {"decision": FraudDecision.BLOCK, "score": 0.85, "reason": "velocity_limit_exceeded"}

    # --- Rule 3: Approve ---
    return {"decision": FraudDecision.APPROVE, "score": 0.1, "reason": "low_risk"}