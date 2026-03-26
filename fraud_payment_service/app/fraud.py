"""
FRAUD DETECTION ENGINE
======================
Real-time fraud scoring and decision logic for incoming orders.

Fraud Detection Rules:
1. High Amount Check: Orders > $10,000 are blocked (95% fraud score)
2. Velocity Check: Users with 3+ orders in 1 hour are blocked (85% fraud score)
3. Default: Low risk (10% fraud score)

This is a rule-based system. In production, you might enhance with ML models.
"""

from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from app.models import Transaction, FraudDecision


def evaluate_fraud(db: Session, order_data: dict) -> dict:
    """
    Evaluate if an order is fraudulent based on rule-based logic.
    
    Args:
        db: Database session
        order_data: Order details {user_id, amount, payment_method, ...}
    
    Returns:
        {
            "decision": FraudDecision.APPROVE | BLOCK | REVIEW,
            "score": float (0-1),  # Fraud probability
            "reason": str          # Why this decision was made
        }
    """
    user_id = order_data.get("user_id")
    amount = order_data.get("amount", 0)

    # --- Rule 1: High Amount Check ---
    # Flag unusually large transactions as high fraud risk
    if amount > 10000:
        return {
            "decision": FraudDecision.BLOCK,
            "score": 0.95,
            "reason": "high_amount"
        }

    # --- Rule 2: Velocity Check (Design Requirement) ---
    # Detect rapid-fire ordering by same user (credit card testing, account takeover)
    # Check how many successful/pending transactions this user has in the last hour
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    recent_order_count = db.query(Transaction).filter(
        Transaction.user_id == user_id,
        Transaction.created_at >= one_hour_ago
    ).count()

    if recent_order_count >= 3:
        return {
            "decision": FraudDecision.BLOCK,
            "score": 0.85,
            "reason": "velocity_limit_exceeded"
        }

    # --- Rule 3: Approve Low-Risk Orders ---
    return {
        "decision": FraudDecision.APPROVE,
        "score": 0.1,
        "reason": "low_risk"
    }