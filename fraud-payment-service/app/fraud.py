def evaluate_fraud(order_data: dict) -> dict:
    """Simple rules-based fraud evaluation"""
    amount = order_data.get("amount", 0)

    # Rule 1: Block high amounts
    if amount > 10000:
        return {"decision": "BLOCK", "score": 0.95, "reason": "high_amount"}

    # Rule 2: Review medium amounts
    if amount > 5000:
        return {"decision": "REVIEW", "score": 0.65, "reason": "medium_risk"}

    # Rule 3: Approve low amounts
    return {"decision": "APPROVE", "score": 0.1, "reason": "low_risk"}