from models import PaymentStatus, FraudDecision

# Map events to states for Payment
PAYMENT_STATE_MAP = {
    "PaymentSucceeded": PaymentStatus.SUCCESS,
    "PaymentFailed": PaymentStatus.FAILED,
    "PaymentPending": PaymentStatus.PENDING,
    "PaymentRefunded": PaymentStatus.REFUNDED,
}

# Map events to states for Fraud
FRAUD_STATE_MAP = {
    "FraudApproved": FraudDecision.APPROVE,
    "FraudBlock": FraudDecision.BLOCK,
    "FraudReview": FraudDecision.REVIEW
}