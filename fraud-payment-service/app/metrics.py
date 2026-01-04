from prometheus_client import Counter, Histogram

# -----------------------
# Fraud metrics
# -----------------------

fraud_check_duration_seconds = Histogram(
    "fraud_check_duration_seconds",
    "Fraud check duration in seconds",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 20, 30),
)

fraud_decisions = Counter(
    "fraud_decisions_total",
    "Total number of fraud decisions",
    ["decision"],  # APPROVE | BLOCK | REVIEW
)

# -----------------------
# Payment metrics
# -----------------------
payment_attempts_total = Counter(
    "payment_attempts_total",
    "Total number of payment attempts",
)

payment_failures = Counter(
    "payment_failures_total",
    "Total number of payment failures",
    ["reason"],
)

payment_refunds_total = Counter(
    "payment_refunds_total",
    "Total number of refunded payments",
)