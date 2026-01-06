from prometheus_client import Counter, Histogram, Gauge

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

# -----------------------
# Kafka metrics
# -----------------------
# Consumer Lag (for the alert)
kafka_consumer_lag = Gauge(
    'kafka_consumer_lag',
    'Consumer lag in messages',
    ['service']
)

# Processing Errors (for debugging failures)
kafka_processing_errors = Counter(
    'kafka_processing_errors_total',
    'Failed message processing',
    ['service']
)

# Messages Processed (verify that system is working)
kafka_messages_processed = Counter(
    'kafka_messages_processed_total',
    'Messages successfully processed',
    ['service']
)