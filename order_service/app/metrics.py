from prometheus_client import Counter, Histogram, Gauge

# -----------------------
# Histogram
# -----------------------
# End-to-end order processing duration in seconds
order_processing_duration_seconds = Histogram(
    "order_processing_duration_seconds",
    "Order processing duration in seconds",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 20, 30),
)

# -----------------------
# Counters
# -----------------------
# Tracks transitions between order statuses
order_status_transitions_total = Counter(
    'order_status_transitions_total',
    'Total number of order status transitions, labeled by new status',
    ['status'],
)

# Tracks total orders created
orders_created_total = Counter(
    "orders_created_total",
    "Total number of orders created"
)

# Tracks total orders confirmed
orders_confirmed_total = Counter(
    "orders_confirmed_total",
    "Total number of confirmed orders"
)

# Tracks total orders canceled, labeled by reason
orders_canceled_total = Counter(
    "orders_canceled_total",
    "Total number of canceled orders",
    ["reason"]
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