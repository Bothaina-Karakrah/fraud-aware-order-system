from prometheus_client import Counter, Histogram, Gauge

# -----------------------
# Histogram
# -----------------------
inventory_reservation_duration_seconds = Histogram(
    "inventory_reservation_duration_seconds",
    "Inventory reservation duration in seconds",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 20, 30),
)

# -----------------------
# Counters
# -----------------------
inventory_reservations_total = Counter(
    "inventory_reservations_total",
    "Total number of inventory reservation attempts"
)

inventory_reservation_failures = Counter(
    "inventory_reservation_failures",
    "Total number of inventory reservation failures",
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