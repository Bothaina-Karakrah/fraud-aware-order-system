from prometheus_client import Counter, Histogram

# Histogram: End-to-end order processing duration in seconds
# Buckets are designed to capture typical and tail latencies for alerting
order_processing_duration_seconds = Histogram(
    "order_processing_duration_seconds",
    "Order processing duration in seconds",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 20, 30),
)

# Counter: tracks transitions between order statuses
order_status_transitions_total = Counter(
    'order_status_transitions_total',
    'Total number of order status transitions, labeled by new status',
    ['status'],
)

# Counter: tracks total orders created
orders_created_total = Counter(
    "orders_created_total",
    "Total number of orders created"
)

# Counter: tracks total orders confirmed
orders_confirmed_total = Counter(
    "orders_confirmed_total",
    "Total number of confirmed orders"
)

# Counter: tracks total orders canceled, labeled by reason
orders_canceled_total = Counter(
    "orders_canceled_total",
    "Total number of canceled orders",
    ["reason"]
)