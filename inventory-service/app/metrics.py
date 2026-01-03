from prometheus_client import Counter, Histogram

# Histogram
inventory_reservation_duration_seconds = Histogram(
    "inventory_reservation_duration_seconds",
    "Inventory reservation duration in seconds",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 20, 30),
)

inventory_reservation_failures = Counter(
    "inventory_reservation_failures",
    "Total number of inventory reservation failures",
    ["reason"]
)