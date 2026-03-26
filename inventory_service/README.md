"""
INVENTORY SERVICE
=================

Purpose: Stock management, reservation, and availability checks

Endpoints:
  GET    /inventory/{product_id}
         Get current stock level for a product
         Response: {product_id, name, available_quantity}
  
  POST   /inventory/check
         Check if sufficient stock exists
         Body: {product_id, quantity_needed}
         Response: {available: true/false, quantity: int}
  
  GET    /health
         Health check for Docker

Event Processing (Kafka Consumer):
  Subscribes to:
    - "OrderCreated" event
  
  Processing Steps:
    1. Receive OrderCreated event with order data
    2. Check if product has sufficient stock
    3. If available: Reserve stock (decrement available_quantity)
    4. If unavailable: Reject reservation
    5. Publish "StockReserved" or "StockReservationFailed" event
    6. On order cancellation: Release reserved stock

Database: PostgreSQL (inventory)
  - products: Product catalog with stock levels
  - inventory_reservations: Tracks reserved stock by order

Dependencies: Kafka, PostgreSQL

Key Files:
  app/main.py       - FastAPI app entry point
  app/api.py        - REST API endpoints for inventory queries
  app/inventory.py  - Stock reservation and release logic
  app/models.py     - Product, InventoryReservation ORM models
  app/db.py         - Database configuration
  app/events.py     - Kafka producer/consumer logic
  app/logging.py    - Structured logging
  app/metrics.py    - Prometheus metrics

Stock Reservation Flow:
  1. User creates order for Item X (qty: 5)
  2. Order Service publishes "OrderCreated" event
  3. Inventory Service receives event
  4. Service checks: available_quantity >= 5?
  5. If YES: Decrement by 5, create reservation record
  6. Publish "StockReserved" success
  7. If NO: Publish "StockReservationFailed"
  8. Order Service updates order status accordingly

Stock Release (on Order Cancellation):
  1. Order is cancelled
  2. Inventory Service releases the reservation
  3. Increment available_quantity back
  4. Emit "StockReleased" event

Concurrency Handling:
  - Database row-level locks prevent overselling
  - Multiple concurrent requests handled safely
"""
