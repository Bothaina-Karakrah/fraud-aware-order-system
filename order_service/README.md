"""
ORDER SERVICE
=============

Purpose: REST API for order creation and management

Endpoints:
  POST   /orders
         Create a new order
         Body: {user_id, product_id, quantity, amount, payment_method}
         Response: {order_id, status, ...}
  
  GET    /orders/{order_id}
         Retrieve order details
         Response: {order_id, user_id, product_id, quantity, status, ...}
  
  GET    /orders
         List all orders
         Response: [{orders...}]
  
  GET    /health
         Health check for Docker

Event Processing:
  Subscribes to Kafka topics:
    - "FraudCheckResult" → Updates order status based on fraud decision
    - "InventoryReserved" → Marks order as CONFIRMED
    - "InventoryFailed" → Cancels order
  
  Publishes to Kafka:
    - "OrderCreated" → Triggered when order is created

Database: PostgreSQL (orders)
Dependencies: Kafka, PostgreSQL

Key Files:
  app/main.py       - FastAPI app entry point
  app/api.py        - REST API endpoints
  app/models.py     - Order, Product, ProcessedEvent ORM models
  app/db.py         - Database configuration
  app/events.py     - Kafka producer/consumer logic
  app/logging.py    - Structured logging setup
  app/metrics.py    - Prometheus metrics definitions
"""
