"""
FRAUD & PAYMENT SERVICE
=======================

Purpose: Real-time fraud detection and payment processing

Endpoints:
  GET    /health
         Health check for Docker

Event Processing (Kafka Consumer):
  Subscribes to:
    - "OrderCreated" event
    
  Processing Steps:
    1. Receive OrderCreated event with order data
    2. Run fraud evaluation rules (check amount, velocity, etc.)
    3. If approved: Process payment
    4. If blocked: Document fraud decision
    5. Publish "FraudCheckResult" event with decision
    6. Publish "PaymentSucceeded" or "PaymentFailed" event

Fraud Detection Rules:
  Rule 1 - High Amount Check:
    If order.amount > $10,000 → BLOCK (95% fraud score)
    Rationale: Unusually large transactions are high risk
  
  Rule 2 - Velocity Check:
    If user has 3+ orders in last 1 hour → BLOCK (85% fraud score)
    Rationale: Rapid-fire ordering suggests account takeover or testing
  
  Rule 3 - Default:
    Otherwise → APPROVE (10% fraud score)

Database: PostgreSQL (payments)
  - transactions: Payment records with status
  - fraud_decisions: Fraud analysis results

Dependencies: Kafka, PostgreSQL

Key Files:
  app/main.py       - FastAPI app entry point & lifecycle
  app/fraud.py      - Fraud scoring engine (rule-based logic)
  app/payment.py    - Payment processing logic
  app/models.py     - Transaction, FraudDecision ORM models
  app/db.py         - Database configuration
  app/events.py     - Kafka producer/consumer logic
  app/logging.py    - Structured logging
  app/metrics.py    - Prometheus metrics

Note: This service is event-driven (no REST endpoints for customers).
      It consumes order events, processes them, and publishes results back to Kafka.
      Other services subscribe to the results.
"""
