# System Architecture

## High-Level Overview

This is a **microservices-based e-commerce order system** with real-time fraud detection built on event-driven architecture.

```
┌──────────────────┐
│  Order Service   │ (Port 8000)
│  Creates orders  │
└────────┬─────────┘
         │
         │ publishes "OrderCreated"
         ▼
    ┌────────────┐
    │   Kafka    │ (Message Bus)
    └────┬───┬──┘
         │   │
    ┌────▼───▼────────┐
    │ Fraud Service   │ (Port 8002)
    │ Blocks fraud    │
    │ Processes       │
    │ payments        │
    └────┬────────────┘
         │
    ┌────▼────────────────┐
    │ Inventory Service   │ (Port 8001)
    │ Reserves stock      │
    └─────────────────────┘
```

## Services Breakdown

### 1. **Order Service** (Port 8000)
- **responsibility**: Order processing orchestration
- **Key Functions**:
  - Create orders (REST endpoint)
  - Publish "OrderCreated" events
  - Listen for fraud & inventory results
  - Update order status based on results
  
- **Tech Stack**:
  - FastAPI
  - PostgreSQL (orders DB)
  - Kafka Consumer
  
- **Main Files**:
  - `app/main.py` - FastAPI entry point
  - `app/models.py` - Order, Product, ProcessedEvent
  - `app/api.py` - REST endpoints
  - `app/events.py` - Kafka producer/consumer logic

### 2. **Fraud & Payment Service** (Port 8002)
- **Responsibility**: Fraud detection & payment processing
- **Key Functions**:
  - Evaluate orders for fraud risk
  - Process payments for approved orders
  - Handle refunds
  - Publish fraud decisions back to Kafka
  
- **Fraud Rules**:
  - ❌ Block if amount > $10,000
  - ❌ Block if user has 3+ orders in 1 hour
  - ✅ Approve otherwise
  
- **Tech Stack**:
  - FastAPI
  - PostgreSQL (payments DB)
  - Kafka Consumer
  
- **Main Files**:
  - `app/main.py` - FastAPI entry point
  - `app/fraud.py` - Fraud scoring logic
  - `app/models.py` - Transaction, FraudDecision
  - `app/payment.py` - Payment processing

### 3. **Inventory Service** (Port 8001)
- **Responsibility**: Stock management & reservation
- **Key Functions**:
  - Check product availability
  - Reserve inventory for orders
  - Publish reservation results
  - Release stock on cancellation
  
- **Tech Stack**:
  - FastAPI
  - PostgreSQL (inventory DB)
  - Kafka Consumer
  
- **Main Files**:
  - `app/main.py` - FastAPI entry point
  - `app/models.py` - Product, InventoryReservation
  - `app/inventory.py` - Stock management logic

## Event Flow

### Successful Order Processing
```
1. Customer submits order
   ↓
2. Order Service creates Order (status: CREATED)
   ↓
3. Order Service publishes "OrderCreated" event
   ↓
4. Fraud Service receives event
   ├─ Evaluates fraud risk
   └─ Publishes "FraudCheckResult" event
   ↓
5. Inventory Service receives event
   ├─ Reserves stock
   └─ Publishes "StockReserved" event
   ↓
6. Order Service receives both results
   ├─ Updates status to CONFIRMED
   └─ Order processing complete ✓
```

### Failure Cases
- **Fraud detected** → Status: CANCELED, triggers refund
- **Payment failed** → Status: CANCELED
- **Stock unavailable** → Status: CANCELED

## Database Schema

### Order Service DB (orders)
```sql
orders
├── order_id (UUID, PK)
├── user_id (UUID)
├── product_id (UUID)
├── quantity (INT)
├── amount (DECIMAL)
├── status (ENUM: CREATED|APPROVED|PAID|CONFIRMED|CANCELED|REFUNDED)
├── created_at (TIMESTAMP)
└── updated_at (TIMESTAMP)

products
├── product_id (UUID, PK)
├── name (VARCHAR)
└── price (DECIMAL)

processed_events
├── event_id (UUID, PK)
├── event_type (VARCHAR)
└── processed_at (TIMESTAMP)
```

### Fraud Service DB (payments)
```sql
transactions
├── transaction_id (UUID, PK)
├── order_id (UUID, FK)
├── user_id (UUID)
├── amount (DECIMAL)
├── status (ENUM: PENDING|SUCCESS|FAILED|REFUNDED)
└── payment_method (VARCHAR)
```

### Inventory Service DB (inventory)
```sql
products
├── product_id (UUID, PK)
├── name (VARCHAR)
└── available_quantity (INT)

inventory_reservations
├── reservation_id (UUID, PK)
├── order_id (UUID, FK)
├── product_id (UUID, FK)
├── quantity (INT)
└── status (ENUM: RESERVED|RELEASED)
```

## Key Patterns

### Microservices Pattern
- ✓ Each service has its own database (no shared DB)
- ✓ Independent deployment & scaling
- ✓ Loose coupling via events

### Event-Driven Architecture
- ✓ Kafka as message bus
- ✓ Asynchronous communication
- ✓ Services don't call each other directly

### Idempotency
- ✓ ProcessedEvent table prevents duplicate processing
- ✓ Kafka consumer gracefully handles duplicate events

### Health Checks
- ✓ Docker healthchecks on databases
- ✓ Kafka broker health monitoring
- ✓ Service dependency ordering (depends_on)

## Monitoring

### Prometheus Metrics
Services expose metrics at `/metrics` endpoint:
- Request latency
- Order processing time
- Status transitions
- Fraud decisions
- Kafka consumer lag

### Logs
- Structured JSON logging
- Trace IDs for request tracking
- Persisted in `./logs/<service>/` directory

## How to Run

```bash
# Start all services
docker-compose up --build

# View logs
docker-compose logs -f order_service

# Stop services
docker-compose down

# Stop and remove volumes (clean state)
docker-compose down -v
```

## Useful Commands

```bash
# Test Order Creation
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "550e8400-e29b-41d4-a716-446655440000",
    "product_id": "550e8400-e29b-41d4-a716-446655440001",
    "quantity": 1,
    "amount": 99.99,
    "payment_method": "credit_card"
  }'

# Get Order Details
curl http://localhost:8000/orders/{order_id}

# View Prometheus Metrics
curl http://localhost:8002/metrics  # Fraud Service metrics
curl http://localhost:8001/metrics  # Inventory Service metrics

# Check Kafka Topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Development Notes

- **Database Credentials**: user / pass (use strong credentials in production!)
- **Kafka Replication**: Set to 1 for local dev (increase for production)
- **Fraud Rules**: Modify `fraud_payment_service/app/fraud.py` to add ML models
- **Timestamps**: UTC timezone for all datetime fields
- **Python Version**: Python 3.9+
