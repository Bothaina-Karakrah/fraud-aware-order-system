# Code Structure Guide

## Quick Navigation

### 📂 Project Root
```
fraud-aware-order-system/
├── README.md                    # Project overview
├── ARCHITECTURE.md              # System architecture & design
├── docker-compose.yml           # Infrastructure setup
├── prometheus/                  # Monitoring configuration
│   ├── prometheus.yml           # Prometheus scrape jobs
│   └── alerts/                  # Alert rules
├── data/                        # Database volumes (local dev)
├── logs/                        # Service logs (mounted from containers)
├── order_service/               # Order creation & orchestration
├── fraud_payment_service/       # Fraud detection & payments
└── inventory_service/           # Stock management
```

## Service Structure

Each service follows the same pattern:
```
service_name/
├── README.md                    # Service-specific documentation
├── Dockerfile                   # Container image definition
├── requirements.txt             # Python dependencies
└── app/
    ├── __init__.py             # Python package marker
    ├── main.py                 # FastAPI entry point ⭐ START HERE
    ├── api.py                  # REST endpoints (if applicable)
    ├── db.py                   # Database configuration
    ├── models.py               # SQLAlchemy ORM models ⭐ UNDERSTAND SCHEMA HERE
    ├── events.py               # Kafka producer/consumer logic
    ├── {service}.py            # Core business logic (fraud/inventory/etc)
    ├── logging.py              # Structured logging setup
    ├── metrics.py              # Prometheus metrics definitions
    └── payment.py              # Payment processing (fraud service only)
```

## 📖 Where to Find What

### Understanding the System
1. **Architecture Overview** → [ARCHITECTURE.md](ARCHITECTURE.md)
2. **Service Details** → Read each service's `README.md`
3. **High-Level Flow** → Look at [docker-compose.yml](docker-compose.yml) diagram

### Understanding Data Models
1. **Order DB Schema** → [order_service/app/models.py](order_service/app/models.py)
2. **Payment DB Schema** → [fraud_payment_service/app/models.py](fraud_payment_service/app/models.py)
3. **Inventory DB Schema** → [inventory_service/app/models.py](inventory_service/app/models.py)

### Understanding Business Logic
- **How orders are created**: [order_service/app/api.py](order_service/app/api.py)
- **How fraud is detected**: [fraud_payment_service/app/fraud.py](fraud_payment_service/app/fraud.py)
- **How stock is managed**: [inventory_service/app/inventory.py](inventory_service/app/inventory.py)

### Understanding Event Communication
- **How events are published/consumed**: Look at `app/events.py` in each service
- **Event types & mapping**: Search for `EVENT_STATE_MAP` or `EVENT_TYPE`

### Understanding Monitoring
- **Prometheus setup**: [prometheus/prometheus.yml](prometheus/prometheus.yml)
- **Alert rules**: [prometheus/alerts/](prometheus/alerts/)
- **Service metrics**: Look for `metrics.py` in each service

## 🔍 Key Code Patterns

### 1. Service Entry Point
```
Every service has this in app/main.py:
- FastAPI app initialization
- Database initialization on startup
- Kafka consumer startup in background
- Graceful shutdown cleanup
```

**Navigate to**: `{service}/app/main.py`

### 2. Event-Driven Communication
```
Every service has this in app/events.py:
- Kafka producer setup (publish events)
- Kafka consumer setup (listen for events)
- Event handler logic (process received events)
- Idempotency checks (prevent duplicate processing)
```

**Navigate to**: `{service}/app/events.py`

### 3. Database Models
```
Every service defines:
- SQLAlchemy ORM models
- Enums for status values
- Timestamps for audit trail
```

**Navigate to**: `{service}/app/models.py`

### 4. REST Endpoints (Order & Inventory only)
```
API routes defined in app/api.py:
- POST endpoints to create/update resources
- GET endpoints to retrieve resources
- DELETE endpoints to remove resources (if applicable)
```

**Navigate to**: `{service}/app/api.py`

### 5. Business Logic
```
Core logic separated into dedicated modules:
- Fraud: fraud_payment_service/app/fraud.py (fraud scoring)
- Payments: fraud_payment_service/app/payment.py (payment processing)
- Inventory: inventory_service/app/inventory.py (stock management)
```

## 📝 File Glossary

| File | Purpose |
|------|---------|
| `main.py` | FastAPI app, lifecycle management |
| `api.py` | REST endpoint definitions |
| `models.py` | SQLAlchemy ORM & data schemas |
| `db.py` | Database connection & session management |
| `events.py` | Kafka producer/consumer & event handling |
| `fraud.py` | Fraud detection rules & scoring |
| `payment.py` | Payment processing logic |
| `inventory.py` | Stock reservation & management |
| `logging.py` | Structured logging configuration |
| `metrics.py` | Prometheus metrics definitions |
| `requirements.txt` | Python package dependencies |
| `Dockerfile` | Container image definition |
| `README.md` | Service documentation |

## 🚀 Common Tasks

### Task: Add a new order status
1. Add status to `OrderStatus` enum in [order_service/app/models.py](order_service/app/models.py)
2. Add mapping in `EVENT_STATE_MAP` in [order_service/app/events.py](order_service/app/events.py)
3. Update fraud/inventory services to publish corresponding events

### Task: Add fraud detection rule
1. Edit [fraud_payment_service/app/fraud.py](fraud_payment_service/app/fraud.py)
2. Add new rule to `evaluate_fraud()` function
3. Return decision with score and reason

### Task: Add new API endpoint
1. Add route to `{service}/app/api.py`
2. Add database query logic to models
3. Add tests if applicable

### Task: Add monitoring alert
1. Edit alert file in [prometheus/alerts/](prometheus/alerts/)
2. Define Prometheus query (PromQL)
3. Set threshold and duration
4. Verify with Prometheus UI at http://localhost:9090

## 💾 Data Flow

### Complete Order Journey
```
1. Client calls POST /orders (Order Service)
   ↓
2. Order Service saves Order to DB (status: CREATED)
   ↓
3. Order Service publishes "OrderCreated" to Kafka
   ↓
4. Fraud Service consumes "OrderCreated"
   → Runs fraud evaluation
   → Publishes "FraudCheckResult" to Kafka
   ↓
5. Inventory Service consumes "OrderCreated"
   → Reserves stock
   → Publishes "StockReserved" to Kafka
   ↓
6. Order Service consumes both results
   → Updates order status
   ↓
7. Final status: CONFIRMED (if all passed) or CANCELED (if any failed)
```

## 🔗 Dependencies Between Files

```
app/main.py
├── imports from app/db.py          (database session)
├── imports from app/models.py      (ORM models)
├── imports from app/events.py      (Kafka setup)
├── imports from app/logging.py     (logger)
└── imports from app/api.py         (routes)

app/events.py
├── imports from app/db.py          (database session)
├── imports from app/models.py      (models for query)
└── imports from app/logging.py     (logger)

app/{service}.py (fraud/inventory)
├── imports from app/models.py      (ORM models)
└── imports from app/db.py          (database session)
```

## ✅ Testing Checklist

When exploring code:
- [ ] Start with ARCHITECTURE.md
- [ ] Read [README.md](README.md) for project overview
- [ ] Check service-specific README.md
- [ ] Review `main.py` to understand initialization
- [ ] Study `models.py` to understand data schema
- [ ] Read `events.py` to understand event flow
- [ ] Trace one request through the entire system

## 📚 Additional Resources

- [ARCHITECTURE.md](ARCHITECTURE.md) - System design & event flows
- [order_service/README.md](order_service/README.md) - Order Service details
- [fraud_payment_service/README.md](fraud_payment_service/README.md) - Fraud & Payment details
- [inventory_service/README.md](inventory_service/README.md) - Inventory Service details
- [docker-compose.yml](docker-compose.yml) - Infrastructure & service dependencies
