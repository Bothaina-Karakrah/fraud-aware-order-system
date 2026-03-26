# Fraud-Aware Order System

## Project Overview
A microservices-based e-commerce order processing system with real-time fraud detection, payment processing, and inventory management. The system uses event-driven architecture via Kafka for asynchronous communication between services.

## Architecture

### Services
- **Order Service** (Port 8000): Handles order creation, retrieval, and orchestration
- **Fraud & Payment Service**: Real-time fraud detection and payment processing
- **Inventory Service**: Manages inventory availability and stock updates

### Data Flow
1. Customer creates order → Order Service
2. Order Service publishes "OrderCreated" event to Kafka
3. Fraud Service & Inventory Service consume events
4. Fraud Service checks risk, publishes "FraudCheckResult" event
5. Inventory Service reserves stock, publishes "InventoryReserved" event
6. Order Service receives results, updates order status

### Technology Stack
- **Framework**: FastAPI (Python)
- **Message Broker**: Apache Kafka + Zookeeper
- **Databases**: PostgreSQL (1 per service)
  - `order-db`: Orders data
  - `payment-db`: Payment & fraud analysis
  - `inventory-db`: Inventory & stock data
- **Monitoring**: Prometheus (metrics at `/metrics` endpoint)
- **Containerization**: Docker & Docker Compose

## Getting Started

### Prerequisites
- Docker & Docker Compose

### Run the System
```bash
docker-compose up --build
```

### Access Services
- Order Service API: http://localhost:8000
- Prometheus: http://localhost:9090 (if exposed)

## Directory Structure
```
fraud-aware-order-system/
├── order_service/           # REST API for order management
├── fraud_payment_service/   # Fraud detection & payment processing
├── inventory_service/       # Inventory management
├── prometheus/              # Monitoring & alerting rules
│   └── alerts/             # Alert configurations
└── docker-compose.yml      # Infrastructure orchestration
```

## Key Features
✓ Asynchronous event-driven communication via Kafka
✓ Real-time fraud detection
✓ Inventory reservation with concurrent request handling
✓ Prometheus metrics for monitoring
✓ Database isolation (microservices pattern)
✓ Health checks for service dependencies
✓ Structured logging across all services
