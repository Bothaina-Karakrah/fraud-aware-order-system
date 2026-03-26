# Quick Start Guide

## 🚀 Getting Started in 5 Minutes

### Prerequisites
- Docker & Docker Compose installed
- `curl` command (or Postman for API testing)

### 1️⃣ Start the System
```bash
cd fraud-aware-order-system/
docker-compose up --build
```

Wait for all services to show "healthy" messages (2-3 minutes).

### 2️⃣ Test Order Creation
```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "550e8400-e29b-41d4-a716-446655440000",
    "product_id": "550e8400-e29b-41d4-a716-446655440001",
    "quantity": 1,
    "amount": 99.99,
    "payment_method": "credit_card"
  }'
```

**Expected Response:**
```json
{
  "order_id": "...",
  "user_id": "...",
  "product_id": "...",
  "quantity": 1,
  "amount": "99.99",
  "payment_method": "credit_card",
  "status": "CREATED"
}
```

### 3️⃣ Retrieve Order Details
```bash
curl http://localhost:8000/orders/{order_id}
```

Order status should progress: `CREATED` → `APPROVED` → `PAID` → `CONFIRMED`
(This takes a few seconds as it goes through Kafka events)

### 4️⃣ View Logs (Live)
```bash
docker-compose logs -f order_service
docker-compose logs -f fraud_payment_service
docker-compose logs -f inventory_service
```

### 5️⃣ Stop Services
```bash
docker-compose down      # Stop but keep volumes
docker-compose down -v   # Stop and remove all data (clean slate)
```

---

## 📊 Testing Scenarios

### ✅ Successful Order (Low Risk)
```bash
# Amount: $50 (below $10k threshold)
# User with 0 recent orders (below 3+ velocity limit)
# Expected: Status = CONFIRMED

curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "550e8400-e29b-41d4-a716-000000000001",
    "product_id": "550e8400-e29b-41d4-a716-446655440001",
    "quantity": 1,
    "amount": 50.00,
    "payment_method": "credit_card"
  }'
```

### ❌ Fraud: High Amount
```bash
# Amount: $15,000 (exceeds $10k threshold)
# Expected: Status = CANCELED

curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "550e8400-e29b-41d4-a716-000000000002",
    "product_id": "550e8400-e29b-41d4-a716-446655440001",
    "quantity": 1,
    "amount": 15000.00,
    "payment_method": "credit_card"
  }'
```

### ❌ Fraud: Velocity Exceeded
```bash
# Create 4 orders for the same user in rapid succession
# Third+ orders should be blocked (velocity limit = 3 per hour)
# Expected: 4th order Status = CANCELED

# Create orders for user: 550e8400-e29b-41d4-a716-000000000003
# Repeat the POST 4 times with same user_id, different amounts
for i in 1 2 3 4; do
  curl -X POST http://localhost:8000/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"user_id\": \"550e8400-e29b-41d4-a716-000000000003\",
      \"product_id\": \"550e8400-e29b-41d4-a716-446655440001\",
      \"quantity\": 1,
      \"amount\": $((50 * i)).00,
      \"payment_method\": \"credit_card\"
    }"
  echo "Order $i created"
  sleep 1
done
```

---

## 📈 Monitoring

### View Service Metrics

**Order Service Metrics:**
```bash
curl http://localhost:8000/metrics
```

**Fraud Service Metrics:**
```bash
curl http://localhost:8002/metrics
```

**Inventory Service Metrics:**
```bash
curl http://localhost:8001/metrics
```

### Prometheus Dashboard
- **URL**: http://localhost:9090
- **Sample Queries:**
  - `orders_confirmed_total` - Total confirmed orders
  - `orders_canceled_total` - Total canceled orders
  - `order_processing_duration_seconds` - Order processing time
  - `kafka_consumer_lag` - Consumer lag per service

---

## 🔍 Debugging Tips

### View All Logs
```bash
docker-compose logs | grep "error\|WARN" | head -50
```

### Check Kafka Topics
```bash
# List all topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# View messages in topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic OrderCreated \
  --from-beginning
```

### Database Queries

**Connect to Order DB:**
```bash
docker exec -it order-db psql -U user -d orders

# Sample queries:
SELECT * FROM orders;
SELECT * FROM products;
SELECT * FROM processed_events;
```

**Connect to Payment DB:**
```bash
docker exec -it payment-db psql -U user -d payments

SELECT * FROM transactions;
```

**Connect to Inventory DB:**
```bash
docker exec -it inventory-db psql -U user -d inventory

SELECT * FROM products;
SELECT * FROM inventory_reservations;
```

### View Service Logs Directly
```bash
# Real-time logs
docker-compose logs -f order_service

# Last 100 lines
docker-compose logs order_service --tail=100

# Service startup logs
docker-compose logs fraud_payment_service 2>&1 | head -50
```

### Check Service Health
```bash
# Order Service
curl http://localhost:8000/health

# Fraud Service (no direct endpoint, check Docker health)
docker ps | grep fraud_payment_service

# Inventory Service
curl http://localhost:8001/health
```

---

## 📁 Important Paths

- **Docker Compose**: `/Users/bothainakarakrah/PycharmProjects/fraud-aware-order-system/docker-compose.yml`
- **Order Service Code**: `./order_service/app/`
- **Fraud Service Code**: `./fraud_payment_service/app/`
- **Inventory Service Code**: `./inventory_service/app/`
- **Service Logs**: `./logs/`
- **Database Configs**: `./prometheus/`

---

## 🔧 Common Issues

### Services won't start
```
Cause: Port already in use
Fix: docker-compose down && docker-compose up --build
```

### Kafka connection timeout
```
Cause: Kafka container not healthy
Fix: Wait 30 seconds longer, docker-compose may need more time
Check: docker-compose logs kafka
```

### Database connection errors
```
Cause: Database container not initialized
Fix: Remove volumes and restart
docker-compose down -v
docker-compose up --build
```

### Can't see order status updates
```
Cause: Kafka events not being processed
Fix: Check service logs for errors
docker-compose logs fraud_payment_service
docker-compose logs inventory_service
```

---

## 📚 Next Steps

1. **Understand Architecture**: Read [ARCHITECTURE.md](../ARCHITECTURE.md)
2. **Explore Code**: Read [CODEBASE_GUIDE.md](../CODEBASE_GUIDE.md)
3. **Service Details**: Check each service's `README.md`
4. **Modify Code**: Edit service logic and rebuild
   ```bash
   docker-compose up --build
   ```

---

## 🎓 Learning Path

1. ✅ Run the system (this guide)
2. 📖 Read ARCHITECTURE.md
3. 🔍 Read CODEBASE_GUIDE.md
4. 📂 Explore each service's README
5. 🐍 Review the models in each service
6. 🎯 Try modifying fraud rules in `fraud_payment_service/app/fraud.py`
7. 🚀 Deploy changes and test

---

## 💡 Pro Tips

- Keep `docker-compose logs -f` open in a terminal tab
- Use `docker ps` to see which containers are running
- Use `docker exec` to run commands inside containers
- Prometheus queries can be complex - start simple
- Each service exports metrics at `/metrics` - useful for debugging

---

## 📞 Need Help?

- Check service-specific logs: `docker-compose logs {service_name}`
- Review error messages in Prometheus or logs
- Verify all containers are healthy: `docker-compose ps`
- Check that ports 8000-8002 are not in use
