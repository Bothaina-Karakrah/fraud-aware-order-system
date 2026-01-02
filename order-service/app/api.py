from uuid import UUID, uuid4
from pydantic import BaseModel, conint
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db import get_db
from app.models import Order, OrderStatus, Product
from app.events import publish_event
from app.logging import get_logger
from app.metrics import (
    order_status_transitions_total,
    orders_created_total
)

logger = get_logger()
router = APIRouter()

class CreateOrderRequest(BaseModel):
    user_id: UUID
    product_id: UUID
    quantity: conint(gt=0)
    payment_method: str

class CreateOrderResponse(BaseModel):
    order_id: UUID
    status: str
    created_at: str

class OrderResponse(BaseModel):
    order_id: UUID
    user_id: UUID
    product_id: UUID
    quantity: int
    amount: float
    status: str
    created_at: str
    updated_at: str

# --- Routes ---

@router.post("/orders", response_model=CreateOrderResponse, status_code=201)
async def create_order(req: CreateOrderRequest, db: Session = Depends(get_db)):
    """Create a new order, push event to Kafka, and record counters"""
    trace_id = str(uuid4())

    # Validate product exists
    product = db.query(Product).filter(Product.product_id == req.product_id).first()
    if not product:
        raise HTTPException(status_code=400, detail="Invalid product_id")

    # Calculate total order amount
    amount = product.price * req.quantity

    try:
        logger.info(
            "Start create_order request",
            extra={
                "service": "order-service",
                "trace_id": trace_id,
                "order_id": None,
                "event_type": "OrderCreated"
            }
        )

        # Add the order to the database
        order = Order(
            order_id=uuid4(),
            user_id=req.user_id,
            product_id=req.product_id,
            quantity=req.quantity,
            payment_method=req.payment_method,
            amount=amount,
            status=OrderStatus.CREATED
        )
        db.add(order)
        db.commit()
        db.refresh(order)

        # Increment counters
        orders_created_total.inc()
        order_status_transitions_total.labels(status="created").inc()

        # Publish event to Kafka
        try:
            await publish_event(
                topic="order-events",
                event_type="OrderCreated",
                payload={
                    "order_id": str(order.order_id),
                    "user_id": str(req.user_id),
                    "product_id": str(req.product_id),
                    "quantity": req.quantity,
                    "payment_method": req.payment_method,
                    "amount": float(amount),
                },
                trace_id=trace_id
            )
        except Exception as e:
            logger.error(
                f"Failed to publish event: {e}",
                extra={
                    "service": "order-service",
                    "trace_id": trace_id,
                    "order_id": str(order.order_id),
                    "event_type": "OrderCreated"
                }
            )

        return CreateOrderResponse(
            order_id=order.order_id,
            status=order.status.value,
            created_at=order.created_at.isoformat() + "Z"
        )

    except Exception as e:
        db.rollback()
        logger.error(
            f"Error creating order: {e}",
            extra={
                "service": "order-service",
                "trace_id": trace_id,
                "order_id": None,
                "event_type": "OrderCreated"
            }
        )
        raise HTTPException(status_code=500, detail="Service failure")


@router.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: UUID, db: Session = Depends(get_db)):
    """Fetch an order by ID"""
    try:
        order = db.query(Order).filter(Order.order_id == order_id).first()
    except Exception as e:
        logger.error(f"Error fetching order {order_id}: {e}")
        raise HTTPException(status_code=500, detail="Service failure")

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    return OrderResponse(
        order_id=order.order_id,
        user_id=order.user_id,
        product_id=order.product_id,
        quantity=order.quantity,
        amount=order.amount,
        status=order.status.value,
        created_at=order.created_at.isoformat() + "Z",
        updated_at=order.updated_at.isoformat() + "Z"
    )


@router.get("/health")
async def health():
    return {"status": "ok"}