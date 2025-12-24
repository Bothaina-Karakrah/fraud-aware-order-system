from pydantic import BaseModel, UUID4, conint
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from models import Order, OrderStatus, Product
from events import publish_event
import uuid

router = APIRouter()

class CreateOrderRequest(BaseModel):
    user_id: UUID4
    product_id: UUID4
    quantity: conint(gt=0)
    payment_method: str

class CreateOrderResponse(BaseModel):
    order_id: UUID4
    status: str
    created_at: str

class OrderResponse(BaseModel):
    order_id: UUID4
    user_id: UUID4
    product_id: UUID4
    quantity: int
    amount: float
    status: str
    created_at: str
    updated_at: str


@router.post("/orders", response_model=CreateOrderResponse, status_code=201)
async def create_order(req: CreateOrderRequest, db: Session = Depends(get_db)):
    # Validate product
    product = db.query(Product).filter(Product.product_id == req.product_id).first()
    if not product:
        raise HTTPException(status_code=400, detail="Invalid product_id")

    # Calculate total amount
    amount = product.price * req.quantity

    try:
        # Create order
        order = Order(
            order_id=uuid.uuid4(),
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

        # Publish event asynchronously - push to Kafka
        try:
            await publish_event(
                topic="order-events",
                event_type="OrderCreated",
                payload={
                    "order_id": str(order.order_id),
                    "user_id": str(req.user_id),
                    "product_id": str(req.product_id),
                    "quantity": req.quantity,
                    "payment_method": str(req.payment_method),
                    "amount": float(amount),
                },
            )
        except Exception as e:
            print(f"Failed to publish event: {e}")
            # Order is saved but event not published
            # Could implement retry logic or dead letter queue here

        return {
            "order_id": order.order_id,
            "status": order.status.value,
            "created_at": order.created_at.isoformat() + "Z"
        }

    except Exception as e:
        db.rollback()
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail="Service failure")


@router.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: str, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.order_id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return {
        "order_id": order.order_id,
        "user_id": order.user_id,
        "product_id": order.product_id,
        "quantity": order.quantity,
        "amount": order.amount,
        "status": order.status.value,
        "created_at": order.created_at.isoformat() + "Z",
        "updated_at": order.updated_at.isoformat() + "Z",
    }

# Fow health check
@router.get("/health")
async def health():
    return {"status": "ok"}