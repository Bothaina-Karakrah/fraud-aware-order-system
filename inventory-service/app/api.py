from sqlalchemy.orm import Session
from models import Inventory
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, UUID4, conint
from app.db import get_db
router = APIRouter()

class InventoryResponse(BaseModel):
    product_id: UUID4
    available_quantity: int
    reserved_quantity: int
    updated_at: str

@router.get("/inventory/{product_id}", response_model=InventoryResponse)
async def get_inventory(product_id: UUID4, db: Session = Depends(get_db)):
    inv = db.query(Inventory).filter(Inventory.product_id == product_id).first()
    if not inv:
        raise HTTPException(status_code=404, detail="Product not found")
    return {
        "product_id": inv.product_id,
        "available_quantity": inv.available_quantity,
        "reserved_quantity": inv.reserved_quantity,
        "updated_at": inv.updated_at,
    }