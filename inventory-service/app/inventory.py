from sqlalchemy.orm import Session
from models import Inventory


def reserve_stock(product_id: str, quantity: int, db: Session) -> tuple[bool, str]:
    """Reserve stock with database row lock"""
    try:
        # Lock the row during this transaction
        inv = db.query(Inventory).filter(
            Inventory.product_id == product_id
        ).with_for_update().first()

        if not inv:
            return False, "Product not found"

        if inv.available_quantity < quantity:
            return False, "Insufficient stock"

        # Update stock
        inv.available_quantity -= quantity
        inv.reserved_quantity += quantity
        db.commit()

        return True, "Stock reserved successfully"

    except Exception as e:
        db.rollback()
        return False, f"Database error: {e}"