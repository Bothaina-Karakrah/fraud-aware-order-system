from sqlalchemy.orm import Session
from app.models import Inventory
from uuid import UUID

def reserve_stock(product_id: str, quantity: int, db: Session) -> tuple[bool, str]:
    """Reserve stock with database row lock and UUID validation"""
    try:
        # Validate and convert product_id to UUID
        try:
            product_uuid = UUID(product_id)
        except ValueError:
            return False, f"Invalid product_id: {product_id}"

        # Lock the row during this transaction
        inv = (
            db.query(Inventory)
            .filter(Inventory.product_id == product_uuid)
            .with_for_update()
            .first()
        )

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