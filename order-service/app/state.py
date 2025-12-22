from models import OrderStatus

ORDER_STATE_MAP = {
    "OrderApproved": OrderStatus.APPROVED,
    "OrderBlocked": OrderStatus.CANCELED,
    "PaymentSucceeded": OrderStatus.PAID,
    "PaymentFailed": OrderStatus.CANCELED,
    "StockReserved": OrderStatus.CONFIRMED,
    "StockReservationFailed": OrderStatus.CANCELED,
}