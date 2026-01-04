import logging
from pythonjsonlogger import json

SERVICE_NAME = "order-service"

def get_logger():
    logger = logging.getLogger(SERVICE_NAME)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    handlers = [
        logging.StreamHandler(),
        logging.FileHandler("logs/order-service.log")
    ]

    formatter = json.JsonFormatter(
        "%(asctime)s %(levelname)s %(service)s %(trace_id)s %(order_id)s %(event_type)s %(message)s"
    )
    for h in handlers:
        h.setFormatter(formatter)
        logger.addHandler(h)

    return logger