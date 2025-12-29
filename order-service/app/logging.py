import logging
import sys
from pythonjsonlogger import json

SERVICE_NAME = "order-service"

def get_logger():
    logger = logging.getLogger(SERVICE_NAME)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)

    formatter = json.JsonFormatter(
        "%(asctime)s %(levelname)s %(service)s %(trace_id)s %(order_id)s %(event_type)s %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger