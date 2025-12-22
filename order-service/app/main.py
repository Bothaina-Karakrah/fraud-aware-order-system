import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from api import router
from models import Order
from state import ORDER_STATE_MAP

from common.db import init_db
from common.events import start_consumer

@asynccontextmanager
async def lifespan(_app: FastAPI):
    # startup
    init_db()
    consumer_task = asyncio.create_task(
        start_consumer(
            topic="order-events",
            group_id="order-service-group",
            model=Order,
            state_map=ORDER_STATE_MAP,
        )
    )
    yield
    # shutdown
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Order Service", lifespan=lifespan)
app.include_router(router)