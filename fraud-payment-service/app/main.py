import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from api import router
from models import Payment
from state import PAYMENT_STATE_MAP, FRAUD_STATE_MAP

from common.db import init_db
from common.events import start_consumer


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()

    fraud_consumer = asyncio.create_task(
        start_consumer(
            topic="order-events",
            group_id="fraud-service-group",
            model=Payment,
            state_map=FRAUD_STATE_MAP,
        )
    )

    payment_consumer = asyncio.create_task(
        start_consumer(
            topic="payment-events",
            group_id="payment-service-group",
            model=Payment,
            state_map=PAYMENT_STATE_MAP,
        )
    )

    yield

    for task in (fraud_consumer, payment_consumer):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Fraud & Payment Service", lifespan=lifespan)
app.include_router(router)