import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from db import init_db
from events import start_consumer

@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    fraud_consumer = asyncio.create_task(start_consumer())
    yield
    fraud_consumer.cancel()
    try:
        await fraud_consumer
    except asyncio.CancelledError:
        pass

app = FastAPI(title="Inventory Service", lifespan=lifespan)