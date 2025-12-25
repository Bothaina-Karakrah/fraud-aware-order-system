import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.db import init_db, Base, engine
from app.events import start_consumer, stop_producer


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # --- Startup ---
    Base.metadata.create_all(bind=engine)
    init_db()

    # Handles Fraud Check, Payment, and Refunds
    consumer_task = asyncio.create_task(start_consumer())
    yield
    # --- Shutdown ---
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    await stop_producer()


app = FastAPI(title="Fraud & Payment Service", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}