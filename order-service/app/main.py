import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api import router
from app.db import init_db, Base, engine
from app.events import stop_producer, start_consumer
from app.logging import get_logger

logger = get_logger()

@asynccontextmanager
async def lifespan(_app: FastAPI):
    # --- Startup ---
    Base.metadata.create_all(bind=engine)
    init_db()

    # Start the Kafka consumer as a background task
    # This allows the Order Service to react to events from other services
    consumer_task = asyncio.create_task(start_consumer())

    yield

    # --- Shutdown ---
    # Cancel the consumer task
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    await stop_producer()


app = FastAPI(title="Order Service", lifespan=lifespan)
app.include_router(router)