import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from db import init_db, Base, engine
from events import start_consumer


@asynccontextmanager
async def lifespan(_app: FastAPI):
    Base.metadata.create_all(bind=engine)
    init_db()
    # Start the consumer to watch for PaymentSucceeded
    consumer_task = asyncio.create_task(start_consumer())
    yield
    consumer_task.cancel()


app = FastAPI(title="Inventory Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "online"}