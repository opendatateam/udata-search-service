import asyncio
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from app.config import settings
from app.presentation import api

from app.presentation.api.api_v1 import api_router


async def consume():
    await consumer.start()
    try:
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        await consumer.stop()

loop = asyncio.get_event_loop()
consumer = AIOKafkaConsumer('dataset', loop=loop, bootstrap_servers='localhost:9092', group_id='elastic')

app = FastAPI(
    title=settings.PROJECT_NAME, openapi_url=f'{settings.API_V1_STR}/openapi.json'
)

app.include_router(api_router, prefix=settings.API_V1_STR)

# @app.on_event("startup")
# def startup_event():
#     asyncio.create_task(consume())
#
# @app.on_event("shutdown")
# def shutdown_event():
#     asyncio.create_task(consumer.stop())
