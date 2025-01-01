import json
import logging
import grpc
import redis
import pika
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, Field, validator
from prometheus_client import Counter, Summary, generate_latest, CONTENT_TYPE_LATEST
from pythonjsonlogger.json import JsonFormatter
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

import domain_service_pb2
import domain_service_pb2_grpc

# Настройка логирования
logger = logging.getLogger("gateway")
logger.setLevel(logging.INFO)
logHandler = logging.StreamHandler()
logHandler.setFormatter(JsonFormatter())
logger.addHandler(logHandler)

# Настройка OpenTelemetry
trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create({"service.name": "gateway"})
    )
)
tracer = trace.get_tracer(__name__)

# Инициализация FastAPI
app = FastAPI()

# Инструментирование FastAPI
FastAPIInstrumentor.instrument_app(app)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Настройка Redis
redis_client = redis.Redis(host='redis', port=6379, db=0)

# Настройка RabbitMQ
credentials = pika.PlainCredentials("admin", "admin")
rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials=credentials))
rabbit_channel = rabbit_connection.channel()
rabbit_channel.queue_declare(queue='crud_operations', durable=True)

# Настройка Prometheus
REQUEST_COUNT = Counter('request_count', 'App Request Count', ['method', 'endpoint'])
REQUEST_LATENCY = Summary('request_latency_seconds', 'Request latency', ['endpoint'])

# gRPC Stub
def get_grpc_stub():
    channel = grpc.insecure_channel('domain_service:50051')
    stub = domain_service_pb2_grpc.DomainServiceStub(channel)
    return stub

# Pydantic модели
class ScheduleBase(BaseModel):
    day_of_week: str = Field(..., example="Monday")
    start_time: str = Field(..., example="09:00")
    subject: str = Field(..., example="Mathematics")
    teacher: str = Field(..., example="John Doe")

    @validator('day_of_week')
    def validate_day(cls, v):
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        if v not in days:
            raise ValueError('Invalid day of week')
        return v

    @validator('start_time')
    def validate_time(cls, v):
        import re
        pattern = re.compile(r'^([01]\d|2[0-3]):([0-5]\d)$')
        if not pattern.match(v):
            raise ValueError('Invalid time format, should be HH:MM')
        return v

class ScheduleCreate(ScheduleBase):
    pass

class ScheduleUpdate(ScheduleBase):
    id: str = Field(..., example="unique_id")


# Middleware для Prometheus
@app.middleware("http")
async def prometheus_middleware(request, call_next):
    REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path).inc()
    with REQUEST_LATENCY.labels(endpoint=request.url.path).time():
        response = await call_next(request)
    return response


@app.get("/schedules")
def list_schedules():
    cache_key = "schedules:all"
    cache_ttl = 300  # Время жизни кэша в секундах (5 минут)

    try:
        # Попытка получить данные из кэша
        cached_schedules = redis_client.get(cache_key)
        if cached_schedules:
            logger.info("Cache hit for schedules")
            schedules = json.loads(cached_schedules)
            return schedules
        else:
            logger.info("Cache miss for schedules, querying gRPC")
            # Запрос к gRPC сервису
            stub = get_grpc_stub()
            request = domain_service_pb2.ListRequest()
            response = stub.ListSchedules(request)
            # Преобразование ответа в список словарей
            schedules = [
                {
                    "id": schedule.id,
                    "day_of_week": schedule.day_of_week,
                    "start_time": schedule.start_time,
                    "subject": schedule.subject,
                    "teacher": schedule.teacher
                }
                for schedule in response.schedules
            ]
            # Сохранение данных в кэш с установленным TTL
            redis_client.setex(cache_key, cache_ttl, json.dumps(schedules))
            logger.info("Schedules cached successfully")
            return schedules
    except grpc.RpcError as e:
        logger.error(f"gRPC error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Эндпоинт GET через gRPC с кешированием
@app.get("/schedule/{item_id}")
def get_schedule(item_id: str):
    cache_key = f"schedule:{item_id}"
    cached = redis_client.get(cache_key)
    if cached:
        logger.info(f"Cache hit for {cache_key}")
        return json.loads(cached)
    logger.info(f"Cache miss for {cache_key}, querying domain service via gRPC")
    stub = get_grpc_stub()
    request = domain_service_pb2.GetRequest(id=item_id)
    try:
        response = stub.GetSchedule(request)
        schedule = json.loads(response.schedule)
        redis_client.set(cache_key, json.dumps(schedule), ex=60)  # Кеш на 60 секунд
        return schedule
    except grpc.RpcError as e:
        logger.error(f"gRPC error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Эндпоинт POST через RabbitMQ
@app.post("/schedule")
def create_schedule(schedule: ScheduleCreate):
    message = {
        "operation": "create",
        "data": schedule.dict()
    }
    try:
        rabbit_channel.basic_publish(
            exchange='',
            routing_key='crud_operations',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        logger.info(f"Sent create operation to RabbitMQ: {message}")
        return {"status": "success", "operation": "create"}
    except Exception as e:
        logger.error(f"Failed to send message to RabbitMQ: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Эндпоинт PUT через RabbitMQ
@app.put("/schedule/{item_id}")
def update_schedule(item_id: str, schedule: ScheduleUpdate):
    if item_id != schedule.id:
        raise HTTPException(status_code=400, detail="ID in path and body do not match")
    message = {
        "operation": "update",
        "data": schedule.dict()
    }
    try:
        rabbit_channel.basic_publish(
            exchange='',
            routing_key='crud_operations',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        logger.info(f"Sent update operation to RabbitMQ: {message}")
        return {"status": "success", "operation": "update"}
    except Exception as e:
        logger.error(f"Failed to send message to RabbitMQ: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Эндпоинт DELETE через RabbitMQ
@app.delete("/schedule/{item_id}")
def delete_schedule(item_id: str):
    message = {
        "operation": "delete",
        "data": {"id": item_id}
    }
    try:
        rabbit_channel.basic_publish(
            exchange='',
            routing_key='crud_operations',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        logger.info(f"Sent delete operation to RabbitMQ: {message}")
        return {"status": "success", "operation": "delete"}
    except Exception as e:
        logger.error(f"Failed to send message to RabbitMQ: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Эндпоинт для метрик Prometheus
@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
