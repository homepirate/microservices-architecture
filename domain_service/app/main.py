import json
import logging
import grpc
from concurrent import futures
import time
import pika
from pymongo import MongoClient

import domain_service_pb2
import domain_service_pb2_grpc

# Настройка логирования
logger = logging.getLogger("domain_service")
logger.setLevel(logging.INFO)
logHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

# Настройка MongoDB
mongo_client = MongoClient('mongodb://mongo:27017/')
db = mongo_client['schedule_db']
collection = db['schedules']

# Настройка RabbitMQ
credentials = pika.PlainCredentials("admin", "admin")
rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials=credentials))
rabbit_channel = rabbit_connection.channel()
rabbit_channel.queue_declare(queue='crud_operations', durable=True)

class DomainServiceServicer(domain_service_pb2_grpc.DomainServiceServicer):
    def GetSchedule(self, request, context):
        schedule = collection.find_one({"_id": request.id})
        if schedule:
            schedule['_id'] = str(schedule['_id'])  # Преобразуем ObjectId в строку
            return domain_service_pb2.GetResponse(schedule=json.dumps(schedule))
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Item not found')
            return domain_service_pb2.GetResponse()

    def ListSchedules(self, request, context):
        try:
            schedules_cursor = collection.find()
            schedules = []
            for sched in schedules_cursor:
                sched['id'] = str(sched['_id'])  # Преобразуем ObjectId в строку
                del sched['_id']  # Удаляем поле _id
                schedules.append(domain_service_pb2.Schedule(
                    id=sched['id'],
                    day_of_week=sched['day_of_week'],
                    start_time=sched['start_time'],
                    subject=sched['subject'],
                    teacher=sched['teacher']
                ))
            return domain_service_pb2.ListResponse(schedules=schedules)
        except Exception as e:
            logger.error(f"Error listing schedules: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Internal Server Error')
            return domain_service_pb2.ListResponse()

def handle_create(data):
    try:
        collection.insert_one(data)
        logger.info(f"Created schedule: {data}")
    except Exception as e:
        logger.error(f"Error creating schedule: {e}")

def handle_update(data):
    try:
        collection.update_one({"_id": data["id"]}, {"$set": data})
        logger.info(f"Updated schedule with id {data['id']}: {data}")
    except Exception as e:
        logger.error(f"Error updating schedule: {e}")

def handle_delete(data):
    try:
        collection.delete_one({"_id": data["id"]})
        logger.info(f"Deleted schedule with id {data['id']}")
    except Exception as e:
        logger.error(f"Error deleting schedule: {e}")

    def ListSchedules(self, request, context):
        try:
            schedules_cursor = collection.find()
            schedules = []
            for sched in schedules_cursor:
                sched['id'] = str(sched['_id'])  # Преобразуем ObjectId в строку
                del sched['_id']  # Удаляем поле _id
                schedules.append(domain_service_pb2.Schedule(
                    id=sched['id'],
                    day_of_week=sched['day_of_week'],
                    start_time=sched['start_time'],
                    subject=sched['subject'],
                    teacher=sched['teacher']
                ))
            return domain_service_pb2.ListResponse(schedules=schedules)
        except Exception as e:
            logger.error(f"Error listing schedules: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Internal Server Error')
            return domain_service_pb2.ListResponse()

def callback(ch, method, properties, body):
    message = json.loads(body)
    operation = message.get('operation')
    data = message.get('data')
    logger.info(f"Received operation: {operation} with data: {data}")

    if operation == 'create':
        handle_create(data)
    elif operation == 'update':
        handle_update(data)
    elif operation == 'delete':
        handle_delete(data)
    else:
        logger.warning(f"Unknown operation: {operation}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_messages():
    rabbit_channel.basic_qos(prefetch_count=1)
    rabbit_channel.basic_consume(queue='crud_operations', on_message_callback=callback)
    logger.info("Started consuming RabbitMQ messages")
    rabbit_channel.start_consuming()

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    domain_service_pb2_grpc.add_DomainServiceServicer_to_server(DomainServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logger.info("gRPC server started on port 50051")
    try:
        while True:
            time.sleep(86400)  # Один день
    except KeyboardInterrupt:
        server.stop(0)
        logger.info("gRPC server stopped")

if __name__ == '__main__':
    import threading
    # Запуск потребителя RabbitMQ в отдельном потоке
    rabbit_thread = threading.Thread(target=consume_messages, daemon=True)
    rabbit_thread.start()

    # Запуск gRPC сервера
    serve_grpc()