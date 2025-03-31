from os import environ

# FastAPI
DATABASE_URL = environ.get("DATABASE_URL")

# Redis
REDIS_HOST = environ.get("REDIS_HOST")
REDIS_PORT = environ.get("REDIS_PORT")
REDIS_PASSWORD = environ.get("REDIS_PASSWORD")
REDIS_DB = "0"
REDIS_CELERY_DB = "1"
REDIS_CELERY_RESULT_DB = "2"

# Celery
CELERY_BROKER_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
CELERY_MAX_TASKS_PER_CHILD = 10
accept_content = ["json"]
result_serializer = "json"
result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_RESULT_DB}"
task_serializer = "json"
broker_connection_retry_on_startup = True

# Kafka
KAFKA_BOOTSTRAP_SERVERS = environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = environ.get("KAFKA_GROUP_ID")
KAFKA_PARTITIONS = int(environ.get("KAFKA_PARTITIONS"))
KAFKA_REPLICATION_FACTOR = int(environ.get("KAFKA_REPLICATION_FACTOR"))

TOPICS = ["new_order"]
