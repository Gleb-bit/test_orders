import json
import logging

from kafka.admin import NewPartitions

from aiokafka import AIOKafkaProducer
from aiokafka.admin import NewPartitions
from aiokafka.errors import KafkaError
from kafka import KafkaAdminClient
from redis.asyncio import Redis

from config.settings import KAFKA_BOOTSTRAP_SERVERS


async def get_partition_key(redis: Redis, topic: str, partition_key: str, logger: logging.Logger) -> int:
    # Получаем текущий ключ партиции для topic_partition_key
    partition_key_int = await redis.get(f"kafka_partition_key_{topic}_{partition_key}")

    if not partition_key_int:
        # Получаем последний использованный номер партиции для этого topic
        last_partition = await redis.get(f"kafka_last_partition_{topic}")

        if not last_partition:
            # Если это первая партиция (кроме master), начинаем с 1
            partition_key_int = 1
        else:
            # Берем следующий номер после последнего использованного
            partition_key_int = int(last_partition) + 1

        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            api_version=(2, 13, 0),
            api_version_auto_timeout_ms=10000,
        )
        try:
            # Создаем новую партицию, если её еще нет
            partitions = admin_client.describe_topics(topics=[topic])
            current_partitions = len(partitions[0].get("partitions"))

            if partition_key_int >= current_partitions:
                success = await create_partition(
                    topic, partition_key_int + 1, KAFKA_BOOTSTRAP_SERVERS, logger
                )
                if not success:
                    exc_str = f"Не удалось создать партицию {partition_key_int} для топика {topic}"

                    logger.error(exc_str)
                    raise RuntimeError(exc_str)

            # Сохраняем маппинг partition_key -> номер партиции
            await redis.set(
                f"kafka_partition_key_{topic}_{partition_key}", partition_key_int
            )
            # Обновляем последний использованный номер партиции
            await redis.set(f"kafka_last_partition_{topic}", partition_key_int)
        finally:
            admin_client.close()

    return int(partition_key_int)


async def create_partition(
    topic: str, new_total_partitions: int, bootstrap_servers: str, logger: logging.Logger
) -> bool:
    """Создает новую партицию в топике"""
    admin_client = None

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            api_version=(2, 13, 0),
            api_version_auto_timeout_ms=10000,
        )
        topic_partitions = {topic: NewPartitions(total_count=new_total_partitions)}
        admin_client.create_partitions(topic_partitions)

        return True
    except Exception as e:
        logger.error(
            "Ошибка при создании партиции",
            extra={
                "topic": topic,
                "new_total_partitions": new_total_partitions,
                "error": str(e),
            },
        )
        return False
    finally:
        if admin_client:
            admin_client.close()


async def ensure_partitions_exist(
    topic: str,
    required_partitions: int,
    bootstrap_servers: str,
    logger: logging.Logger,
) -> None:
    """Проверка и создание недостающих партиций для топика Kafka."""

    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        api_version=(2, 13, 0),
        api_version_auto_timeout_ms=10000,
    )
    try:
        # Получаем информацию о текущих партициях топика
        topic_metadata = admin_client.describe_topics(topics=[topic])
        current_partitions = len(topic_metadata[0].get("partitions"))

        if required_partitions > current_partitions:
            logger.info(
                f"Создание {required_partitions - current_partitions} новых партиций для топика {topic}"
            )
            await create_partition(topic, required_partitions, bootstrap_servers, logger)

    except Exception as e:
        logger.error(
            f"Ошибка при проверке/создании партиций для топика {topic}: {str(e)}"
        )

    finally:
        admin_client.close()


async def send_to_producer(
    producer: "AIOKafkaProducer",
    processed_objects: list[dict],
    logger: logging.Logger,
    redis: Redis,
    topic: str = "new_order",
    partition_key: str = "master",
) -> None:
    """Отправка валидных объектов в Kafka producer"""

    max_partition_key = max(
        [
            await get_partition_key(redis, topic, partition_key, logger)
            for obj in processed_objects
        ]
    )

    await ensure_partitions_exist(
        topic=topic,
        required_partitions=max_partition_key + 1,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        logger=logger,
    )
    await producer.start()

    logger.info(f"Отправка в Kafka {len(processed_objects)} объектов для {topic}")

    for obj in processed_objects:
        try:
            partition_key_int = await get_partition_key(redis, topic, partition_key, logger)

            await producer.send_and_wait(
                topic,
                value=json.dumps(obj, ensure_ascii=False),
                key=partition_key,
                partition=partition_key_int,
            )

        except KafkaError as e:
            exc_str = str(e)

            if "Unknown partition" in exc_str:
                logger.warning(
                    "Партиция не существует, пробуем создать",
                    extra={"topic": topic, "partition": partition_key_int},
                )

                success = await create_partition(
                    topic,
                    partition_key_int,
                    KAFKA_BOOTSTRAP_SERVERS,
                    logger
                )

                if success:
                    await producer.send_and_wait(
                        topic,
                        value=json.dumps(obj, ensure_ascii=False),
                        key=partition_key,
                        partition=partition_key_int,
                    )
                else:
                    raise RuntimeError(
                        f"Не удалось создать партицию {partition_key_int} для топика {topic}"
                    )

            else:
                logger.error(
                    f"Ошибка при отправке в Kafka: {exc_str}",
                    extra={"topic": topic},
                )

        except Exception as e:
            logger.error(
                f"Ошибка при отправке в Kafka: {str(e)}",
                extra={"topic": topic},
            )

        finally:
            await producer.stop()
