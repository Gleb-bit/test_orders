import logging
import asyncio
import uuid

from aiokafka import AIOKafkaConsumer, TopicPartition, ConsumerRebalanceListener
from kafka import KafkaAdminClient

from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPICS
from core.kafka.utils import ensure_topics_exist, get_msg_data
from core.redis.utils import get_redis_client

logger = logging.getLogger("consumer")


class PeriodicRebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer: "AIOKafkaConsumer", rebalance_interval: int):
        self.consumer = consumer
        self.rebalance_interval = rebalance_interval
        self.topics: list[TopicPartition] = []
        self.redis_client = get_redis_client()
        self.known_partition_counts: dict[str, int] = {}

        self.admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            api_version=(2, 13, 0),
            api_version_auto_timeout_ms=10000,
        )

    async def force_rebalance(self) -> None:
        try:
            logger.info("Начало ребалансировки")
            self.consumer.assign(self.topics)
            logger.info("Ребалансировка успешно выполнена")
        except Exception as e:
            logger.error(f"Ошибка при ребалансировке: {e}")

    async def get_current_partition_counts(self) -> dict:
        """Получает текущее количество партиций для каждого топика"""

        try:
            cluster_metadata = self.admin_client.describe_topics(TOPICS)
            return {
                topic.get("topic"): len(topic.get("partitions"))
                for topic in cluster_metadata
            }
        except Exception as e:
            logger.error(f"Ошибка при получении метаданных топиков: {e}")
            return {}

    async def check_new_partitions(self) -> bool:
        """Проверяет, появились ли новые партиции"""

        current_counts = await self.get_current_partition_counts()

        if not self.known_partition_counts:
            # Первый запуск - сохраняем текущее состояние
            self.known_partition_counts = current_counts
            return False

        has_new_partitions = False
        for topic, count in current_counts.items():
            old_count = self.known_partition_counts.get(topic, 0)
            if count > old_count:
                logger.info(
                    "Обнаружены новые партиции",
                    extra={"topic": topic, "old_count": old_count, "new_count": count},
                )
                has_new_partitions = True

        if has_new_partitions:
            self.known_partition_counts = current_counts
            self.topics = await self.get_topics()

        return has_new_partitions

    async def rebalance_loop(self) -> None:
        """Цикл ребалансировки"""

        while True:
            try:
                await asyncio.sleep(self.rebalance_interval)

                if await self.check_new_partitions():
                    logger.info("Запуск ребалансировки из-за новых партиций")
                    await self.force_rebalance()
                else:
                    logger.debug("Новых партиций не обнаружено")

            except Exception as e:
                logger.error(f"Ошибка в цикле ребалансировки: {e}")
                await asyncio.sleep(5)

    async def get_topics(self) -> list[TopicPartition]:
        """Получает список всех партиций для каждого топика"""

        topics_partitions = []
        try:
            # Получаем метаданные для всех топиков
            topic_metadata = self.admin_client.describe_topics(TOPICS)

            # Для каждого топика получаем все его партиции
            for topic in topic_metadata:
                partition_count = len(topic.get("partitions"))
                # Добавляем TopicPartition для каждой партиции
                for partition in range(partition_count):
                    topics_partitions.append(
                        TopicPartition(topic=topic.get("topic"), partition=partition)
                    )

                logger.info(
                    f"Получены партиции для топика {topic.get('topic')}",
                    extra={
                        "topic": topic.get("topic"),
                        "partition_count": partition_count,
                        "partitions": list(range(partition_count)),
                    },
                )

        except Exception as e:
            logger.error(f"Ошибка при получении партиций топиков: {e}", exc_info=True)

        return topics_partitions

    async def start(self) -> None:
        self.topics = await self.get_topics()
        await ensure_topics_exist(self.admin_client)
        await self.consumer.start()

        self.consumer.assign(self.topics)

        self.rebalance_task = asyncio.create_task(self.rebalance_loop())

        try:
            while True:
                async for msg in self.consumer:
                    message_uuid = str(uuid.uuid4())
                    logger.info(
                        "Получено сообщение",
                        extra=get_msg_data(msg) | {"message_uuid": message_uuid},
                    )
                    pass
        except Exception as e:
            logger.error(f"Ошибка при запуске консьюмера: {e}")

            pass
        finally:
            if hasattr(self, "rebalance_task"):
                self.rebalance_task.cancel()  # Отменяем задачу ребалансировки
            await self.consumer.stop()

    async def on_partitions_assigned(self, assigned: list[TopicPartition]):
        logger.info(f"Получены новые партиции: {assigned}")
        self.current_partitions = set(assigned)
        logger.info(f"Текущие активные партиции: {self.current_partitions}")

    async def on_partitions_revoked(self, revoked: list[TopicPartition]):
        logger.info(f"Партиции отзываются: {revoked}")
        self.current_partitions -= set(revoked)
        logger.info(f"Текущие активные партиции: {self.current_partitions}")
