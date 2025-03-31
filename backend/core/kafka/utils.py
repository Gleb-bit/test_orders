import logging

from aiokafka import TopicPartition, ConsumerRecord
from aiokafka.admin import NewPartitions
from kafka import KafkaAdminClient

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_PARTITIONS,
    KAFKA_REPLICATION_FACTOR,
    TOPICS,
)
from core.kafka.consumer import get_kafka_consumer


logger = logging.getLogger("consumer")


async def ensure_topics_exist(admin_client: KafkaAdminClient) -> None:
    """Проверяет существование топиков и создает их при необходимости."""

    try:
        existing_topics = admin_client.list_topics()
        topics_to_create = [topic for topic in TOPICS if topic not in existing_topics]

        if topics_to_create:
            from kafka.admin import NewTopic

            new_topics = [
                NewTopic(
                    name=topic,
                    num_partitions=KAFKA_PARTITIONS,
                    replication_factor=KAFKA_REPLICATION_FACTOR,
                )
                for topic in topics_to_create
            ]
            admin_client.create_topics(new_topics)
            logger.info(f"Созданы топики: {topics_to_create}")
    except Exception as e:
        logger.error(f"Ошибка при создании топиков: {e}")
        raise


def get_msg_data(msg: ConsumerRecord, optional: bool = True) -> dict:
    data = {
        "topic": msg.topic,
        "key": msg.key if msg.key else None,
        "value": msg.value if msg.value else None,
    }
    if optional:
        data |= {
            "partition": msg.partition,
            "offset": msg.offset,
            "headers": msg.headers,
        }

    return data


class KafkaManager:

    def __init__(self, bootstrap_servers):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            api_version=(2, 13, 0),
            api_version_auto_timeout_ms=10000,
        )

    @staticmethod
    async def count_messages_in_partition(topic: str, partition: int) -> int:
        """Асинхронно возвращает количество сообщений в указанной партиции"""

        consumer = get_kafka_consumer()

        try:
            tp = TopicPartition(topic, partition)
            await consumer.start()
            consumer.assign([tp])

            await consumer.seek_to_beginning(tp)
            first_offset = await consumer.position(tp)

            await consumer.seek_to_end(tp)
            last_offset = await consumer.position(tp)

            return last_offset - first_offset

        finally:
            await consumer.stop()

    async def create_partitions(self, topic: str):
        """Создает партиции в топике"""

        new_partition_count = self.get_partition_count(topic) + 1
        topic_partitions = {topic: NewPartitions(total_count=new_partition_count)}
        self.admin_client.create_partitions(topic_partitions)

        consumer = get_kafka_consumer()
        await consumer.start()

        consumer.assign([TopicPartition(topic, new_partition_count)])
        await consumer.stop()

    def delete_topic(self, topic: str):
        """Удаляет топик из Kafka"""

        try:
            self.admin_client.delete_topics([topic])
        except Exception:
            pass

    def delete_all_topics(self):
        for topic in self.get_topics():
            self.delete_topic(topic)

    def get_partition_count(self, topic: str) -> int:
        """Получает количество партиций в топике"""

        topic_metadata = self.get_topics_data([topic])
        return len(topic_metadata[0]["partitions"])

    def get_partition_for_message(self, topic: str, key: int):
        """Определяет, в какую партицию Kafka отправит сообщение с заданным ключом"""

        topic_metadata = self.get_topics_data([topic])

        partitions = topic_metadata[0]["partitions"]
        partition_index = hash(key) % len(partitions)

        logger.info(
            f"Сообщение с ключом {key} должно попасть в партицию {partition_index}"
        )
        return partition_index

    def get_topics(self) -> list[str]:
        """Получает названия всех топиков"""

        return self.admin_client.list_topics()

    def get_topics_data(self, topics: list[str] = None) -> list[dict]:
        """Получает данные об указанных топиках"""

        return self.admin_client.describe_topics(topics=topics)

    def get_topic_partitions(self, topic: str) -> list[int]:
        """Возвращает список всех партиций в топике"""

        topic_metadata = self.get_topics_data([topic])
        return [p["partition"] for p in topic_metadata[0]["partitions"]]

    def get_topics_and_partitions(
        self, topics: list[str] = None
    ) -> list[TopicPartition]:
        """Получает список всех топиков и партиций для каждого топика"""

        topics_partitions = []
        try:
            topic_metadata = self.get_topics_data(topics)

            for topic in topic_metadata:
                partition_count = len(topic.get("partitions", []))
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

    @staticmethod
    async def is_message_in_partition(
        topic: str, partition: int, search_value: str, encoding: str = "utf-8"
    ) -> bool:
        """Проверяет, есть ли указанное сообщение в партиции"""

        search_bytes = search_value.encode(encoding)

        consumer = get_kafka_consumer()
        tp = TopicPartition(topic, partition)

        await consumer.start()
        consumer.assign([tp])

        try:
            await consumer.seek_to_beginning(tp)

            async for msg in consumer:
                if msg.value == search_bytes:
                    return True

        finally:
            await consumer.stop()

        return False

    async def is_partition_readable(self, topic: str, partition: int) -> bool:
        """Проверяет, читаются ли сообщения из указанной партиции"""

        if partition not in self.get_topic_partitions(topic):
            return False

        consumer = get_kafka_consumer()
        await consumer.start()

        tp = TopicPartition(topic, partition)

        try:
            if tp:
                consumer.assign([tp])
                await consumer.seek_to_end(tp)

                last_offset = await consumer.position(tp)
                return last_offset > 0
        finally:
            await consumer.stop()


def get_kafka_manager(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS) -> KafkaManager:
    return KafkaManager(bootstrap_servers=bootstrap_servers)
