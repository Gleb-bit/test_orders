from aiokafka import AIOKafkaConsumer

from config.settings import KAFKA_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS


def get_kafka_consumer() -> "AIOKafkaConsumer":
    return AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_deserializer=lambda k: k.decode("utf-8"),
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=KAFKA_GROUP_ID,
        heartbeat_interval_ms=3000,
        session_timeout_ms=10000,
        max_poll_interval_ms=300000,
        max_poll_records=100,
    )
