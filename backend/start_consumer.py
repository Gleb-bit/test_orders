import asyncio
import logging
import sys

from apps.consumer.services import process_message
from config.logger import setup_logger
from core.kafka.utils import get_kafka_consumer
from core.kafka.rebalance import PeriodicRebalanceListener

setup_logger("consumer")
logger = logging.getLogger("consumer")


async def main():
    consumer = get_kafka_consumer()
    listener = PeriodicRebalanceListener(consumer, rebalance_interval=2)

    try:
        await listener.start(process_message)
    except asyncio.CancelledError:
        logger.info("Получен сигнал завершения")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        # Убедимся, что все ресурсы освобождены
        if hasattr(listener, "admin_client"):
            listener.admin_client.close()


if __name__ == "__main__":

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа завершена пользователем")
        sys.exit(0)
