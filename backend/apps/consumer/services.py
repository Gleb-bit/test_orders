import json

from aiokafka import ConsumerRecord

from config.celery import process_order


def process_message(msg: ConsumerRecord):
    order_id = json.loads(msg.value)
    process_order.delay(order_id)
