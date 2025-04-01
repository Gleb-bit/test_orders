import json
from functools import wraps
from fastapi import HTTPException

from apps.orders.models import OrderResponse
from apps.orders.tables import Order
from apps.auth.tables import User

from core.sqlalchemy.orm import Orm


def order_owner_check(func):
    """Декоратор для проверки, что заказ принадлежит текущему пользователю"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        session = kwargs["session"]
        credentials = kwargs["credentials"]
        order_id = kwargs["order_id"]

        user = await Orm.scalar(User, session, User.email == credentials.email)
        exists = await Orm.exists(
            [Order.user_id == user.id, Order.id == order_id], session
        )

        if not exists:
            raise HTTPException(
                status_code=404, detail="Заказ не найден или не принадлежит вам."
            )

        return await func(*args, **kwargs)

    return wrapper


def get_order_json(order):
    order_data = OrderResponse.model_validate(order).model_dump()
    order_data["id"] = str(order_data["id"])
    order_data["created_at"] = str(order_data["created_at"])

    return order_data


async def save_in_cache(order, redis_client):
    order_json = get_order_json(order)

    await redis_client.set(
        f"order__{order.id}",
        json.dumps(order_json),
        ex=60 * 5,
    )
