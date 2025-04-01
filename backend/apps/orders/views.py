import json
import logging
import uuid

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Depends, HTTPException
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from apps.auth.conf import AUTH_MODEL, auth
from apps.auth.tables import User
from apps.orders.models import OrderCreate, OrderResponse, OrderUpdate
from apps.orders.services import save_in_cache, order_owner_check
from apps.orders.tables import Order
from config.database import get_session
from core.kafka.producer import send_to_producer, get_producer
from core.redis.utils import get_async_redis_client
from core.sqlalchemy.crud import Crud
from core.sqlalchemy.orm import Orm

crud = Crud(Order)

logger = logging.getLogger("api")

orders_router = APIRouter(dependencies=[Depends(auth.get_request_user)])


@orders_router.post("/", response_model=OrderResponse)
async def create_order(
    order: OrderCreate,
    session: AsyncSession = Depends(get_session),
    credentials: AUTH_MODEL = Depends(auth.get_request_user),
    redis_client: Redis = Depends(get_async_redis_client),
    producer: AIOKafkaProducer = Depends(get_producer),
):
    user = await Orm.scalar(User, session, User.email == credentials.email)
    if not user:
        raise HTTPException(404, "User not found")

    data = {**order.model_dump(), "user_id": user.id}

    order = await crud.create(data, session)

    await save_in_cache(order, redis_client)
    await send_to_producer(producer, [str(order.id)], logger, redis_client)

    return order


@orders_router.patch("/{order_id}/", response_model=OrderResponse)
@order_owner_check
async def update_order(
    order_id: uuid.UUID,
    order_data: OrderUpdate,
    session: AsyncSession = Depends(get_session),
    redis_client: Redis = Depends(get_async_redis_client),
):
    updated_order = await crud.update(
        order_data.model_dump(exclude_unset=True), order_id, session
    )
    await save_in_cache(updated_order, redis_client)

    return updated_order


@orders_router.get("/{order_id}/", response_model=OrderResponse)
@order_owner_check
async def get_order(
    order_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    redis_client: Redis = Depends(get_async_redis_client),
):
    order = await redis_client.get(f"order__{order_id}")
    if order:
        return json.loads(order)

    return await crud.retrieve(order_id, session)


@orders_router.get("/user", response_model=list[OrderResponse])
async def get_user_orders(
    session: AsyncSession = Depends(get_session),
    credentials: AUTH_MODEL = Depends(auth.get_request_user),
):
    user = await Orm.scalar(User, session, User.email == credentials.email)

    return await crud.list(session, filters=[Order.user_id == user.id])
