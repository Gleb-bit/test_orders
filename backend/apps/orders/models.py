from uuid import UUID
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from typing import Any


class OrderStatus(str, Enum):
    """Перечисление статусов заказа"""

    PENDING = "PENDING"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    CANCELED = "CANCELED"


class OrderCreate(BaseModel):
    """Схема для создания заказа"""

    items: list[dict[str, Any]] = Field(
        ..., example=[{"product_id": 123, "quantity": 2}]
    )
    total_price: float = Field(..., example=499.99)


class OrderUpdate(BaseModel):
    """Схема для обновления статуса заказа"""

    status: OrderStatus = Field(..., example=OrderStatus.PAID)


class OrderResponse(BaseModel):
    """Схема для получения заказа"""

    id: UUID
    user_id: int
    items: list[dict[str, Any]]
    total_price: float
    status: OrderStatus
    created_at: datetime

    class Config:
        from_attributes = True
