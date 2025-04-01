import enum
import uuid
from datetime import datetime

from sqlalchemy import Column, UUID, Integer, ForeignKey, JSON, Float, DateTime, Enum

from config.database import Base


class OrderStatus(enum.Enum):
    PENDING = "PENDING"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    CANCELED = "CANCELED"


class Order(Base):
    """Таблица заказа"""

    __tablename__ = "orders"

    id = Column(UUID, primary_key=True, index=True, default=uuid.uuid4)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    items = Column(JSON, nullable=False)
    total_price = Column(Float, nullable=False)
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.PENDING.value)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
