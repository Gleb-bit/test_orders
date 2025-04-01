from sqlalchemy import Column, Integer, String, Boolean

from config.database import Base


class User(Base):
    """Таблица пользователя"""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)

    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)

    first_name = Column(String, nullable=False)
    last_name = Column(String)

    role = Column(String, nullable=False, default="user")
    is_active = Column(Boolean, default=True)
