from typing import Optional

from pydantic import BaseModel, EmailStr


class UserModel(BaseModel):
    email: EmailStr
    password: str
    first_name: str
    last_name: str
    role: str = "user"


class UserReadModel(BaseModel):
    id: Optional[int] = None
    email: Optional[EmailStr]
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: Optional[str]

    class Config:
        from_attributes = True


class TokenModel(BaseModel):
    email: EmailStr
    role: Optional[str] = None
