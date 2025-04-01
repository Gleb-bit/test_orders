from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from apps.auth.conf import auth
from apps.auth.tables import User
from apps.auth.models import UserReadModel, UserModel
from config.database import get_session
from core.sqlalchemy.crud import Crud
from core.sqlalchemy.orm import Orm

auth_router = APIRouter()

crud = Crud(User)


@auth_router.post("/register/", response_model=UserReadModel)
async def register(
    user: UserModel,
    session: AsyncSession = Depends(get_session),
):
    hashed_password = auth.get_password_hash(user.password)
    data = {
        "email": user.email,
        "hashed_password": hashed_password,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "role": user.role,
    }

    return await Orm.create(User, data, session)


@auth_router.post("/token/")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session),
):
    user = await Orm.scalar(User, session, User.email == form_data.username)

    if not user:
        raise auth.get_credentials_exc("Invalid email")

    if not auth.verify_password(form_data.password, user.hashed_password):
        raise auth.get_credentials_exc()

    access_token, refresh_token = auth.get_tokens(
        {"sub": user.email, "role": user.role}
    )

    return {"access_token": access_token, "refresh_token": refresh_token}
