from datetime import timedelta, datetime

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jwt import InvalidTokenError
from passlib.context import CryptContext

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login/")


class AuthEmail:
    def __init__(
        self,
        secret_key: str,
        user_model,
        algorithm: str = "HS256",
        access_token_expire_hours: int = 24,
        refresh_token_expire_days: int = 7,
    ):
        self.secret_key = secret_key
        self.algorithm = algorithm

        self.access_token_expire = timedelta(hours=access_token_expire_hours)
        self.refresh_token_expire = timedelta(days=refresh_token_expire_days)

        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        self.user_model = user_model

    def create_jwt_token(self, data: dict, expires_delta: timedelta):
        to_encode = data.copy()
        expire = datetime.now() + expires_delta

        to_encode.update({"exp": expire})

        return jwt.encode(to_encode, self.secret_key, self.algorithm)

    def get_request_user(self, token: str = Depends(oauth2_scheme)):
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            email: str = payload.get("sub")

            if email is None:
                raise self.get_credentials_exc("Invalid email")

            token_data = self.user_model(email=email)

        except InvalidTokenError:
            raise self.get_credentials_exc()

        return token_data

    def get_request_user_with_roles(self, required_roles: list):
        user_model = self.user_model

        def role_checker(current_user: user_model = Depends(self.get_request_user)):
            if current_user.role not in required_roles:
                raise self.get_permissions_exc()

            return current_user

        return role_checker

    def get_tokens(self, jwt_data):
        access_token = self.create_jwt_token(jwt_data, self.access_token_expire)
        refresh_token = self.create_jwt_token(jwt_data, self.refresh_token_expire)

        return access_token, refresh_token

    def get_password_hash(self, password: str):
        return self.pwd_context.hash(password)

    @staticmethod
    def get_credentials_exc(detail="Invalid password", status_code=401):
        return HTTPException(status_code, detail, {"WWW-Authenticate": "Bearer"})

    @staticmethod
    def get_permissions_exc(
        detail="You do not have the required permissions", status_code=403
    ):
        return HTTPException(status_code, detail, {"WWW-Authenticate": "Bearer"})

    def verify_password(self, plain_password, hashed_password):
        return self.pwd_context.verify(plain_password, hashed_password)
