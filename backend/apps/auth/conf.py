from apps.auth.models import TokenModel
from config.settings import SECRET_KEY
from core.fastapi.auth import AuthEmail

AUTH_MODEL = TokenModel
auth = AuthEmail(SECRET_KEY, AUTH_MODEL)
