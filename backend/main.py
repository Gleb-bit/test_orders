from fastapi import FastAPI
from sqlalchemy.exc import IntegrityError, DBAPIError

from apps.auth.views import auth_router
from apps.orders.views import orders_router
from config.exc_handlers.base import (
    value_error_handler,
    related_errors_handler,
    input_error_handler,
)
from config.logger import setup_logger


setup_logger("api")

app = FastAPI(title="Test orders app")

exc_handlers = {
    DBAPIError: input_error_handler,
    IntegrityError: related_errors_handler,
    ValueError: value_error_handler,
}
routers = {"/auth": auth_router, "/orders": orders_router}

for exception, handler in exc_handlers.items():
    app.add_exception_handler(exception, handler)

for prefix, router in routers.items():
    app.include_router(router, prefix=prefix)
