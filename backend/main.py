from fastapi import FastAPI
from sqlalchemy.exc import IntegrityError, DBAPIError

from config.exc_handlers.base import (
    value_error_handler,
    related_errors_handler,
    input_error_handler,
)

app = FastAPI(title="Test orders app")

exc_handlers = {
    DBAPIError: input_error_handler,
    IntegrityError: related_errors_handler,
    ValueError: value_error_handler,
}
routers = {}

for exception, handler in exc_handlers.items():
    app.add_exception_handler(exception, handler)

for prefix, router in routers.items():
    app.include_router(router, prefix=prefix)
