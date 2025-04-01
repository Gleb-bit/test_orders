import logging
import json
from pathlib import Path
from typing import Optional


LOGGER_CONFIG = {
    "api": "api",
    "celery": "celery",
}

LIBS = ["celery", "uvicorn", "fastapi"]


class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }

        extra_fields = {
            key: value
            for key, value in record.__dict__.items()
            if key
            not in {
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
                "component",
            }
        }
        if extra_fields:
            log_record.update(extra_fields)

        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)

        if hasattr(record, "component"):
            log_record["component"] = record.component

        return json.dumps(log_record, default=str, ensure_ascii=False)


def setup_logger(
    component_name: Optional[str] = "api",
    log_file: Optional[str] = None,
    in_file: bool = False,
) -> None:
    logger_name = LOGGER_CONFIG.get(component_name, component_name)
    logger = logging.getLogger(logger_name)

    if in_file:
        if log_file is None:
            log_file = f"logs/{component_name}.log"

        Path(log_file).parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(JsonLogFormatter())

        logger.addHandler(file_handler)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    logger.addHandler(handler)

    logger.setLevel(logging.INFO)

    for lib in LIBS:
        lib_logger = logging.getLogger(lib)
        lib_logger.setLevel(logging.WARNING)
        lib_logger.propagate = False

        lib_logger.handlers = [
            h for h in lib_logger.handlers if not isinstance(h, logging.FileHandler)
        ]

        if in_file:
            if not any(isinstance(h, logging.FileHandler) for h in lib_logger.handlers):
                lib_logger.addHandler(file_handler)

        if not any(isinstance(h, logging.StreamHandler) for h in lib_logger.handlers):
            lib_logger.addHandler(handler)

    if component_name:
        old_factory = logging.getLogRecordFactory()

        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            record.component = component_name
            return record

        logging.setLogRecordFactory(record_factory)
