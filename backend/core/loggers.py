import traceback
from datetime import datetime

import pytz


timezone = pytz.timezone("Europe/Moscow")


def log_params(*args, separator):
    stack = traceback.extract_stack()[:-2]
    caller_frame = stack[-1]
    caller_filename = caller_frame.filename
    caller_lineno = caller_frame.lineno

    _now = datetime.now(timezone).strftime("%H:%M %d.%m.%y")

    args_str = separator.join(str(arg) for arg in args)
    location_str = f"[{_now}:{caller_filename}:{caller_lineno}]"

    print(f"{args_str} {location_str}")


def printl(*args, separator=", "):
    log_params(*args, separator=separator)
