import logging
import inspect
from contextvars import ContextVar

execution_context: ContextVar[str] = ContextVar("execution_context", default="Global")


class TraceableFormatter(logging.Formatter):
    def format(self, record):
        record.context = execution_context.get()
        return super().format(record)


def get_logger():
    logger = logging.getLogger("SparkFramework")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = TraceableFormatter('%(asctime)s | %(levelname)-8s | %(context)s | %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def trace_execution(func):
    import functools
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        module = inspect.getmodule(func).__name__ if inspect.getmodule(func) else "Unknown"
        method = func.__name__
        token = execution_context.set(f"[{module}:{method}]")
        try:
            return func(*args, **kwargs)
        finally:
            execution_context.reset(token)

    return wrapper
