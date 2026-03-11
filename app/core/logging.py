"""Logging helpers that attach request trace identifiers to log records."""

import contextvars
import logging


_trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "trace_id", default="-"
)


class TraceIdFilter(logging.Filter):
    """Inject the active trace id into log records before formatting."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Populate the `trace_id` attribute expected by the log formatter.

        Args:
            record (logging.LogRecord): Log record being processed by the handler.

        Returns:
            bool: Always `True` so the record continues through the logging pipeline.
        """
        record.trace_id = _trace_id_var.get("-")
        return True


def set_trace_id(value: str | None) -> None:
    """Store the current request trace id in the logging context.

    Args:
        value (str | None): Trace id to persist for subsequent log statements.
    """
    _trace_id_var.set(value or "-")


def configure_logging(debug: bool) -> None:
    """Configure the root logger with the project's formatter and trace filter.

    Args:
        debug (bool): Whether debug-level logging should be enabled.
    """
    level = logging.DEBUG if debug else logging.INFO
    root = logging.getLogger()
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s trace_id=%(trace_id)s %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.addFilter(TraceIdFilter())
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger configured by `configure_logging`.

    Args:
        name (str): Logger name, typically `__name__`.

    Returns:
        logging.Logger: Logger instance for the requested namespace.
    """
    return logging.getLogger(name)
