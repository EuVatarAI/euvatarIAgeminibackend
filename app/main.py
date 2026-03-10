import time
import uuid

from fastapi import FastAPI
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.core.logging import get_logger
from app.core.logging import set_trace_id
from app.routes.router import api_router

settings = get_settings()
configure_logging(settings.app_debug)
logger = get_logger(__name__)

app = FastAPI(
    title=settings.app_name,
    debug=settings.app_debug,
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    trace_id = (request.headers.get("X-Request-Id") or "").strip() or str(uuid.uuid4())
    request.state.trace_id = trace_id
    request.state.request_started_at = time.time()
    set_trace_id(trace_id)
    logger.info("[request] start method=%s path=%s", request.method, request.url.path)
    response = None
    try:
        response = await call_next(request)
        return response
    except Exception:
        logger.exception(
            "[request] error method=%s path=%s",
            request.method,
            request.url.path,
        )
        raise
    finally:
        elapsed_ms = int(
            (time.time() - getattr(request.state, "request_started_at", time.time()))
            * 1000
        )
        status_code = response.status_code if response is not None else 500
        logger.info(
            "[request] end method=%s path=%s status=%s duration_ms=%s",
            request.method,
            request.url.path,
            status_code,
            elapsed_ms,
        )
        set_trace_id(None)
        if response is not None:
            response.headers["X-Request-Id"] = trace_id


app.include_router(api_router)
