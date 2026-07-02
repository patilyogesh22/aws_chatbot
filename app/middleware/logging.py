import time
import json
import logging

logger = logging.getLogger("docchat")
logger.setLevel(logging.INFO)


async def log_requests(request, call_next):
    start = time.time()

    response = await call_next(request)

    duration_ms = round((time.time() - start) * 1000, 2)

    log_data = {
        "path": request.url.path,
        "method": request.method,
        "status": response.status_code,
        "duration_ms": duration_ms,
    }

    print(json.dumps(log_data))
    logger.info(json.dumps(log_data))

    return response