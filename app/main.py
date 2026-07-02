import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import DATA_RAW_DIR
from app.auth import router as auth_router
from app.services.startup_service import startup
from aws_xray_sdk.core import xray_recorder, patch_all
from app.routes import health
from app.routes import upload
from app.routes import chat
from app.routes import files
from app.routes import history
from app.routes import stats
from app.routes import internal_dbt
from app.middleware.logging import log_requests

app = FastAPI(
    title="RAG Chatbot API Authenticated",
    version="3.5.0"
)
xray_recorder.configure(service="docchat-api")
patch_all(psycopg2=False)

@app.middleware("http")
async def xray_trace_requests(request, call_next):
    segment = xray_recorder.begin_segment("docchat-api")

    segment.put_annotation("path", request.url.path)
    segment.put_annotation("method", request.method)

    try:
        response = await call_next(request)
        segment.put_annotation("status_code", response.status_code)
        return response

    except Exception as e:
        segment.add_exception(e, stack=True)
        raise

    finally:
        xray_recorder.end_segment()
app.middleware("http")(log_requests)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(DATA_RAW_DIR, exist_ok=True)

app.include_router(auth_router)
app.include_router(health.router)
app.include_router(upload.router)
app.include_router(chat.router)
app.include_router(files.router)
app.include_router(history.router)
app.include_router(stats.router)
app.include_router(internal_dbt.router)


@app.on_event("startup")
def on_startup():
    startup()


@app.get("/")
def root():
    return {
        "message": "Authenticated RAG + Structured SQL Chatbot API running 🚀",
        "version": "3.5.0"
    }