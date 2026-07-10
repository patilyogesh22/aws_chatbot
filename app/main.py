import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import DATA_RAW_DIR
from app.auth import router as auth_router
from app.services.startup_service import startup
from app.routes import health
from app.routes import upload
from app.routes import chat
from app.routes import files
from app.routes import history
from app.routes import stats
from app.routes import internal_dbt
from app.middleware.logging import log_requests
from app.routes.athena_test import router as athena_test_router

app = FastAPI(
    title="RAG Chatbot API Authenticated",
    version="3.5.0"
)
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

app.include_router(
    athena_test_router,
    prefix="/api",
)

@app.on_event("startup")
def on_startup():
    startup()


@app.get("/")
def root():
    return {
        "message": "Authenticated RAG + Structured SQL Chatbot API running 🚀",
        "version": "3.5.0"
    }