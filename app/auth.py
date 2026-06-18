import os
from datetime import datetime, timedelta

import psycopg2
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr

from app.config import PG_DSN


router = APIRouter(prefix="/auth", tags=["auth"])

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-this-secret-key")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


class RegisterRequest(BaseModel):
    name: str
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


def get_conn():
    return psycopg2.connect(PG_DSN)


def init_auth_tables():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS app_documents (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
                    file_name TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    s3_key TEXT,
                    file_size BIGINT DEFAULT 0,
                    uploaded_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(user_id, file_hash)
                );

                CREATE TABLE IF NOT EXISTS chat_history (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
                    question TEXT NOT NULL,
                    answer TEXT NOT NULL,
                    file_name TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
        conn.commit()


def hash_password(password: str):
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(
            status_code=400,
            detail="Password is too long. Please use a password under 72 bytes."
        )
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str):
    if len(password.encode("utf-8")) > 72:
        return False
    return pwd_context.verify(password, password_hash)


def create_token(user_id: int, email: str):
    expire = datetime.utcnow() + timedelta(minutes=JWT_EXPIRE_MINUTES)
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": expire,
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def authenticate_user(email: str, password: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, email, password_hash
                FROM app_users
                WHERE email = %s
            """, (email,))
            row = cur.fetchone()

    if not row or not verify_password(password, row[3]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return {
        "id": row[0],
        "name": row[1],
        "email": row[2],
    }


@router.post("/register")
def register(req: RegisterRequest):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM app_users WHERE email = %s", (req.email,))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="Email already registered")

            cur.execute("""
                INSERT INTO app_users (name, email, password_hash)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (
                req.name,
                req.email,
                hash_password(req.password),
            ))

            user_id = cur.fetchone()[0]
        conn.commit()

    return {
        "access_token": create_token(user_id, req.email),
        "token_type": "bearer",
        "user": {
            "id": user_id,
            "name": req.name,
            "email": req.email,
        },
    }


# Swagger Authorize button uses this endpoint.
# Enter email in the "username" field.
@router.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(
        email=form_data.username,
        password=form_data.password,
    )

    return {
        "access_token": create_token(user["id"], user["email"]),
        "token_type": "bearer",
        "user": user,
    }


# Streamlit/frontend JSON login should use this endpoint.
@router.post("/login-json")
def login_json(req: LoginRequest):
    user = authenticate_user(
        email=req.email,
        password=req.password,
    )

    return {
        "access_token": create_token(user["id"], user["email"]),
        "token_type": "bearer",
        "user": user,
    }


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM],
        )
        user_id = int(payload["sub"])

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, email
                FROM app_users
                WHERE id = %s
            """, (user_id,))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=401, detail="User not found")

    return {
        "id": row[0],
        "name": row[1],
        "email": row[2],
    }