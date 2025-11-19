# bionicpro_auth.py
import os
import uuid
import time
import sys
import json
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from jose import jwt, JWTError
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import httpx
import redis
from cryptography.fernet import Fernet
from fastapi.middleware.cors import CORSMiddleware
import logging

logging.basicConfig(
    level=logging.DEBUG,  # или INFO
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # важно для Docker!
)

logger = logging.getLogger("auth")
# --------------------
# Конфигурация (env)
# --------------------
KEYCLOAK_URL = os.getenv("AUTH_KEYCLOAK_BASE_URL", "https://keycloak.example.com/auth")
KEYCLOAK_CONTAINER_URL = os.getenv(
    "AUTH_KEYCLOAK_CONTAINER_URL", "http://keycloak:8080"
)
REALM = os.getenv("AUTH_KEYCLOAK_REALM", "myrealm")
CLIENT_ID = os.getenv("AUTH_KEYCLOAK_CLIENT_ID", "bionicpro-auth")
CLIENT_SECRET = os.getenv(
    "AUTH_KEYCLOAK_CLIENT_SECRET", ""
)  # confidential client secret
BIONIC_HOST = os.getenv("BIONIC_HOST", "localhost:3010")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SESSION_COOKIE_NAME = "bionicpro_session"
SESSION_COOKIE_SECURE = False  # True in prod; False only for local http dev
SESSION_COOKIE_SAMESITE = "Lax"  # or "Strict"
SESSION_MAX_AGE = int(
    os.getenv("AUTH_SESSION_LIFETIME_SECONDS", 60 * 30)
)  # seconds, > access token life
ACCESS_TOKEN_LIFESPAN = int(
    os.getenv("AUTH_ACCESS_TOKEN_LIFETIME", 120)
)  # seconds (<= 2 min)

FERNET_KEY = os.getenv("FERNET_KEY") or Fernet.generate_key().decode()
fernet = Fernet(FERNET_KEY.encode())

# --------------------
# Хранилище сесий
# --------------------
use_redis = True
redis_client = None
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
except Exception as e:
    print("Redis not available, falling back to in-memory store:", e)
    use_redis = False

# in-memory stores (fallback)
_in_memory_sessions: Dict[str, Dict[str, Any]] = {}
_in_memory_tokens: Dict[str, Dict[str, Any]] = {}


# --------------------
# Утилиты хранилища
# --------------------
def _store_session(session_id: str, payload: dict, ttl: Optional[int] = None):
    if use_redis:
        redis_client.set(f"session:{session_id}", json.dumps(payload))
        if ttl:
            redis_client.expire(f"session:{session_id}", ttl)
    else:
        _in_memory_sessions[session_id] = payload


def _get_session(session_id: str) -> Optional[dict]:
    if use_redis:
        v = redis_client.get(f"session:{session_id}")
        return json.loads(v) if v else None
    else:
        return _in_memory_sessions.get(session_id)


def _delete_session(session_id: str):
    if use_redis:
        redis_client.delete(f"session:{session_id}")
    else:
        _in_memory_sessions.pop(session_id, None)


def _store_tokens(
    session_id: str,
    access_token: dict,
    refresh_token_enc: str,
    ttl: Optional[int] = None,
):
    payload = {
        "access_token": access_token,
        "refresh_token_enc": refresh_token_enc,
        "updated_at": int(time.time()),
    }
    if use_redis:
        redis_client.set(f"tokens:{session_id}", json.dumps(payload))
        if ttl:
            redis_client.expire(f"tokens:{session_id}", ttl)
    else:
        _in_memory_tokens[session_id] = payload


def _get_tokens(session_id: str) -> Optional[dict]:
    if use_redis:
        v = redis_client.get(f"tokens:{session_id}")
        return json.loads(v) if v else None
    else:
        return _in_memory_tokens.get(session_id)


def _delete_tokens(session_id: str):
    if use_redis:
        redis_client.delete(f"tokens:{session_id}")
    else:
        _in_memory_tokens.pop(session_id, None)


async def load_userinfo(access_token: str):
    async with httpx.AsyncClient() as client:
        logger.info(f"Token: {access_token}")

        resp = await client.get(
            USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10.0,
        )
    if resp.status_code != 200:
        return None
    return resp.json()


# --------------------
# Keycloak endpoints
# --------------------
AUTH_URL = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/auth"
TOKEN_URL = f"http://keycloak:8080/realms/{REALM}/protocol/openid-connect/token"
USERINFO_URL = (
    f"{KEYCLOAK_CONTAINER_URL}/realms/{REALM}/protocol/openid-connect/userinfo"
)

# --------------------
# FastAPI app
# --------------------
app = FastAPI()

origins = [
    "http://localhost:3000",  # фронтенд
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------
# Helper: build auth redirect
# --------------------
def build_auth_redirect(state: str, redirect_uri: str):
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": "openid profile email",
        "redirect_uri": redirect_uri,
        "state": state,
    }
    import urllib.parse

    url = AUTH_URL + "?" + urllib.parse.urlencode(params)
    return url


# --------------------
# Login: redirect to Keycloak
# --------------------
@app.get("/login")
async def login():
    state = str(uuid.uuid4())
    redirect_uri = f"http://{BIONIC_HOST}/callback"
    url = build_auth_redirect(state, redirect_uri)
    # store state somewhere if needed (for CSRF) — here we skip storage for brevity
    return RedirectResponse(url)


# --------------------
# Callback: exchange code -> tokens; create session; set cookie
# --------------------
@app.get("/callback")
async def callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code:
        raise HTTPException(400, "Missing code")

    redirect_uri = f"http://{BIONIC_HOST}/callback"
    async with httpx.AsyncClient() as client:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        resp = await client.post(TOKEN_URL, data=data, headers=headers, timeout=30.0)
        if resp.status_code != 200:
            raise HTTPException(502, f"Token exchange failed: {resp.text}")
        token_resp = resp.json()

    # token_resp contains access_token, refresh_token, expires_in, refresh_expires_in, etc.
    # create session:
    session_id = str(uuid.uuid4())

    # store access token metadata (we do not store plaintext refresh token)
    access_token = {
        "token": token_resp["access_token"],
        "expires_at": int(time.time())
        + int(token_resp.get("expires_in", ACCESS_TOKEN_LIFESPAN)),
    }

    # encrypt refresh token
    refresh_token_plain = token_resp["refresh_token"]
    refresh_token_enc = fernet.encrypt(refresh_token_plain.encode()).decode()

    # store session metadata
    now = int(time.time())
    session_payload = {
        "session_id": session_id,
        "created_at": now,
        "last_used": now,
        "user": None,  # can fill by calling userinfo if needed
    }
    _store_session(session_id, session_payload, ttl=SESSION_MAX_AGE)
    _store_tokens(session_id, access_token, refresh_token_enc, ttl=SESSION_MAX_AGE)

    # set cookie
    response = RedirectResponse(url=f"http://localhost:3000/")  # redirect SPA root
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
        max_age=SESSION_MAX_AGE,
    )
    return response


# --------------------
# Utility: refresh access token using refresh_token
# --------------------
async def refresh_access_token(session_id: str) -> bool:
    tokens = _get_tokens(session_id)
    if not tokens:
        return False
    refresh_token_enc = tokens.get("refresh_token_enc")
    try:
        refresh_token = fernet.decrypt(refresh_token_enc.encode()).decode()
    except Exception:
        return False

    async with httpx.AsyncClient() as client:
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        resp = await client.post(TOKEN_URL, data=data, headers=headers, timeout=30.0)
        if resp.status_code != 200:
            # refresh failed (token expired etc.)
            return False
        new_tokens = resp.json()

    # update stored tokens (encrypt new refresh token)
    new_access = {
        "token": new_tokens["access_token"],
        "expires_at": int(time.time())
        + int(new_tokens.get("expires_in", ACCESS_TOKEN_LIFESPAN)),
    }
    new_refresh_enc = fernet.encrypt(new_tokens["refresh_token"].encode()).decode()
    _store_tokens(session_id, new_access, new_refresh_enc, ttl=SESSION_MAX_AGE)
    return True


# --------------------
# Middleware: attach session to request.state (simple)
# --------------------
class SessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        request.state.session = None
        if session_id:
            s = _get_session(session_id)
            t = _get_tokens(session_id)
            if s and t:
                # update last_used
                s["last_used"] = int(time.time())
                _store_session(session_id, s, ttl=SESSION_MAX_AGE)
                request.state.session = {"id": session_id, "meta": s, "tokens": t}
        response = await call_next(request)
        return response


app.add_middleware(SessionMiddleware)


# --------------------
# Helper: ensure authenticated, refresh if needed, rotate session
# --------------------
async def require_session(request: Request):
    sess = request.state.session
    if not sess:
        raise HTTPException(401, "Not authenticated")

    session_id = sess["id"]
    tokens = _get_tokens(session_id)
    if not tokens:
        raise HTTPException(401, "No tokens stored")

    access = tokens.get("access_token")
    expires_at = access.get("expires_at", 0)
    now = int(time.time())
    if now >= expires_at - 5:  # expired or about to expire (5s early)
        ok = await refresh_access_token(session_id)
        if not ok:
            # refresh failed — invalidate session
            _delete_tokens(session_id)
            _delete_session(session_id)
            raise HTTPException(401, "Session expired and refresh failed")
        tokens = _get_tokens(session_id)  # updated

    # session rotation: generate new session id and move tokens
    new_session_id = str(uuid.uuid4())
    # move session meta and tokens
    meta = _get_session(session_id)
    tokens = _get_tokens(session_id)
    if not meta or not tokens:
        raise HTTPException(401, "Session missing")
    meta["rotated_from"] = session_id
    meta["last_used"] = now
    _store_session(new_session_id, meta, ttl=SESSION_MAX_AGE)
    _store_tokens(
        new_session_id,
        tokens["access_token"],
        tokens["refresh_token_enc"],
        ttl=SESSION_MAX_AGE,
    )
    # delete old
    _delete_session(session_id)
    _delete_tokens(session_id)

    # set cookie in response via returning this info
    return {"new_session_id": new_session_id, "meta": meta, "tokens": tokens}


@app.get("/api/protected")
async def protected(request: Request):
    # 1. Проверяем сессию
    try:
        result = await require_session(request)
    except HTTPException as e:
        raise e

    new_session_id = result["new_session_id"]
    access_token = result["tokens"]["access_token"]["token"]

    # 2. Читаем payload без проверки подписи
    try:
        payload = jwt.get_unverified_claims(access_token)
    except Exception:
        raise HTTPException(401, "Cannot decode access_token")

    # 3. Определяем user_id (email предпочтительнее)
    user_id = (
        payload.get("email") or payload.get("preferred_username") or payload.get("sub")
    )

    if not user_id:
        raise HTTPException(500, "Cannot extract user identifier from token")

    # 4. Возвращаем данные
    return JSONResponse(
        {
            "ok": True,
            "session_id": new_session_id,
            "user_id": user_id,
            "user": payload,
        }
    )


# --------------------
# Logout
# --------------------
@app.post("/logout")
async def logout(request: Request):
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if session_id:
        _delete_tokens(session_id)
        _delete_session(session_id)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE_NAME)
    return resp


@app.get("/session")
async def check_session(request: Request):
    session_id = request.cookies.get("bionicpro_session")
    if not session_id:
        raise HTTPException(status_code=401, detail="No session")

    tokens = _get_tokens(session_id)
    if not tokens:
        raise HTTPException(status_code=401, detail="Session expired")

    access = tokens["access_token"]
    if int(time.time()) >= access["expires_at"]:
        # пытаемся обновить через refresh
        if not await refresh_access_token(session_id):
            raise HTTPException(status_code=401, detail="Session expired")

    return {"ok": True, "user": "example@example.com"}
