import re
from urllib.parse import urlparse
import requests
from requests.exceptions import HTTPError
from typing import Union
import jwt
import time
import os

from dbapi2.exception import (
    InterfaceError,
    OperationalError,
    AuthenticationError,
)

ACCESS_TOKEN_EXPIRE_MINUTES=30
ACCESS_TOKEN_DELTA_SECONDS = 60

# Regex to check if schema is snake case
_SCHEMA_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def validate_dsn_url(dsn: str) -> str:
    """
    Validate if dsn comply to format https://localhost:PORT/<schema_snake_case>.
    Returns schema_snake_case
    Throw InterfaceError if fails
    """
    parsed = urlparse(dsn)

    # if parsed.scheme not in ["http", "https"]:
    #     raise InterfaceError("DSN must be http or https")

    if parsed.port is not None and not (1 <= parsed.port <= 65535):
        raise InterfaceError("port number is not valid (not in range 1‑65535)")

    if (
        parsed.params
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise InterfaceError("DSN must not contain query, fragment, user/password…")

    parts = dsn.rsplit("/", 1)
    url, db = parts[0], parts[1]

    return db, url


def login(url: str, username: str, password: str) -> str:
    data = {
        "username": username,
        "password": password,
    }

    try:
        r = requests.post(f"{url}/auth/connect", data=data, timeout=5)
        r.raise_for_status()  # 4xx/5xx → HTTPError
        return r.json()["access_token"]
    except HTTPError as exc:
        err_json = exc.response.json()  # FastAPI return JSON: {"detail": "..."}
        detail = err_json.get("detail", exc.response.text)
        raise OperationalError(detail)
    except requests.RequestException as exc:
        # Internet error/timeout/etc.
        raise exc


def validate_token(url: str, token: str) -> Union[str, None]:
    # Load ACCESS_TOKEN_DELTA_SECONDS from .env
    refresh_threshold = int(os.getenv("ACCESS_TOKEN_DELTA_SECONDS") or ACCESS_TOKEN_DELTA_SECONDS)

    # Decode token without verification to check expiration
    payload = jwt.decode(token, options={"verify_signature": False})
    current_time = time.time()
    expiration_time = payload["exp"]

    # Calculate remaining time
    remaining_time = expiration_time - current_time

    # If token is still valid AND not near expiration -> return None
    if remaining_time > refresh_threshold:
        print("Token still valid and not near expiration. Proceeds to query...")
        return None

    # Token expired -> call to endpoint /refresh
    try:
        print(
            f"Token will expire in {remaining_time:.0f} seconds. Refreshing a new one..."
        )
        header = {"Authorization": f"Bearer {token}"}
        r = requests.post(f"{url}/auth/refresh", headers=header, timeout=5)
        r.raise_for_status()
        return r.json()["access_token"]
    except HTTPError as exc:
        err_json = exc.response.json()  # FastAPI return JSON: {"detail": "..."}
        detail = err_json.get("detail", exc.response.text)
        raise AuthenticationError(detail)
    except requests.RequestException as exc:
        # Internet error/timeout/etc.
        raise exc


def query(url: str, token: str, db: str, query: str) -> dict:
    header = {
        "Authorization": f"Bearer {token}"
    }  # this token's exp date is never invalid since it is refreshed in the previous step (validate_token)
    data = {"sql": query, "db": db}
    try:
        r = requests.post(f"{url}/query/sql/", json=data, headers=header, timeout=5)
        r.raise_for_status()
        return r.json()
    except HTTPError as exc:
        err_json = exc.response.json()  # FastAPI return JSON: {"detail": "..."}
        detail = err_json.get("detail", exc.response.text)
        raise OperationalError(detail)
    except requests.RequestException as exc:
        # Internet error/timeout/etc.
        raise exc
