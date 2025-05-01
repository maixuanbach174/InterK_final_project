from typing import Annotated

from fastapi import APIRouter, Form

from dbcsv.app.api.schemas.auth import Token, User
from dbcsv.app.dependencies import current_user_dependency
from dbcsv.app.security.auth import auth_manager

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/connect")
async def connection(
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
) -> Token:
    return auth_manager.login_for_access_token(username, password)


@router.post("/refresh")
async def refresh_token(
    current_user: Annotated[User, current_user_dependency],
) -> Token:
    return auth_manager.refresh_for_access_token(current_user)
