from typing import Annotated

from fastapi import APIRouter, Depends, Form, HTTPException

from dbcsv.app.api.schemas.auth import Token, User
from dbcsv.app.core.database_engine import DatabaseEngine, get_engine
from dbcsv.app.security.auth import current_user_dependency, auth_manager

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/connect")
async def connection(
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    db: Annotated[str, Form()],
    database_engine: DatabaseEngine = Depends(get_engine)
) -> Token:
    if db not in database_engine.__dbs:
        raise HTTPException(400, f"Schema not found: {db}")
    return auth_manager.login_for_access_token(username, password)


@router.post("/refresh")
async def refresh_token(
    current_user: Annotated[User, current_user_dependency],
) -> Token:
    return auth_manager.refresh_for_access_token(current_user)
