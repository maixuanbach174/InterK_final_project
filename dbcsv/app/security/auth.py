import json
import os
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Dict

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from dbcsv.app.api.schemas.auth import Token, TokenData, User, UserInDB


class AuthManager:
    __accounts_json: Dict[str, Any]
    __secret_key: str
    __algorithm: str
    __access_token_expire_minutes: int

    def __init__(
        self, secret_key: str, algorithm: str, access_token_expire_minutes: int
    ) -> None:
        self.__secret_key = secret_key
        self.__algorithm = algorithm
        self.__access_token_expire_minutes = access_token_expire_minutes

    @property
    def accounts_json(self) -> Dict[str, Any]:
        return self.__accounts_json

    @property
    def secret_key(self) -> str:
        return self.__secret_key

    @property
    def algorithm(self) -> str:
        return self.__algorithm

    @property
    def access_token_expire_minutes(self) -> int:
        return self.__access_token_expire_minutes

    def login_for_access_token(
        self,
        username: str,
        password: str,
    ) -> Token:
        user = self.authenticate_user(username.strip(), password.strip())
        access_token = self.create_access_token(
            data={"sub": user.username},
            expires_delta=timedelta(minutes=self.__access_token_expire_minutes),
        )
        return Token(access_token=access_token)

    def refresh_for_access_token(self, user: User) -> Token:
        new_access_token = self.create_access_token(
            data={"sub": user.username},
            expires_delta=timedelta(minutes=self.__access_token_expire_minutes),
        )
        return Token(access_token=new_access_token)

    def authenticate_user(self, username: str, password: str) -> UserInDB:
        user = self.get_user(username)
        if not user or (password != user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return user

    def create_access_token(self, data: dict, expires_delta: timedelta) -> str:
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + expires_delta
        to_encode.update({"exp": expire})
        return jwt.encode(to_encode, self.__secret_key, algorithm=self.__algorithm)

    def get_user(self, username: str) -> UserInDB | None:
        user_dict = self.__accounts_json.get(username)
        if user_dict:
            return UserInDB(**user_dict)
        return None

    def get_current_user(
        self,
        token: Annotated[str, Depends(OAuth2PasswordBearer(tokenUrl="token"))],
    ) -> User:
        try:
            payload: Dict[str, Any] = jwt.decode(
                token, self.__secret_key, algorithms=[self.__algorithm]
            )
            username: str = payload.get("sub")
            token_data = TokenData(username=username)
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=403, detail="Token verification failed")
        user = self.get_user(token_data.username)
        return user

    def read_accounts_json(self) -> None:
        with open("dbcsv/data/accounts.json", "r") as file:
            self.__accounts_json = json.load(file)


auth_manager = AuthManager(
    os.getenv("SECRET_KEY"),
    os.getenv("ALGORITHM"),
    int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES")),
)
