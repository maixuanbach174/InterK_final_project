from contextlib import asynccontextmanager

from fastapi import FastAPI

from dbcsv.app.api.routes import auth, query, websocket
from dbcsv.app.security.auth import auth_manager


@asynccontextmanager
async def life_span(app: FastAPI):
    auth_manager.read_accounts_json()
    yield


app = FastAPI(lifespan=life_span)


@app.get("/")
def root():
    return "This is for database engine!"


app.include_router(router=query.router)
app.include_router(router=auth.router)
app.include_router(router=websocket.router)
