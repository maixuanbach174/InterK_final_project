from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from dbcsv.app.api.routes import auth, query
from dbcsv.app.core.database_engine import DataAccessError, SQLValidationError
from dbcsv.app.security.auth import auth_manager


@asynccontextmanager
async def life_span(app: FastAPI):
    auth_manager.read_accounts_json()
    yield

app = FastAPI(lifespan=life_span)

@app.exception_handler(SQLValidationError)
async def sql_validation_exception_handler(request: Request, exc: SQLValidationError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})

@app.exception_handler(DataAccessError)
async def handle_data_error(request: Request, exc: DataAccessError):
    # 500 Internal Server Error for server-side failures
    return JSONResponse(status_code=500, content={"detail": str(exc)})

app.include_router(router=query.router)
app.include_router(router=auth.router)
