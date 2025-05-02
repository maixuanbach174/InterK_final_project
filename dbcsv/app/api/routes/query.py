from typing import Annotated

from fastapi import APIRouter , Depends

from dbcsv.app.api.schemas.auth import User
from dbcsv.app.api.schemas.sql_request import SQLRequest
from dbcsv.app.core.database_engine import DatabaseEngine, get_engine
from dbcsv.app.security.auth import current_user_dependency

router = APIRouter(
    prefix='/query',
    tags=['Query']
)

@router.post('/sql')
def query_by_sql(sql_request: SQLRequest, current_user: Annotated[User, current_user_dependency], database_engine: Annotated[DatabaseEngine, Depends(get_engine)]):
    result = database_engine.execute(sql_request.sql_statement, sql_request.schema)
    result2 = []
    for row in result:
        result2.append(row)
    return result2