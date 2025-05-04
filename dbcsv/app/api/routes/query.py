import datetime
import decimal
import json
from typing import Annotated

from fastapi import APIRouter , Depends, HTTPException
from fastapi.responses import StreamingResponse

from dbcsv.app.api.schemas.auth import User
from dbcsv.app.api.schemas.sql_request import SQLRequest
from dbcsv.app.core.database_engine import DataAccessError, DatabaseEngine, SQLValidationError, get_engine
from dbcsv.app.security.auth import current_user_dependency

router = APIRouter(
    prefix='/query',
    tags=['Query']
)

def _serialize_value(v):
    if isinstance(v, datetime.date):
        # ISO 8601 date
        return v.isoformat()
    if isinstance(v, datetime.datetime):
        # ISO 8601 datetime with timezone if present
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        # convert Decimal → float (or str, if you prefer exact)
        return float(v)
    # leave int, float, bool, str, None, lists, dicts alone
    return v

@router.post('/sql')
def stream_query_by_sql(
    sql_request: SQLRequest,
    current_user: Annotated[User, current_user_dependency],
    database_engine: Annotated[DatabaseEngine, Depends(get_engine)],
):
    # 1) parse & validate before starting the stream
    try:
        iterator = database_engine.execute(sql_request.sql, sql_request.db)
    except SQLValidationError as e:
        # bad SQL or bad schema → 400
        raise HTTPException(status_code=400, detail=str(e))
    except DataAccessError as e:
        # server/data problem → 500
        raise HTTPException(status_code=500, detail=str(e))

    def batch_generator(batch_size: int = 1024):
        batch = []
        try:
            for row in iterator:
                batch.append([_serialize_value(v) for v in row])
                if len(batch) >= batch_size:
                    yield json.dumps(batch, default=str) + "\n"
                    batch = []
            if batch:
                yield json.dumps(batch, default=str) + "\n"
        except DataAccessError as e:
            # data failure mid‑stream → abort with HTTP 500
            # Raising HTTPException inside a generator will cause FastAPI to terminate the stream
            raise HTTPException(status_code=500, detail=str(e))

    return StreamingResponse(
        batch_generator(),
        media_type="application/x-ndjson",
    )