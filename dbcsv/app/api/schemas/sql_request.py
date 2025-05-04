from pydantic import BaseModel, Field


class SQLRequest(BaseModel):
    sql: str = Field(
        description="User's request is a sql statement."
    )
    db: str | None = Field(max_length=255, description="Database name.", default=None)
