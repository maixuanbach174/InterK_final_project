from pydantic import BaseModel, Field


class SQLRequest(BaseModel):
    sql_statement: str = Field(
        max_length=255, description="User's request is a sql statement."
    )
    schema: str | None = Field(max_length=255, description="Schema name.", default=None)
