from pathlib import Path
import os
from typing import Any, Iterator, List

from sqlglot import ParseError

from dbcsv.app.core.sql_validator import SQLValidator
from dbcsv.app.core.storage_layer.metadata import Metadata
from dbcsv.app.core.storage_layer.query_executor import QueryExecutor

class SQLValidationError(Exception):
    """Raised when SQL parsing or validation fails."""

class DataAccessError(Exception):
    """Raised when reading CSV or metadata fails (server fault)."""

class DatabaseEngine():
    def __init__(self):
        self.__dbs : list[str] = self.__loadSchemas()
        self.__metadatas : dict[str, Metadata]= self.__loadMetadatas()  # Initialize the dict
        self.__validator = SQLValidator(self.__metadatas)
        self.__executor = QueryExecutor(self.__metadatas)
    
    def __loadMetadatas(self) -> dict[str, Metadata]:
        self.__metadatas = {schema: Metadata(schema) for schema in self.__dbs}
        return self.__metadatas
    
    def __loadSchemas(self) -> list[str]:
        path = Path(__file__).parent.parent.parent / 'data' 
        dbs = [db_name for db_name in os.listdir(path) 
                  if os.path.isdir(Path(path) / db_name)]
        return dbs

    def execute(self, sql_statement: str, db: str) -> Iterator[List[Any]]:
        if db not in self.__dbs:
            raise SQLValidationError(f"Database not found: {db}")
        try:
            tree = self.__validator.parse(sql_statement)
            parsed = self.__validator.validate(tree, db)
        except (ValueError, SyntaxError, ParseError, TypeError) as e:
            raise SQLValidationError(str(e))
        try:
            return self.__executor.execute_parsed_sql(parsed)
        except (FileNotFoundError, Exception) as e:
            raise DataAccessError(f"Data access error: {e}") from e

db_engine = DatabaseEngine()

def get_engine() -> DatabaseEngine:
    return db_engine