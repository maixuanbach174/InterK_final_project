import sys
from pathlib import Path
import os
from typing import Any, Iterator, List

from fastapi import HTTPException
from lark import Lark

from dbcsv.app.core.parser.parser import SQLTransformer, grammar
from dbcsv.app.core.storage_layer.metadata import Metadata
from dbcsv.app.core.storage_layer.query_executor import QueryExecutor


class DatabaseEngine():
    def __init__(self):
        self.__schemas : list[str] = self.__loadSchemas()
        self.__metadatas : dict[str, Metadata]= self.__loadMetadatas()  # Initialize the dict
        self.__parser = Lark(grammar, parser='lalr', transformer=SQLTransformer(), start='start')
        self.__executor = QueryExecutor
    
    def __loadMetadatas(self) -> dict[str, Metadata]:
        self.__metadatas = {schema: Metadata(schema) for schema in self.__schemas}
        return self.__metadatas
    
    def __loadSchemas(self) -> list[str]:
        path = Path(__file__).parent.parent.parent / 'data' 
        schemas = [schema_name for schema_name in os.listdir(path) 
                  if os.path.isdir(Path(path) / schema_name)]
        return schemas

    def execute(self, sql_statement: str, schema: str) -> Iterator[List[Any]]:
        if schema not in self.__schemas:
            raise ValueError(f"Schema {schema} not found!")
        try:
            results = self.__executor.execute_sql(sql_statement, self.__metadatas[schema], self.__parser)
        except Exception as e:
            raise e
        return results

db_engine = DatabaseEngine()

def get_engine() -> DatabaseEngine:
    return db_engine