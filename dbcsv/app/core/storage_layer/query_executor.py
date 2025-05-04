from typing import List, Any, Iterator

from dbcsv.app.core.storage_layer.metadata import Metadata
from dbcsv.app.core.storage_layer.utils import sql_to_logical_plan

class QueryExecutor:
    """Executes SQL queries""" 
    def __init__(self, metadatas: dict[str, Metadata]):
        self.__metadatas = metadatas
    def execute_parsed_sql(self, sql_dict: dict[str, Any]) -> Iterator[List[Any]]:
        metadata = self.__metadatas[sql_dict['db']]
        try:  
            logical_plan = sql_to_logical_plan(sql_dict, metadata)
            result = logical_plan.execute()
            return result
        except Exception as e:
            raise Exception(f"Failed to execute query: {e}")