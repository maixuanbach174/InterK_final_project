from typing import List, Any, Iterator, Callable, Dict

from lark import Lark

from dbcsv.app.core.storage_layer.metadata import Metadata
from dbcsv.app.core.storage_layer.utils import sql_to_logical_plan

class QueryExecutor:
    """Executes SQL queries"""
    
    @staticmethod
    def execute_sql(sql: str, metadata: Metadata,  parser: Lark) -> Iterator[List[Any]]:
        """Parse, optimize, and execute a SQL query"""
        try:
            # Parse SQL to logical plan
            parsed_tree = parser.parse(sql)
            if parsed_tree is None or len(parsed_tree.children) == 0:
                raise ValueError("Parsed query is None")
            else:
                parsed_query = parsed_tree.children[0]
            print("Parsed query:", parsed_query)    
            logical_plan = sql_to_logical_plan(parsed_query, metadata)
            print("Logical Plan created.")
            print("Type of logical plan:", type(logical_plan))
            print("Logical Plan:", logical_plan)

            # Execute the plan
            result = logical_plan.execute()
            print("Type of result:", type(result))
            return result
        except Exception as e:
            print(f"Error executing query: {e}")
            raise