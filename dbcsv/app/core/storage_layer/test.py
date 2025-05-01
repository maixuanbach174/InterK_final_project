
from typing import Any, Callable, List
from lark import Lark
from dbcsv.app.core.parser.parser import SQLTransformer, grammar
from dbcsv.app.core.storage_layer.metadata import Metadata
from dbcsv.app.core.storage_layer.query_executor import QueryExecutor

if __name__ == "__main__":
    sql = "SELECT * FROM people WHERE age > 30"
    parser = Lark(grammar, parser='lalr', transformer=SQLTransformer(), start='start')
    
    schema = "schema1"
    table="table1"
    metadata = Metadata(schema)
    
    results = QueryExecutor.execute_sql(sql, metadata, parser)
    cnt = 0
    for row in results:
        print("ROW: ", row)
        cnt += 1
        if cnt > 10:
            break

        