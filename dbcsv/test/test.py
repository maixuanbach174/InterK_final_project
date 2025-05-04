from dbcsv.app.core.sql_validator import SQLValidator
from dbcsv.app.core.storage_layer.metadata import Metadata
from dbcsv.app.core.storage_layer.query_executor import QueryExecutor

tests = [
    # simple equality on integer column
    "SELECT * FROM table3 WHERE 'Aaha' < full_name",  
    
    # numeric greater‑than on FLOAT column
    "SELECT * FROM table3 WHERE gpa > 3.0",  
    
    # boolean comparison
    "SELECT * FROM table3 WHERE is_enrolled < student_id",  
    
    # date comparison with string literal
    "SELECT * FROM table3 WHERE birth_date < '2000-01-01'",  
    
    # combined AND on numeric and boolean
    "SELECT * FROM table3 WHERE gpa >= 3.5 AND is_enrolled < TRUE",  
    
    # mixed OR/AND with parentheses
    "SELECT * FROM table3 WHERE gpa > student_id AND birth_date >= '1999-01-01'",  
]

if __name__ == "__main__":
    metadatas = {
        'db1': Metadata('db1'),
        'db2': Metadata('db2')
    }
    validator = SQLValidator(metadatas)
    query_executor = QueryExecutor(metadatas)
    for sql in tests:
        sql_dict = validator.parse(sql)
        sql_dict = validator.validate(sql_dict, 'db1')
        
        schema = "db1"
        table="table1"
        metadata = Metadata(schema)
        
        results = query_executor.execute_parsed_sql(sql_dict)
        cnt = 0
        print("_" * 30)
        for row in results:
            print("ROW: ", row)
            cnt += 1
            if cnt > 10:
                break

