import os
import csv
import json

from typing import Callable, List, Any, Iterator, Optional, Dict
from lark import Lark, Transformer, Token

from metadata import Metadata

grammar = r"""
    start: select_statement 

    select_statement: "SELECT" column_list "FROM" table_name [where_clause]

    column_list: ASTERISK | column_name ("," column_name)*

    column_name: CNAME
    table_name: CNAME

    ASTERISK: "*"

    where_clause: "WHERE" condition

    condition: expression

    expression: comparison_expression
              | expression "AND" expression -> and_expr
              | expression "OR" expression  -> or_expr

    comparison_expression: operand COMPARISON_OP operand
                         | operand "IS" "NULL"         -> is_null
                         | operand "IS" "NOT" "NULL"   -> is_not_null

    SINGLE_QUOTED_STRING: /'(?:[^'\\]|\\.)*'/

    operand: CNAME
           | SIGNED_NUMBER
           | ESCAPED_STRING
           | "NULL"
           | SINGLE_QUOTED_STRING

    COMPARISON_OP: ">" | "<" | "=" | ">=" | "<=" | "!=" | "<>" | "LIKE"


    %import common.CNAME
    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
"""

class SQLTransformer(Transformer):
    def select_statement(self, items):
        return {
            'type': 'select',
            'columns': items[0],
            'table': items[1],
            'where': items[2] if len(items) > 2 else None
        }

    def column_list(self, items):
        if isinstance(items[0], Token) and items[0].type == 'ASTERISK':
            return ['*']
        return [item for item in items if not isinstance(item, Token) or item.type != ',']

    def column_name(self, items):
        return items[0].value

    def table_name(self, items):
        return items[0].value

    def where_clause(self, items):
        return items[0]

    def condition(self, items):
        return items[0]

    def expression(self, items):
        return items[0]

    def and_expr(self, items):
        return {
            'op': 'AND',
            'left': items[0],
            'right': items[1]
        }

    def or_expr(self, items):
        return {
            'op': 'OR',
            'left': items[0],
            'right': items[1]
        }

    def comparison_expression(self, items):
        return {
            'left_operand': items[0],
            'op': items[1].value,
            'right_operand': items[2]
        }

    def is_null(self, items):
        return {
            'left_operand': items[0],
            'op': 'IS NULL'
        }

    def is_not_null(self, items):
        return {
            'left_operand': items[0],
            'op': 'IS NOT NULL'
        }

    def operand(self, items: list[Token]):
        token = items[0]
        if isinstance(token, Token):
            match token.type:
                case "SIGNED_NUMBER":
                    try:
                        return int(token.value)
                    except ValueError:
                        return float(token.value)
                case "CNAME"| "ESCAPED_STRING" | "SINGLE_QUOTED_STRING":
                    return token.value
                case "NULL":
                    return None
                case _:
                    raise ValueError(f"Unexpected token type: {type(token)}")
        else:
            raise ValueError(f"Expected Token, got {type(token)}")
        

       
class LogicalPlan:
    """Base class for all logical plan nodes"""
    def execute(self) -> Iterator[List[Any]]:
        """Execute the plan and return an iterator over result rows"""
        raise NotImplementedError("Subclasses must implement execute()")
    @property
    def columns(self) -> List[str]:
        """Return the list of column names produced by this plan"""
        raise NotImplementedError("Subclasses must implement columns property")
    @property
    def column_types(self) -> List[str]:
        raise NotImplementedError("Subclasses must implement column_types property")

    def __repr__(self):
        return f"{self.__class__.__name__}()"


class Scan(LogicalPlan):
    """Scan a CSV file and produce rows"""
    def __init__(self, schema: str, table: str, metadata: dict[str, str], batch_size: int = 1000):
        self.schema_name = schema.lower()
        self.table_name = table.lower()
        self._metadata = metadata
        self._columns = list(metadata.keys()) if metadata else []
        self._column_types = list(metadata.values()) if metadata else [] 
        self.batch_size = batch_size
        
    def execute(self) -> 'TableIterator':
        return TableIterator(self.schema_name, self.table_name, self._metadata, self.batch_size)
    
    @property
    def columns(self) -> List[str]:
        return self._columns
    @property
    def column_types(self) -> List[str]:
        return self._column_types
    
    def __repr__(self):
        # Print the table name and batch size for better readability
        return f"{self.__class__.__name__}(schema={self._columns}: {self._column_types}, table_name={self.table_name}, batch_size={self.batch_size})"


class Filter(LogicalPlan):
    """Filter rows based on a predicate"""
    def __init__(self, child: LogicalPlan, predicate: Callable[[List[Any], List[str]], bool]):
        self.child = child
        self.predicate = predicate
        
    def execute(self) -> 'FilterIterator':
        return FilterIterator(self.child.execute(), self.predicate, self.child.columns, self.child.column_types)
    
    @property
    def columns(self) -> List[str]:
        return self.child.columns
    @property
    def column_types(self) -> List[str]:
        return self.child.column_types
    
    def __repr__(self):
        return f"{self.__class__.__name__}(predicate={self.predicate}, child={self.child})"


class Project(LogicalPlan):
    """Project specific columns from child plan"""
    def __init__(self, child: LogicalPlan, columns: List[str]):
        self.child = child
        child_collums = child.columns
        
        self._columns = columns if columns != ['*'] else child_collums
        # Calculate column indices for projection
        self._column_indices = [child_collums.index(col) if col in child_collums else -1 for col in self.columns]
        self._column_types = [child.column_types[i] for i in self._column_indices if i != -1]

    def execute(self) -> 'ProjectIterator':
        return ProjectIterator(self.child.execute(), self._column_indices)
    
    @property
    def columns(self) -> List[str]:
        return self._columns
    @property
    def column_types(self) -> List[str]:
        return self._column_types

    def __repr__(self):
        return f"{self.__class__.__name__}(columns={self.columns}, child={self.child})"



from pathlib import Path
DB_DIR = str(Path(__file__).parent.parent.parent.parent / "data")

class TableIterator:
    # path: schema/table_name
    def __init__(self, schema: str, table: str, metadata: dict[str, str] = None, batch_size: int = 1000):
        self.schema = schema.lower()
        self.table_name = table.lower()
        self.batch_size = batch_size
        self._columns = list(metadata.keys()) if metadata else []
        self._column_types = list(metadata.values()) if metadata else []
        self._file = self._load_file(schema=self.schema, table=self.table_name)
        self._reader = csv.reader(self._file)
        self._check_header()
        self._is_done = False
        self._cache: List[List[Any]] = []
        self._used: List[List[Any]] = []
    
    def __iter__(self) -> 'TableIterator':
        return self
    
    def _load_next_batch(self) -> List[List[Any]]:
        self._cache = []
        for _ in range(self.batch_size):
            try:
                row = next(self._reader)
                row = DBTypeObject.convert_type(row, self._column_types)
                
                if len(row) != len(self._columns):
                    raise ValueError(f"Row length does not match column length in {self.schema}/{self.table_name}.")
                self._cache.append(row)
            except StopIteration:
                self._is_done = True
                break
    
    def __next__(self) -> List[List[Any]]:
        if not self._cache and not self._is_done:
            self._load_next_batch()

        if self._cache:
            self._used.append(self._cache[0])
            return self._cache.pop(0)
        else:
            self.close()
            raise StopIteration
        
    def close(self) -> None:
        if hasattr(self, "_file") and self._file:
            self._file.close()


    def _load_file(self, schema: str, table: str):
        schema, table = schema.lower(), table.lower()
        data_path = os.path.join(DB_DIR, schema, table + ".csv")
        print(f"Loading data from {data_path}")
        try:
            return open(data_path, "r", encoding="utf-8")
        except FileNotFoundError:
            raise FileNotFoundError(f"Table {self.table_name} not found.")
        except Exception as e:
            raise Exception(f"Error loading table {self.table_name}: {e}")
        
    def _check_header(self) -> None:
        header = next(self._reader)
        if len(header) != len(self._columns):
            raise ValueError(f"Header length does not match column length in {self.schema}/{self.table_name}.")
        if any(col.lower() != header[i].lower() for i, col in enumerate(self._columns)):
            raise ValueError(f"Header names do not match column names in {self.schema}/{self.table_name}.")

    def __del__(self):
        self.close()

    def __repr__(self):
        result = f"Table: {self.table_name}\n"
        columns = [f"\n\t{col} ({typ})" for col, typ in zip(self._columns, self._column_types)]
        result += f"Columns: {''.join(columns)}\n"
        
        col_widths = [max(len(str(cell)) for cell in [col] + [row[i] for row in self._data]) for i, col in enumerate(self._columns)]

        header = [h.ljust(width) for h, width in zip(self._columns, col_widths)]
        result += " | ".join(header) + "\n"
        result += "-" * (sum(col_widths) + 3 * (len(header) - 1)) + "\n"

        for row in self._cache:
            row_str = [str(cell).ljust(width) for cell, width in zip(row, col_widths)]
            result += " | ".join(row_str) + "\n"

        return result
    
    def to_json(self, limit: int = None):
        if not limit:
            limit = self.batch_size

        tmp_file = self._load_file(schema=self.schema, table=self.table_name)
        tmp_reader = csv.reader(tmp_file)
        next(tmp_reader)  # Skip the header

        data = []
        for i, row in enumerate(tmp_reader):
            if i >= limit:
                break
            if len(row) != len(self._columns):
                raise ValueError(f"Row length does not match column length in {self.schema}/{self.table_name}.")
            data.append(row)

        tmp_file.close()

        result = {
            "table_name": self.table_name,
            "columns": self._columns,
            "column_types": self._column_types,
            "data": data
        }
        return json.dumps(result, indent=4)

    @property
    def cache(self):
        return self._cache

    @property
    def columns(self):
        return self._columns

    @property
    def column_types(self):
        return self._column_types


class FilterIterator:
    """Iterator that filters rows from a child iterator using a predicate"""
    def __init__(self, child_iter: Iterator[List[Any]], 
                 predicate: Callable[[List[Any], List[str]], bool],
                 columns: List[str],
                 column_types: List[str]):
        self.child_iter = child_iter
        self.predicate = predicate
        self._columns = columns
        self._column_types = column_types
        
    def __iter__(self) -> 'FilterIterator':
        return self
        
    def __next__(self) -> List[Any]:
        if not callable(self.predicate):
            raise TypeError("Predicate must be a callable function")
            
        while True:
            try:
                row = next(self.child_iter)
                if self.predicate(row, self.columns):
                    return row
            except StopIteration:
                raise
    
    @property
    def columns(self):
        return self._columns

    @property
    def column_types(self):
        return self._column_types


class ProjectIterator:
    """Iterator that projects specific columns from a child iterator"""
    def __init__(self, child_iter: Iterator[List[Any]], column_indices: List[int]):
        self.child_iter = child_iter
        self.column_indices = column_indices
        
    def __iter__(self) -> 'ProjectIterator':
        return self
        
    def __next__(self) -> List[Any]:
        row = next(self.child_iter)
        return [row[i] if 0 <= i < len(row) else None for i in self.column_indices]




OPERATORS = {
    "=": lambda x, y: x == y,
    "==": lambda x, y: x == y,
    "!=": lambda x, y: x != y,
    "<>": lambda x, y: x != y,
    "<": lambda x, y: x < y,
    "<=": lambda x, y: x <= y,
    ">": lambda x, y: x > y,
    ">=": lambda x, y: x >= y,
}

def sql_to_logical_plan(parsed_query: dict, metadata: Metadata) -> LogicalPlan:
    """
    Convert a parsed SQL query into a logical plan.
    
    Args:
        parsed_query: The parsed SQL query produced by the SQLTransformer
        
    Returns:
        A LogicalPlan object representing the query
    """
    schema = metadata.name
    table_metadata = metadata.get_table(parsed_query['table'])

    if parsed_query['type'] != 'select':
        raise ValueError(f"Unsupported query type: {parsed_query['type']}")
    
    # Start with the base scan operation
    plan = Scan(schema, parsed_query['table'], table_metadata)
    
    # Add a filter if there's a WHERE clause
    if parsed_query['where'] is not None:
        predicate = build_predicate(parsed_query['where'])
        plan = Filter(plan, predicate)
    
    # Add projection for the selected columns
    plan = Project(plan, parsed_query['columns'])
    
    return plan



def build_predicate(condition: dict) -> Callable[[List[Any], List[str]], bool]:
    """
    Build a predicate function from a condition AST.
    
    Args:
        condition: The condition AST from the parsed query
        
    Returns:
        A function that takes a row and schema and returns True if the row matches the condition
    """
    if 'op' in condition:
        if condition['op'] in ('AND', 'OR'):
            left_predicate = build_predicate(condition['left'])
            right_predicate = build_predicate(condition['right'])
            
            if condition['op'] == 'AND':
                return lambda row, schema: left_predicate(row, schema) and right_predicate(row, schema)
            else:  # OR
                return lambda row, schema: left_predicate(row, schema) or right_predicate(row, schema)
    
        elif condition['op'] in OPERATORS:
            left_expr = build_expression(condition['left_operand'])
            right_expr = build_expression(condition['right_operand'])
            op_func = OPERATORS[condition['op']]
            
            return lambda row, schema: op_func(left_expr(row, schema), right_expr(row, schema))
        
        else:
            raise ValueError(f"Unsupported operator: {condition['op']}")
    
    raise ValueError(f"Invalid condition structure: {condition}")

def build_expression(operand: Any) -> Callable[[List[Any], List[str]], Any]:
    """
    Build a function that evaluates an expression operand.
    
    Args:
        operand: A column name, literal value, or complex expression
        
    Returns:
        A function that takes a row and schema and returns the evaluated value
    """
    if isinstance(operand, str):
        # Check if it's a string literal (quoted)
        if (operand.startswith("'") and operand.endswith("'")) or \
           (operand.startswith('"') and operand.endswith('"')):
            # String literal - strip quotes
            literal = operand[1:-1]
            return lambda row, schema: literal
        else:
            # Column reference
            return lambda row, schema: get_column_value(row, schema, operand)
    elif operand is None:
        # NULL literal
        return lambda row, schema: None
    else:
        # Numeric or boolean literal
        return lambda row, schema: operand

def get_column_value(row: List[Any], schema: List[str], column_name: str) -> Any:
    """
    Get the value of a column from a row.
    
    Args:
        row: The data row
        schema: The column names
        column_name: The name of the column to get
        
    Returns:
        The value of the column
    """
    try:
        col_index = schema.index(column_name)
        return row[col_index] if col_index < len(row) else None
    except ValueError:
        raise ValueError(f"Column '{column_name}' not found in schema {schema}")




class Optimizer:
    """Optimizes logical plans before execution"""
    
    @staticmethod
    def optimize(plan: LogicalPlan) -> LogicalPlan:
        """Apply optimization rules to the logical plan"""
        # Apply push down filters
        plan = Optimizer._push_down_filters(plan)
        
        # Additional optimization rules can be added here
        
        return plan
    
    @staticmethod
    def _push_down_filters(plan: LogicalPlan) -> LogicalPlan:
        """Push Filter operations below Project operations"""
        if isinstance(plan, Project) and isinstance(plan.child, Filter):
            # Check if we can safely push the filter below the projection
            # This is possible if all columns used in the filter predicate
            # are still available after the projection
            filter_plan = plan.child
            new_filter = Filter(filter_plan.child, filter_plan.predicate)
            optimized_plan = Project(new_filter, plan.columns)
            print("Optimizer: Pushed Filter below Project")
            return optimized_plan
            
        # Recursively apply the optimization to child plans
        if hasattr(plan, 'child') and plan.child:
            plan.child = Optimizer._push_down_filters(plan.child)
            
        return plan




import datetime

class DBTypeObject:
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        if other in self.values:
            return True
        return False
    
    @staticmethod
    def convert_datatype(data: str, dtype: str = "") -> any:
        if dtype.lower() == STRING:
            if data.startswith("'") and data.endswith("'"):
                return data[1:-1]
            return data
        elif data.startswith("'") and data.endswith("'") and dtype.lower() != STRING:
            raise ValueError(f"Invalid {dtype.lower} format: {data} is a string, not a {dtype.lower()}")
        elif dtype.lower() == INTEGER:
            try:
                return int(data)
            except ValueError:
                raise ValueError(f"Invalid integer format: {data} is not an integer")
        elif dtype.lower() == FLOAT:
            try:
                return float(data)
            except ValueError:
                raise ValueError(f"Invalid float format: {data} is not a float")
        elif dtype.lower() == BOOLEAN:
            if data.lower() in ["true", "false"]:
                return data.lower() == "true"
            else:
                raise ValueError(f"Invalid boolean format: {data} is not a boolean value")
        elif dtype.lower() == DATE or dtype.lower() == DATETIME:
            try:
                return datetime.datetime.strptime(data, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid date format (format %Y-%m-%d): {data}")
        elif dtype.lower() == NULL:
            if data.lower() == "null":
                return None
            else:
                raise ValueError(f"Invalid null format: {data}")
        else:
            try:
                if data.startswith("'") and data.endswith("'"):
                    return data[1:-1]
                else:
                    try:
                        return int(data)
                    except ValueError:
                        try:
                            return float(data)
                        except ValueError:
                            return data
            except Exception as e:
                raise ValueError(f"Invalid data format: {data}") from e
            
    @staticmethod
    def convert_type(row: list[str], column_types: list[str]) -> list[any]:
        if len(row) != len(column_types):
            raise ValueError(f"Row length {len(row)} does not match column types length {len(column_types)}")
        return [DBTypeObject.convert_datatype(data, dtype) for data, dtype in zip(row, column_types)]
            

STRING = DBTypeObject("varchar", "text", "char")
INTEGER = DBTypeObject("integer", "int", "bigint", "smallint", "tinyint")
FLOAT = DBTypeObject("float", "double", "decimal", "dec")
BOOLEAN = DBTypeObject("boolean", "bool")
DATE = DBTypeObject("date")
DATETIME = DBTypeObject("datetime", "timestamp")
NULL = DBTypeObject("null")










# ------------------ Query Executor ------------------
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

        