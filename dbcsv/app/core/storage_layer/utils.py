from typing import List, Any, Callable

from dbcsv.app.core.storage_layer.logical_plan.logical_plan import LogicalPlan
from dbcsv.app.core.storage_layer.logical_plan.scan import Scan
from dbcsv.app.core.storage_layer.logical_plan.filter import Filter
from dbcsv.app.core.storage_layer.logical_plan.project import Project
from dbcsv.app.core.storage_layer.metadata import Metadata
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
    schema = metadata.name
    table_metadata = metadata.get_table(parsed_query['table'])

    if parsed_query['type'] != 'select':
        raise ValueError(f"Unsupported query type: {parsed_query['type']}")
    
    plan = Scan(schema, parsed_query['table'], table_metadata)
    
    if parsed_query['where'] is not None:
        predicate = build_predicate(parsed_query['where'])
        plan = Filter(plan, predicate)
    
    plan = Project(plan, parsed_query['columns'])
    
    return plan



def build_predicate(condition: dict) -> Callable[[List[Any], List[str]], bool]:
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
    if isinstance(operand, str):
        if (operand.startswith("'") and operand.endswith("'")) or \
           (operand.startswith('"') and operand.endswith('"')):
            literal = operand[1:-1]
            return lambda row, schema: literal
        else:
            # Column reference
            return lambda row, schema: get_column_value(row, schema, operand)
    elif operand is None:
        return lambda row, schema: None
    else:
        return lambda row, schema: operand

def get_column_value(row: List[Any], schema: List[str], column_name: str) -> Any:
    try:
        col_index = schema.index(column_name)
        return row[col_index] if col_index < len(row) else None
    except ValueError:
        raise ValueError(f"Column '{column_name}' not found in schema {schema}")


