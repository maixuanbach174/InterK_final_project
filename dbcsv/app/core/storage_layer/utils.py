from typing import Any, Dict

from dbcsv.app.core.storage_layer.logical_plan.logical_plan import LogicalPlan
from dbcsv.app.core.storage_layer.logical_plan.scan import Scan
from dbcsv.app.core.storage_layer.logical_plan.filter import Filter
from dbcsv.app.core.storage_layer.logical_plan.project import Project
from dbcsv.app.core.storage_layer.metadata import Metadata

def sql_to_logical_plan(sql_dict: Dict[str, Any], metadata: Metadata) -> LogicalPlan:
    db = metadata.name
    table_metadata = metadata.get_table(sql_dict['table'])
    
    plan = Scan(db, sql_dict['table'], table_metadata)
    
    if sql_dict['predicate'] is not None:
        plan = Filter(plan, sql_dict['predicate'])
    plan = Project(plan, sql_dict['columns'])
    
    return plan