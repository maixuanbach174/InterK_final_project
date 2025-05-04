from typing import List, Any, Callable

from dbcsv.app.core.storage_layer.logical_plan.logical_plan import LogicalPlan
from dbcsv.app.core.storage_layer.iterator.filter_iterator import FilterIterator



class Filter(LogicalPlan):
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

