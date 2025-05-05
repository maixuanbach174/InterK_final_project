from typing import List, Any, Callable, Optional

from dbcsv.app.core.storage_layer.logical_plan.logical_plan import LogicalPlan
from dbcsv.app.core.storage_layer.iterator.filter_iterator import FilterIterator



class Filter(LogicalPlan):
    def __init__(self, child: LogicalPlan, predicate: Optional[Callable[[List[Any], List[str]], bool]]):
        self.__child = child
        self.__predicate = predicate
        
    def execute(self) -> 'FilterIterator':
        if self.__predicate is None:
            return self.__child.execute()
        return FilterIterator(self.__child.execute(), self.__predicate, self.__child.columns, self.__child.column_types)
    
    @property
    def columns(self) -> List[str]:
        return self.__child.columns
    @property
    def column_types(self) -> List[str]:
        return self.__child.column_types
    
    def __repr__(self):
        return f"{self.__class__.__name__}(predicate={self.predicate}, child={self.__child})"

