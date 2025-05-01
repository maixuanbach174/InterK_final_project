from typing import List

from dbcsv.app.core.storage_layer.logical_plan.logical_plan import LogicalPlan
from dbcsv.app.core.storage_layer.iterator.project_iterator import ProjectIterator

class Project(LogicalPlan):
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
