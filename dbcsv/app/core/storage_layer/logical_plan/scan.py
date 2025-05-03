from typing import List

from dbcsv.app.core.storage_layer.logical_plan.logical_plan import LogicalPlan
from dbcsv.app.core.storage_layer.iterator.table_iterator import TableIterator
from dbcsv.app.core.storage_layer.metadata import Metadata


class Scan(LogicalPlan):
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