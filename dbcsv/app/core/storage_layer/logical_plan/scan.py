from typing import List

from dbcsv.app.core.storage_layer.logical_plan.logical_plan import LogicalPlan
from dbcsv.app.core.storage_layer.iterator.table_iterator import TableIterator
from dbcsv.app.core.storage_layer.metadata import Metadata


class Scan(LogicalPlan):
    def __init__(self, db: str, table: str, metadata: dict[str, str], batch_size: int = 1024):
        self.__db = db.lower()
        self.__table = table.lower()
        self.__metadata = metadata
        self.__batch_size = batch_size
        self.__columns = list(metadata.keys())
        self.__column_types = list(metadata.values())
        
    def execute(self) -> 'TableIterator':
        return TableIterator(self.__db, self.__table, self.__metadata, self.__batch_size)
    
    @property
    def columns(self):
        return self.__columns

    @property
    def column_types(self):
        return self.__column_types

