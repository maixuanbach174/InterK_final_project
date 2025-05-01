from typing import List, Any, Iterator, Callable


class FilterIterator:
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
