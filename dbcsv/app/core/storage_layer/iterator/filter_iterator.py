from typing import List, Any, Iterator, Callable


class FilterIterator:
    def __init__(self, child_iter: Iterator[List[Any]], 
                 predicate: Callable[[List[Any], List[str]], bool]):
        self.child_iter = child_iter
        self.predicate = predicate
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
