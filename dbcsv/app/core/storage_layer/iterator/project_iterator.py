from typing import List, Any, Iterator

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
