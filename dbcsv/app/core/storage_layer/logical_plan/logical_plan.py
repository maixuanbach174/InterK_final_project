from typing import List, Any, Dict, Iterator

class LogicalPlan:
    def execute(self) -> Iterator[List[Any]]:
        raise NotImplementedError("Subclasses must implement execute()")
    @property
    def columns(self) -> List[str]:
        raise NotImplementedError("Subclasses must implement columns property")
    @property
    def column_types(self) -> List[str]:
        raise NotImplementedError("Subclasses must implement column_types property")

    def __repr__(self):
        return f"{self.__class__.__name__}()"