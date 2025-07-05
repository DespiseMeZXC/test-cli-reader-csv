import abc
from typing import Optional, Any, List


class IFilter(abc.ABC):
    """
    Абстракция фильтра
    """

    @abc.abstractmethod
    def match(self, row: dict) -> bool:
        pass


class IAggregator(abc.ABC):
    """
    Абстракция агрегатора
    """

    @abc.abstractmethod
    def aggregate(self, data: Any) -> Optional[float]:
        pass


class IDataReader(abc.ABC):
    """
    Абстракция для чтения данных
    """

    @abc.abstractmethod
    def read(self, file_path: str) -> Any:
        pass


class IPrinter(abc.ABC):
    """
    Абстракция для вывода данных
    """

    @abc.abstractmethod
    def print(self, data: Any):
        pass


class IAggregationStrategy(abc.ABC):
    @abc.abstractmethod
    def aggregate(self, values: List[float]) -> float:
        pass


class ICommand(abc.ABC):
    @abc.abstractmethod
    def execute(self, rows: List[dict]) -> List[dict]:
        pass
