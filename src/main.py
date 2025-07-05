import argparse
import csv
import re
from typing import List, Optional, Dict, Callable
from statistics import median
from tabulate import tabulate

from src.interfaces import (
    IFilter,
    IAggregator,
    IDataReader,
    IPrinter,
    IAggregationStrategy,
    ICommand,
)


class AvgAggregation(IAggregationStrategy):
    def aggregate(self, values: List[float]) -> float:
        return sum(values) / len(values) if values else float("nan")


class MinAggregation(IAggregationStrategy):
    def aggregate(self, values: List[float]) -> float:
        return min(values) if values else float("nan")


class MaxAggregation(IAggregationStrategy):
    def aggregate(self, values: List[float]) -> float:
        return max(values) if values else float("nan")


class MedianAggregation(IAggregationStrategy):
    def aggregate(self, values: List[float]) -> float:
        return median(values) if values else float("nan")


class AggregationStrategyFactory:
    _strategies: Dict[str, Callable[[], IAggregationStrategy]] = {
        "avg": AvgAggregation,
        "min": MinAggregation,
        "max": MaxAggregation,
        "median": MedianAggregation,
    }

    @classmethod
    def get(cls, name: str) -> IAggregationStrategy:
        if name not in cls._strategies:
            raise ValueError(f"Неизвестная агрегация: {name}")
        return cls._strategies[name]()


class FilterFactory:
    """
    Фабрика для создания фильтров
    """

    @staticmethod
    def create(column: str, op: str, value: str) -> IFilter:
        return RowFilter(column, op, value)


class DictAggregator(IAggregator):
    """
    Агрегатор для списка словарей (обычный CSV)
    """

    def __init__(self, column: str, strategy: IAggregationStrategy):
        self.column = column
        self.strategy = strategy

    def aggregate(self, rows: List[dict]) -> Optional[float]:
        values = [
            float(row[self.column])
            for row in rows
            if row[self.column] not in (None, "")
        ]
        if not values:
            return None
        return self.strategy.aggregate(values)


class AggregatorFactory:
    """
    Фабрика для создания агрегаторов
    """

    @staticmethod
    def create(column: str, func: str) -> IAggregator:
        strategy = AggregationStrategyFactory.get(func)
        return DictAggregator(column, strategy)


class RowFilter(IFilter):
    """
    Конкретная реализация фильтра для строк
    """

    def __init__(self, column: str, op: str, value: str):
        self.column = column
        self.op = op
        self.value = value

    def match(self, row: dict) -> bool:
        """
        Проверяет, соответствует ли значение в ячейке фильтру
        """
        cell = row[self.column]
        try:
            cell_val = float(cell)
            value_val = float(self.value)
        except ValueError:
            cell_val = cell
            value_val = self.value
        if self.op == ">":
            return cell_val > value_val
        elif self.op == "<":
            return cell_val < value_val
        elif self.op == "=":
            return cell_val == value_val
        else:
            raise ValueError(f"Неизвестный оператор: {self.op}")


class CsvDictReader(IDataReader):
    """
    Чтение CSV в список словарей
    """

    def read(self, file_path: str) -> List[dict]:
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)


class DictPrinter(IPrinter):
    """
    Вывод для списка словарей
    """

    def print(self, rows: List[dict]):
        print(tabulate(rows, headers="keys", tablefmt="grid"))


class FilterCommand(ICommand):
    def __init__(self, filter_factory: FilterFactory, column: str, op: str, value: str):
        self.filter = filter_factory.create(column, op, value)

    def execute(self, rows: List[dict]) -> List[dict]:
        return [row for row in rows if self.filter.match(row)]


class OrderByCommand(ICommand):
    def __init__(self, column: str, reverse: bool):
        self.column = column
        self.reverse = reverse

    def execute(self, rows: List[dict]) -> List[dict]:
        def key_func(r):
            val = r[self.column]
            try:
                return float(val)
            except (ValueError, TypeError):
                return val

        return sorted(rows, key=key_func, reverse=self.reverse)


class AggregateCommand(ICommand):
    def __init__(self, aggregator: IAggregator, column: str, func: str):
        self.aggregator = aggregator
        self.column = column
        self.func = func

    def execute(self, rows: List[dict]) -> List[dict]:
        result = self.aggregator.aggregate(rows)
        print(tabulate([[result]], headers=[f"{self.func}"], tablefmt="grid"))
        return rows


class ArgsParser:
    """
    Парсер аргументов
    """

    def __init__(self, description: str):
        self.parser = argparse.ArgumentParser(description=description)
        self.parser.add_argument("--file", required=True, help="Путь к CSV-файлу")
        self.parser.add_argument(
            "--where",
            nargs=1,
            metavar="COLUMN[OP]VALUE",
            help="Фильтрация: колонка=значение, колонка>значение или "
            'колонка<значение (например, "brand=xiaomi", "price>100")',
        )
        self.parser.add_argument(
            "--aggregate",
            nargs="+",
            metavar="COLUMN=FUNC",
            help="Агрегация: колонка=функция (например, rating=avg)",
        )
        self.parser.add_argument(
            "--order-by", metavar="COLUMN=asc|desc", help="Сортировка: колонка=asc|desc"
        )

    def parse(self):
        args = self.parser.parse_args()
        # Обработка --aggregate: поддержка rating=avg
        if args.aggregate:
            if len(args.aggregate) == 1 and "=" in args.aggregate[0]:
                column, func = args.aggregate[0].split("=", 1)
                args.aggregate = [column, func]
            else:
                raise ValueError("Формат --aggregate: rating=avg")
        # Обработка --where: поддержка column=value, column>value, column<value
        if args.where:
            where_arg = args.where[0]  # nargs=1 возвращает список
            m = re.match(r"(.+?)\s*([<>=])\s*(.+)", where_arg)
            if m:
                column, op, value = m.group(1).strip(), m.group(2), m.group(3).strip()
                args.where = [column, op, value]
            else:
                raise ValueError(
                    "Формат --where: column=value, column>value или column<value"
                )
        return args


# --- Приложение --
class CsvApp:
    """
    Класс приложения для работы с обычным CSV
    """

    def __init__(
        self,
        reader: IDataReader,
        printer: IPrinter,
        filter_factory: FilterFactory,
        aggregator_factory: AggregatorFactory,
    ):
        self.reader = reader
        self.printer = printer
        self.filter_factory = filter_factory
        self.aggregator_factory = aggregator_factory

    def run(self, args):
        rows = self.reader.read(args.file)
        commands: List[ICommand] = []
        # Фильтрация
        if args.where:
            column, op, value = args.where
            commands.append(FilterCommand(self.filter_factory, column, op, value))
        # Сортировка
        if args.order_by:
            if "=" not in args.order_by:
                raise ValueError("Формат --order-by: COLUMN=asc|desc")
            column, direction = args.order_by.split("=")
            reverse = direction.lower() == "desc"
            commands.append(OrderByCommand(column, reverse))
        # Агрегация
        if args.aggregate:
            column, func = args.aggregate
            aggregator = self.aggregator_factory.create(column, func)
            commands.append(AggregateCommand(aggregator, column, func))
        # Выполнение команд
        for cmd in commands:
            rows = cmd.execute(rows)
        # Если не было агрегации, выводим таблицу
        if not args.aggregate:
            self.printer.print(rows)


def main():
    args = ArgsParser("CSV фильтр, агрегатор и сортировка").parse()
    app = CsvApp(
        reader=CsvDictReader(),
        printer=DictPrinter(),
        filter_factory=FilterFactory(),
        aggregator_factory=AggregatorFactory(),
    )
    app.run(args)


if __name__ == "__main__":
    main()
