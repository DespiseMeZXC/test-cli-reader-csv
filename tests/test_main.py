import sys
import os
import pytest


from src.main import (
    RowFilter,
    FilterFactory,
    DictAggregator,
    AggregatorFactory,
    AvgAggregation,
    MinAggregation,
    MaxAggregation,
    MedianAggregation,
    OrderByCommand,
    CsvApp,
    ArgsParser,
    AggregationStrategyFactory,
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


# --- Тестовые данные ---
DATA = [
    {"brand": "A", "price": "100"},
    {"brand": "B", "price": "200"},
    {"brand": "C", "price": "300"},
    {"brand": "A", "price": "400"},
]


# --- Фильтрация ---
def test_row_filter_gt():
    f = RowFilter("price", ">", "150")
    filtered = [row for row in DATA if f.match(row)]
    assert len(filtered) == 3
    assert all(float(row["price"]) > 150 for row in filtered)


def test_row_filter_eq():
    f = RowFilter("brand", "=", "A")
    filtered = [row for row in DATA if f.match(row)]
    assert len(filtered) == 2
    assert all(row["brand"] == "A" for row in filtered)


# --- Агрегации ---
def test_aggregations():
    values = [float(row["price"]) for row in DATA]
    assert AvgAggregation().aggregate(values) == 250
    assert MinAggregation().aggregate(values) == 100
    assert MaxAggregation().aggregate(values) == 400
    assert MedianAggregation().aggregate(values) == 250


def test_dict_aggregator_avg():
    agg = DictAggregator("price", AvgAggregation())
    assert agg.aggregate(DATA) == 250


def test_aggregator_factory():
    agg = AggregatorFactory.create("price", "median")
    assert agg.aggregate(DATA) == 250


def test_order_by_command_asc():
    cmd = OrderByCommand("brand", reverse=False)
    sorted_rows = cmd.execute(DATA)
    brands = [row["brand"] for row in sorted_rows]
    assert brands == ["A", "A", "B", "C"]


def test_order_by_command_desc():
    cmd = OrderByCommand("brand", reverse=True)
    sorted_rows = cmd.execute(DATA)
    brands = [row["brand"] for row in sorted_rows]
    assert brands == ["C", "B", "A", "A"]


# --- Интеграционный тест ---
class DummyPrinter:
    def __init__(self):
        self.last = None

    def print(self, rows):
        self.last = rows


class DummyReader:
    def __init__(self, data):
        self.data = data

    def read(self, file_path):
        return self.data


def test_app_filter_aggregate_order():
    class Args:
        file = "dummy.csv"
        where = None
        aggregate = None
        order_by = None

    printer = DummyPrinter()
    reader = DummyReader(DATA)
    app = CsvApp(
        reader=reader,
        printer=printer,
        filter_factory=FilterFactory(),
        aggregator_factory=AggregatorFactory(),
    )
    app.run(Args)
    assert printer.last == DATA
    filtered = [row for row in DATA if float(row["price"]) > 150]
    sorted_expected = sorted(filtered, key=lambda r: r["brand"])
    cmd = OrderByCommand("brand", reverse=False)
    sorted_rows = cmd.execute(filtered)
    assert sorted_rows == sorted_expected


def test_aggregate_wrong_format(monkeypatch):
    parser = ArgsParser("test")
    monkeypatch.setattr(
        sys, "argv", ["main.py", "--file", "f.csv", "--aggregate", "ratingavg"]
    )
    with pytest.raises(ValueError):
        parser.parse()


def test_where_wrong_format(monkeypatch):
    parser = ArgsParser("test")
    monkeypatch.setattr(sys, "argv", ["main.py", "--file", "f.csv", "--where", "brand"])
    with pytest.raises(ValueError):
        parser.parse()


def test_aggregate_unknown_func():
    with pytest.raises(ValueError):
        AggregationStrategyFactory.get("unknown")


def test_row_filter_unknown_op():
    f = RowFilter("price", "!", "100")
    with pytest.raises(ValueError):
        f.match({"price": "100"})


def test_empty_aggregate():
    agg = AggregatorFactory.create("price", "avg")
    assert agg.aggregate([]) is None


def test_app_no_commands():
    class Args:
        file = "dummy.csv"
        where = None
        aggregate = None
        order_by = None

    printer = DummyPrinter()
    reader = DummyReader([])
    app = CsvApp(
        reader=reader,
        printer=printer,
        filter_factory=FilterFactory(),
        aggregator_factory=AggregatorFactory(),
    )
    app.run(Args)
    assert printer.last == []


def test_app_only_aggregate():
    # Проверяем, что если только агрегация, printer.print не вызывается
    class Args:
        file = "dummy.csv"
        where = None
        aggregate = ["price", "avg"]
        order_by = None

    printer = DummyPrinter()
    reader = DummyReader(DATA)
    app = CsvApp(
        reader=reader,
        printer=printer,
        filter_factory=FilterFactory(),
        aggregator_factory=AggregatorFactory(),
    )
    app.run(Args)
    assert printer.last is None


def test_app_only_order_by():
    # Проверяем, что если только сортировка, printer.print вызывается
    class Args:
        file = "dummy.csv"
        where = None
        aggregate = None
        order_by = "price=asc"

    printer = DummyPrinter()
    reader = DummyReader(DATA)
    app = CsvApp(
        reader=reader,
        printer=printer,
        filter_factory=FilterFactory(),
        aggregator_factory=AggregatorFactory(),
    )
    app.run(Args)
    # Проверяем, что результат отсортирован по price
    prices = [int(row["price"]) for row in printer.last]
    assert prices == sorted(prices)


def test_app_only_where():
    # Проверяем, что если только фильтрация, printer.print вызывается
    class Args:
        file = "dummy.csv"
        where = ["brand", "=", "A"]
        aggregate = None
        order_by = None

    printer = DummyPrinter()
    reader = DummyReader(DATA)
    app = CsvApp(
        reader=reader,
        printer=printer,
        filter_factory=FilterFactory(),
        aggregator_factory=AggregatorFactory(),
    )
    app.run(Args)
    assert all(row["brand"] == "A" for row in printer.last)
