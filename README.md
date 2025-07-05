##

## запуск и вывод файла
```bash
uv run python -m src.main --file files/products.csv
```
## Запуск работы с фильтрацией
```bash
uv run python -m src.main --file files/products.csv --where "price>500"
```
## Запуск работы с сортировкой
```bash
uv run python -m src.main --file files/products.csv --order-by "brand=desc"
```

## Запуск работы с агрегацией
```bash
uv run python -m src.main --file files/products.csv --aggregate "rating=avg"
```

## запуск тестов
```bash
uv run pytest --cov=src tests/
```

