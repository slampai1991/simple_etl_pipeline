import logging
from typing import Any
import pandas as pd


logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Класс-обработчик данных.
    Содержит методы преобразования извлеченных данных.
    """

    def __init__(self, config: dict) -> None:
        self.config = config

    def transform(
        self, data: dict[str, dict[str, list[str] | list[tuple[Any, ...]]]]
    ) -> dict[str, pd.DataFrame]:
        """
        Обрабатывает данные SQLite согласно конфигурации:
        - Удаление null-значений
        - Удаление дубликатов

        Args:
            data (dict): Словарь с таблицами и их данными (результат extract_sqlite)

        Returns:
            dict: Трансформированные данные в виде DataFrame по таблицам
        """

        cfg = self.config.get("transformations", {})
        transformed = {}
        dropna = cfg.get("dropna", False)
        drop_duplicates = cfg.get("drop_duplicates", False)

        for name, payload in data.items():
            cols = payload.get("columns", [])
            rows = payload.get("rows", [])

            # Создаём DataFrame
            df = pd.DataFrame(rows, columns=cols)

            # Удаляем null-значения
            if dropna:
                df = df.dropna()

            # Удаляем дубликаты
            if drop_duplicates:
                df = df.drop_duplicates()

            # Сохраняем результат
            transformed[name] = df

        return transformed
