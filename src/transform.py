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

        cfg = self.config.get("transformation_config", {})
        transformed = {}
        dropna = cfg.get("dropna", False)
        drop_duplicates = cfg.get("drop_duplicates", False)

        for name, payload in data.items():
            logger.info(f"Трансформация таблицы '{name}'...")
            cols = payload.get("columns", [])
            rows = payload.get("rows", [])

            # Создаём DataFrame
            df = pd.DataFrame(rows, columns=cols)

            # Удаляем null-значения
            if dropna:
                logger.info(f"Удаление null-значений в таблице '{name}'")
                df = df.dropna()
                logger.info(f"Удалено {len(rows) - len(df)} null-значений в таблице {name}")

            # Удаляем дубликаты
            if drop_duplicates:
                logger.info(f"Удаляем дубликаты в таблице {name}")
                df = df.drop_duplicates()
                logger.info(f"Удалено {len(rows) - len(df)} дубликатов в таблице {name}")

            # Сохраняем результат
            transformed[name] = df

        return transformed
