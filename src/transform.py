import logging
import os
import sqlite3
import pandas as pd


logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Класс-обработчик данных.
    Содержит методы преобразования извлеченных данных.
    """

    def __init__(self, config: dict) -> None:
        self.config = config

    def transform_sqlite(
        self, data: dict[str, tuple[list[str], list[tuple]]]
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

        config = self.config.get("transformations", {})
        transformed = {}

        for table_name, (columns, rows) in data.items():
            if not rows:
                continue

            logger.info(f"Преобразование таблицы: {table_name}")
            df = pd.DataFrame(rows, columns=columns)

            # Удаление null-ов
            clean_cfg = config.get("clean_data", {})
            if clean_cfg.get("type") == "drop_nulls":
                cols = clean_cfg.get("columns", "*")
                (
                    df.dropna(inplace=True)
                    if cols == "*"
                    else df.dropna(subset=cols, inplace=True)
                )

            # Удаление дубликатов
            dup_cfg = config.get("remove_duplicates", {})
            if dup_cfg.get("type") == "drop_duplicates":
                cols = dup_cfg.get("columns", "*")
                (
                    df.drop_duplicates(inplace=True)
                    if cols == "*"
                    else df.drop_duplicates(subset=cols, inplace=True)
                )

            transformed[table_name] = df.reset_index(drop=True)

        return transformed
