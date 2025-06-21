import logging
import sqlite3
import pandas as pd
from typing import Any
from pathlib import Path


logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Базовый класс для извлечения данных.
    Наследники реализуют поддержку нескольких источников: SQLite, CSV, MongoDB, API.
    """

    def __init__(self, load_cfg: dict):
        self.cfg = load_cfg

    def extract(self) -> Any:
        pass


class SQLiteExtractor(DataExtractor):
    def __init__(self, sqlite_cfg: dict):
        super().__init__(sqlite_cfg)
        self.db_name = sqlite_cfg.get("db_name")
        self.db_path = sqlite_cfg.get("db_path")
        self.tables = sqlite_cfg.get("tables", [])

    def extract(self) -> dict[str, pd.DataFrame]:
        """
        Извлекает данные из SQLite БД и возвращает как словарь таблиц.

        Returns:
            dict[str, pd.DataFrame]: Табличные данные в формате {table_name: DataFrame}
        """
        full_path = Path(self.db_path) / self.db_name

        if not full_path.exists():
            raise FileNotFoundError(
                f"База данных по пути '{full_path}' не найдена, либо директория не существует."
            )

        logger.info(f"Извлечение данных из SQLite по пути: {full_path}")
        result = {}

        with sqlite3.connect(full_path) as conn:
            for table in self.tables:
                try:
                    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                    result[table] = df
                    logger.info(f"Таблица '{table}' извлечена. {len(df)} строк.")
                except Exception as e:
                    logger.warning(f"Ошибка извлечения таблицы '{table}': {e}")

        return result


class CSVExtractor(DataExtractor):
    def __init__(self, csv_cfg: dict):
        super().__init__(csv_cfg)

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("CSV извлечение ещё не реализовано.")
        return {}


class MongoExtractor(DataExtractor):
    def __init__(self, mongo_cfg: dict):
        super().__init__(mongo_cfg)

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("MongoDB извлечение пока не реализовано.")
        return {}


class APIExtractor(DataExtractor):
    def __init__(self, api_cfg: dict):
        super().__init__(api_cfg)

    def extract(self) -> dict[str, pd.DataFrame | Any]:
        logger.info("API извлечение пока не реализовано.")
        return {}
