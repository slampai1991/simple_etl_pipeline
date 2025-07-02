import logging
import sqlite3
import pandas as pd
from typing import Any
from pathlib import Path
from typing import Optional, Union

LoggerType = Union[logging.Logger, logging.LoggerAdapter]


class DataExtractor:
    """
    Базовый класс для извлечения данных.
    Наследники реализуют поддержку нескольких источников: SQLite, CSV, MongoDB, API.
    """

    def __init__(self, load_cfg: dict, logger: Optional[LoggerType] = None):
        self.cfg = load_cfg
        self.logger: LoggerType = (
            logger if logger is not None else logging.getLogger(__name__)
        )

    def extract(self, *args, **kwargs) -> Any:
        pass


class SQLiteExtractor(DataExtractor):
    def __init__(self, sqlite_cfg: dict, logger: Optional[LoggerType] = None):
        super().__init__(sqlite_cfg, logger=logger)
        self.tables = sqlite_cfg.get("tables", [])

    def extract(self, db_name: Any, db_path: Any) -> dict[str, pd.DataFrame]:
        """
        Извлекает данные из SQLite БД и возвращает как словарь таблиц.

        Returns:
            dict[str, pd.DataFrame]: Табличные данные в формате {table_name: DataFrame}
        """
        
        full_path = Path(db_path) / db_name

        if not full_path.exists():
            raise FileNotFoundError(
                f"База данных по пути '{full_path}' не найдена, либо директория не существует."
            )

        self.logger.info(f"Извлечение данных из SQLite по пути: {full_path}")
        result = {}

        with sqlite3.connect(full_path) as conn:
            for table in self.tables:
                try:
                    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                    result[table] = df
                    self.logger.info(f"Таблица '{table}' извлечена. {len(df)} строк.")
                except Exception as e:
                    self.logger.warning(f"Ошибка извлечения таблицы '{table}': {e}")

        return result


class CSVExtractor(DataExtractor):
    def __init__(self, csv_cfg: dict, logger: Optional[LoggerType] = None):
        super().__init__(csv_cfg, logger=logger)

    def extract(self) -> dict[str, pd.DataFrame]:
        self.logger.info("CSV извлечение ещё не реализовано.")
        return {}


class MongoExtractor(DataExtractor):
    def __init__(self, mongo_cfg: dict, logger: Optional[LoggerType] = None):
        super().__init__(mongo_cfg, logger=logger)

    def extract(self) -> dict[str, pd.DataFrame]:
        self.logger.info("MongoDB извлечение пока не реализовано.")
        return {}


class APIExtractor(DataExtractor):
    def __init__(self, api_cfg: dict, logger: Optional[LoggerType] = None):
        super().__init__(api_cfg, logger=logger)

    def extract(self) -> dict[str, pd.DataFrame | Any]:
        self.logger.info("API извлечение пока не реализовано.")
        return {}
