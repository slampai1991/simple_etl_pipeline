import logging
import os
import sqlite3
import pandas as pd

logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Базовый класс для извлечения данных.
    Предусматривает поддержку нескольких источников: SQLite, CSV, MongoDB, API.
    """

    def __init__(self, load_cfg: dict):
        self.cfg = load_cfg

    def extract(self, source: str) -> dict[str, pd.DataFrame]:
        """
        Универсальный метод извлечения данных из заданного источника.

        Args:
            source (str): источник ('sqlite', 'csv', 'mongodb', 'api')

        Returns:
            dict[str, pd.DataFrame]: таблицы и соответствующие датафреймы
        """
        if source == "sqlite" and self.cfg.get("sqlite", {}).get("enabled"):
            return SQLiteExtractor(self.cfg["sqlite"]).extract()

        elif source == "csv" and self.cfg.get("csv", {}).get("enabled"):
            return CSVExtractor(self.cfg["csv"]).extract()

        elif source == "mongodb" and self.cfg.get("mongodb", {}).get("enabled"):
            return MongoExtractor(self.cfg["mongodb"]).extract()

        elif source == "api" and self.cfg.get("api", {}).get("enabled"):
            return APIExtractor(self.cfg["api"]).extract()

        else:
            logger.warning(
                f"Источник данных '{source}' не поддерживается или отключён."
            )
            return {}


class SQLiteExtractor(DataExtractor):
    def __init__(self, sqlite_cfg: dict):
        super().__init__(sqlite_cfg)
        self.db_name = sqlite_cfg.get("db_name")
        self.db_path = sqlite_cfg.get("db_path")
        self.tables = sqlite_cfg.get("tables", [])

    def set_db_name(self, db_name: str):
        self.db_name = db_name

    def set_db_path(self, db_path: str):
        self.db_path = db_path

    def get_full_path(self) -> str:
        if not self.db_name or not self.db_path:
            raise ValueError("Необходимо указать и путь, и имя базы данных.")
        return os.path.join(self.db_path, self.db_name)

    def extract(self, source: str = "") -> dict[str, pd.DataFrame]:
        """
        Извлекает данные из SQLite БД и возвращает как словарь таблиц.

        Returns:
            dict[str, pd.DataFrame]: Табличные данные в формате {table_name: DataFrame}
        """
        full_path = self.get_full_path()

        if not os.path.exists(full_path):
            raise FileNotFoundError(f"База данных по пути '{full_path}' не найдена.")

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
        self.directory = csv_cfg.get("directory", "")

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info(f"Извлечение данных из CSV-файлов в {self.directory}")
        result = {}
        if not os.path.exists(self.directory):
            raise FileNotFoundError(f"Каталог CSV не найден: {self.directory}")

        for fname in os.listdir(self.directory):
            if fname.endswith(".csv"):
                path = os.path.join(self.directory, fname)
                try:
                    df = pd.read_csv(path)
                    table_name = os.path.splitext(fname)[0]
                    result[table_name] = df
                    logger.info(f"CSV '{fname}' загружен. {len(df)} строк.")
                except Exception as e:
                    logger.warning(f"Ошибка чтения файла '{fname}': {e}")
        return result


class MongoExtractor(DataExtractor):
    def __init__(self, mongo_cfg: dict):
        super().__init__(mongo_cfg)
        # from pymongo import MongoClient
        # self.client = MongoClient(mongo_cfg["uri"])
        # self.db = self.client[mongo_cfg["database"]]

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("MongoDB извлечение пока не реализовано.")
        return {}


class APIExtractor(DataExtractor):
    def __init__(self, api_cfg: dict):
        super().__init__(api_cfg)
        self.base_url = api_cfg.get("base_url")
        self.endpoints = api_cfg.get("endpoints", [])

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("API извлечение пока не реализовано.")
        return {}
