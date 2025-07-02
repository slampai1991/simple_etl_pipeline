import os
import sqlite3
import logging
import pandas as pd
from typing import Literal, Union, Optional
from sqlalchemy import create_engine
from clickhouse_connect import get_client

LoggerType = Union[logging.Logger, logging.LoggerAdapter]


class PostgresLoader:
    def __init__(self, config: dict, logger: Optional[LoggerType] = None):
        pg_conf = config["load_config"]["postgres"]
        self.engine = self._create_engine(pg_conf)
        self.logger: LoggerType = logger or logging.getLogger(__name__)

    def _create_engine(self, conf):
        url = (
            f"postgresql+psycopg2://{conf['user']}:{conf['password']}"
            f"@{conf['host']}:{conf['port']}/{conf['database']}"
        )
        return create_engine(url)

    def load_dataframes(
        self,
        data: dict[str, pd.DataFrame],
        if_exists: Literal["fail", "replace", "append"] = "replace",
    ):
        for table_name, df in data.items():
            if df.empty:
                self.logger.info(
                    f"Пропущена загрузка пустой таблицы '{table_name}' в PostgreSQL"
                )
                continue
            try:
                self.logger.info(f"Загрузка таблицы '{table_name}' в PostgreSQL...")
                df.to_sql(table_name, self.engine, index=False, if_exists=if_exists)
                self.logger.info(f"Таблица '{table_name}' успешно загружена в PostgreSQL.")
            except Exception as e:
                self.logger.error(f"Ошибка при загрузке '{table_name}' в PostgreSQL: {e}")


class ClickHouseLoader:
    def __init__(self, config: dict, logger: Optional[LoggerType] = None):
        ch_conf = config["load_config"]["clickhouse"]
        self.client = get_client(
            host=ch_conf["host"],
            port=ch_conf["port"],
            username=ch_conf["user"],
            password=ch_conf["password"],
            secure=ch_conf.get("secure", False),
        )
        self.logger: LoggerType = logger or logging.getLogger(__name__)

    def load_dataframe(self, df: pd.DataFrame, table: str):
        if df.empty:
            self.logger.info(f"Пропущена загрузка пустой таблицы '{table}' в ClickHouse.")
            return

        try:
            self.logger.info(f"Загрузка таблицы '{table}' в ClickHouse...")
            self.client.insert_df(table, df)
            self.logger.info(f"Таблица '{table}' успешно загружена в ClickHouse.")
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке '{table}' в ClickHouse: {e}")


class SQLiteLoader:
    """
    Класс для загрузки обработанных данных в целевое SQLite хранилище.
    """
    def __init__(self, load_config: dict, logger: Optional[LoggerType] = None):
        self.cfg = load_config
        self.logger: LoggerType = logger or logging.getLogger(__name__)
        if not os.path.exists(self.cfg["db_path"]):
            os.makedirs(self.cfg["db_path"])
            self.logger.info(f"Создана директория для базы данных: {self.cfg['db_path']}")
        self.db_path = os.path.join(self.cfg["db_path"], self.cfg["db_name"])

    def load_dataframe(self, table: str, df: pd.DataFrame) -> None:
        """
        Загружает одну таблицу (DataFrame) в SQLite.

        Args:
            table (str): Имя таблицы в SQLite.
            df (pd.DataFrame): Данные для загрузки.
        """
        if df.empty:
            self.logger.info(f"Пропущена загрузка пустой таблицы '{table}' в SQLite.")
            return

        try:
            self.logger.info(f"Начинается загрузка таблицы '{table}' в SQLite...")
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(table, conn, if_exists=self.cfg["if_exists"], index=False)
            self.logger.info(f"Таблица '{table}' успешно загружена в SQLite.")
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке '{table}' в SQLite: {e}", exc_info=True)

    def load_all(self, data: dict[str, pd.DataFrame]) -> None:
        """
        Загружает все таблицы из словаря в SQLite.

        Args:
            data (dict[str, pd.DataFrame]): Словарь, где ключ — имя таблицы, значение — соответствующий DataFrame.
        """
        self.logger.info("Начинается массовая загрузка всех таблиц в SQLite...")
        
        for table_name, df in data.items():
            self.load_dataframe(table_name, df)
