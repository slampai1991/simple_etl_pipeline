import logging
import psycopg2
import pandas as pd
from typing import Literal
from sqlalchemy import create_engine
from clickhouse_connect import get_client


logger = logging.getLogger(__name__)


class PostgresLoader:
    def __init__(self, config: dict):
        pg_conf = config["load_config"]["postgres"]
        self.engine = self._create_engine(pg_conf)

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
                logger.info(
                    f"Пропущена загрузка пустой таблицы '{table_name}' в PostgreSQL"
                )
                continue
            try:
                logger.info(f"Загрузка таблицы '{table_name}' в PostgreSQL...")
                df.to_sql(table_name, self.engine, index=False, if_exists=if_exists)
                logger.info(f"Таблица '{table_name}' успешно загружена в PostgreSQL.")
            except Exception as e:
                logger.error(f"Ошибка при загрузке '{table_name}' в PostgreSQL: {e}")


class ClickHouseLoader:
    def __init__(self, config: dict):
        ch_conf = config["load_config"]["clickhouse"]
        self.client = get_client(
            host=ch_conf["host"],
            port=ch_conf["port"],
            username=ch_conf["user"],
            password=ch_conf["password"],
            secure=ch_conf.get("secure", False),
        )

    def load_dataframe(self, df: pd.DataFrame, table: str):
        if df.empty:
            logger.info(f"Пропущена загрузка пустой таблицы '{table}' в ClickHouse.")
            return

        try:
            logger.info(f"Загрузка таблицы '{table}' в ClickHouse...")
            self.client.insert_df(table, df)
            logger.info(f"Таблица '{table}' успешно загружена в ClickHouse.")
        except Exception as e:
            logger.error(f"Ошибка при загрузке '{table}' в ClickHouse: {e}")
