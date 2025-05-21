import logging
import requests
import os
import sqlite3
import csv

logger = logging.getLogger(__name__)


class DataExtractor:
    def __init__(self, config: dict):
        self.config = config

    def extract_csv(self, file_path: str) -> list:
        """
        Извлекает данные из CSV файла.

        Args:
            file_path (str): Расположение CSV-файла.

        Returns:
            list: Список строк из CSV файла.

        Raises:
            Exception: При ошибке чтения файла.
        """
        try:
            logger.info(f"Извлечение данных из файла '{file_path}'...")

            with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile, delimiter=",")
                # Проверяю, что первая строка - заголовки.
                # Так как по умолчанию проект работает с config-файлом,
                # я не буду пытаться реализовать какую-то универсальную логику.
                headers = next(reader)
                if headers == self.config["csv_config"]["headers"]:
                    data = list(reader)
                else:
                    data = [headers] + list(reader)

            logger.info(f"Данные успешно извлечены из файла '{file_path}'.")
            return data

        except Exception as e:
            logger.error(f"Ошибка при извлечении данных из CSV: {e}")
            raise

    def extract_sqlite(
        self, query: str | None = None, db_path: str | None = None
    ) -> list:
        """
        Извлекает данные из SQLite базы данных.

        Args:
            query (str, optional): SQL запрос. Если не указан, будет использован запрос по умолчанию.
            db_path (str, optional): Расположение SQLite базы данных. Если не указан,
                                    будет использовано значение из конфигурации.

        Returns:
            list: Результат выполнения запроса.

        Raises:
            sqlite3.Error: При ошибке работы с базой данных.
        """
        if not db_path:
            db_path = os.path.join(
                self.config["data_sources"]["sqlite"],
                self.config["sqlite_config"]["db_name"],
            )

        if not query:
            query = self.config["sqlite_config"]["predefined_queries"]["get_all_users"]

        try:
            logger.info(f"Извлечение данных из базы данных SQLite '{db_path}'...")
            logger.info(f"Запрос: {query}")
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()
                if not query:
                    query = self.config["sqlite_config"]["query"]

                cursor.execute(str(query))
                data = cursor.fetchall()
                logger.info(
                    f"Данные успешно извлечены из базы данных SQLite '{db_path}'."
                )
                return data

        except sqlite3.Error as e:
            logger.error(f"Ошибка при работе с БД SQLite: {e}")
            raise

    # Дальше можно добавлять любые подходящие методы извлечения
