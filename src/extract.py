import logging
import os
import sqlite3
import csv

logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Класс содержащий методы извлечения данных.
    Пока реализованы только два метода: sqlite и csv экстракторы.
    """
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
        self,
        db_path: str | None = None,
        table_name: str | None = None,
        query: str | None = None,
    ) -> dict[str, list[tuple]]:
        """
        Извлекает данные из SQLite БД.
        Поддерживает извлечение:
        - по произвольному SQL-запросу;
        - одной таблицы по имени;
        - всех таблиц в БД.

        Args:
            db_path (str, optional): Путь к SQLite БД. По умолчанию — из конфигурации.
            table_name (str, optional): Имя таблицы для извлечения. Если None — извлекаются все.
            query (str, optional): Пользовательский SQL-запрос. Если задан — используется вместо table_name.

        Returns:
            dict[str, list[tuple]]: Словарь с именами таблиц и извлечёнными данными.

        Raises:
            sqlite3.Error: В случае ошибки доступа к БД.
        """
        if not db_path:
            db_path = os.path.join(
                self.config["data_sources"]["sqlite"],
                self.config["sqlite_config"]["db_name"],
            )

        extracted_data = {}

        try:
            logger.info(f"Открытие подключения к БД: {db_path}")
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()

                if query:
                    logger.info(f"Извлечение по пользовательскому SQL-запросу: {query}")
                    cursor.execute(query)
                    extracted_data["custom_query"] = cursor.fetchall()
                    return extracted_data

                if table_name:
                    logger.info(f"Извлечение таблицы '{table_name}' из БД.")
                    cursor.execute(f"SELECT * FROM {table_name}")
                    extracted_data[table_name] = cursor.fetchall()
                    return extracted_data

                # Извлечение всех таблиц
                logger.info("Извлечение всех таблиц из БД.")
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                )
                tables = [row[0] for row in cursor.fetchall()]

                for tbl in tables:
                    logger.info(f"Извлечение таблицы: {tbl}")
                    cursor.execute(f"SELECT * FROM {tbl}")
                    extracted_data[tbl] = cursor.fetchall()

                return extracted_data

        except sqlite3.Error as e:
            logger.error(f"Ошибка при извлечении данных: {e}")
            raise
