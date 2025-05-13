import logging
import requests
import csv
import os
import sqlite3
import yaml


logger = logging.getLogger(__name__)


def load_config(config_path="config.yaml"):
    """
    Загружает конфигурацию из YAML файла.
    
    Args:
        config_path (str): Путь к файлу конфигурации.
        
    Returns:
        dict: Загруженная конфигурация.
        
    Raises:
        FileNotFoundError: Если файл конфигурации не найден.
    """
    try:
        with open(config_path, "r", encoding="utf8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Файл конфигурации '{config_path}' не найден.")
        raise


class DataExtractor:
    def __init__(self, config: dict):
        """
        Инициализирует экстрактор данных.
        
        Args:
            config (dict): Словарь с конфигурацией.
        """
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
                data = list(reader)
            
            logger.info(f"Данные успешно извлечены из файла '{file_path}'.")
            return data

        except Exception as e:
            logger.error(f"Ошибка при извлечении данных из CSV: {e}")
            raise

    def extract_sqlite(self, query: str = None, db_path: str = None) -> list:
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
                self.config["sqlite_config"]["db_name"]
            )

        try:
            logger.info(f"Извлечение данных из базы данных SQLite '{db_path}'...")
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                if not query:
                    query = self.config["sqlite_config"]["query"]
                    
                cursor.execute()
                data = cursor.fetchall()
                logger.info(f"Данные успешно извлечены из базы данных SQLite '{db_path}'.")
                return data
                    
        except sqlite3.Error as e:
            logger.error(f"Ошибка при работе с БД SQLite: {e}")
            raise


# Пример использования (закомментирован для использования как модуль)
if __name__ == "__main__":
    config = load_config()
    extractor = DataExtractor(config)
    data = extractor.extract_sqlite()
    print(f"Извлечено {len(data) if data else 0} записей")