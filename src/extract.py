import logging
import requests
import csv
import sqlite3
from main import load_config

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataExtractor:
    def __init__(self, config):
        self.config = config

    def extract_from_api(self, endpoint, headers):
        """
        Извлечение данных из API.

        :param endpoint: URL-адрес API для извлечения данных.
        :param headers: Заголовки для запроса API.
        :return: Данные в формате JSON или None в случае ошибки.
        """
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            logging.info("Data successfully extracted from API.")
            return response.json()
        except Exception as e:
            logging.error(f"Error extracting data from API: {e}")
            return None

    def extract_from_csv(self, file_path):
        """
        Извлечение данных из CSV-файла.

        :param file_path: Путь к CSV-файлу.
        :return: Список словарей с данными или None в случае ошибки.
        """
        try:
            with open(file_path, mode='r') as file:
                reader = csv.DictReader(file)
                data = [row for row in reader]
            logging.info("Data successfully extracted from CSV.")
            return data
        except Exception as e:
            logging.error(f"Error extracting data from CSV: {e}")
            return None

    def extract_from_db(self, connection_string, query):
        """
        Извлечение данных из базы данных SQLite.

        :param connection_string: Строка подключения к базе данных SQLite.
        :param query: SQL-запрос для извлечения данных.
        :return: Список кортежей с данными или None в случае ошибки.
        """
        try:
            conn = sqlite3.connect(connection_string)
            cursor = conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            conn.close()
            logging.info("Data successfully extracted from database.")
            return data
        except Exception as e:
            logging.error(f"Error extracting data from database: {e}")
            return None

if __name__ == "__main__":
    config = load_config()
    extractor = DataExtractor(config)

    for source in config['data_sources']:
        if source['type'] == 'api':
            data = extractor.extract_from_api(source['endpoint'], source.get('headers', {}))
        elif source['type'] == 'file':
            data = extractor.extract_from_csv(source['path'])
        elif source['type'] == 'db':
            data = extractor.extract_from_db(source['connection_string'], source['query'])
        else:
            logging.warning(f"Unknown source type: {source['type']}")