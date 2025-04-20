import logging
import requests
import csv
import sqlite3
from main import load_config

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(уровень)s - %(message)s"
)


class DataExtractor:
    def __init__(self, config: dict):
        self.config = config

    def extract_from_api(self, endpoint: str, headers: dict = None) -> dict | None:
        """Data extraction from API.

        Args:
            endpoint (str): api endpoint URL.
            headers (dict): request headers for authentication.

        Returns:
            dict: response data in JSON format or None in case of error.
        """

        logging.info(f"Data extraction from API: {endpoint}")
        try:
            # Make GET request to the API
            if headers is None:
                # Default headers if not provided
                headers = {}
                headers["Content-Type"] = "application/json"
                headers["Accept"] = "application/json"
                headers["User-Agent"] = (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
                )
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            logging.info("Data successfully extracted from API.")
            return response.json()
        except Exception as e:
            logging.error(f"Error extracting data from API: {e}")
            return None

    def extract_from_csv(self, file_path: str) -> list | None:
        """CSV data extraction.
        CSV file must have a header row.

        Args:
            file_path (str): path to the CSV file.

        Returns:
            list: list of dictionaries with data or None in case of error.
        """
        logging.info(f"Data extraction from CSV: {file_path}")
        try:
            # Read CSV file
            with open(file_path, mode="r") as file:
                reader = csv.DictReader(file)
                data = [row for row in reader]
            logging.info("Data successfully extracted from CSV.")
            return data
        except Exception as e:
            logging.error(f"Error extracting data from CSV: {e}")
            return None

    def extract_from_sqlite_db(self, connection_string: str, query: str) -> list | None:
        """Data extraction from SQLite database.

        Args:
            connection_string (str): SQLite connection string.
            query (str): SQL query to execute.

        Returns:
            list: list of tuples with data or None in case of error.
        """
        try:
            logging.info(f"Data extraction from SQLite DB: {connection_string}")
            # Connect to SQLite database
            conn = sqlite3.connect(connection_string)
            cursor = conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            conn.close()
            logging.info("Данные успешно извлечены из базы данных.")
            return data
        except Exception as e:
            logging.error(f"Ошибка при извлечении данных из базы данных: {e}")
            return None

    def extract_from_file(self, file_path: str) -> list | None:
        logging.info(f"Data extraction from file: {file_path}")
        try:
            with open(file_path, "r") as file:
                data = file.readlines()
            logging.info("Data successfully extracted from file.")
            return data
        except Exception as e:
            logging.error(f"Error extracting data from file: {e}")
            return None
