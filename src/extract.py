import logging
import requests
import csv

# Инициализация логирования
# Устанавливаем уровень логирования и формат сообщений
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Будем использовать класс DataExtractor для определения методов извлечения данных4
# Для демонстрации определим два метода извлечения из API и из CSV
class DataExtractor:
    def __init__(self, config: dict):
        # config - словарь с настройками
        # Он определяется в main.py при инициализации класса
        self.config = config

    def extract_from_api(self, endpoint: str, headers: dict = None) -> dict | None:
        """Метод извлечения данных из API

        Args:
            endpoint (str): адрес запроса к API
            headers (dict, optional): Заголовки запроса к API. По умолчанию None.

        Returns:
            dict | None: словарь с данными или None в случае ошибки
        """
        logging.info(f"Извлечение данных из API: {endpoint}")

        if not headers:
            # Если заголовки не указаны, используем headers из конфигурации
            headers = self.config.get("headers", {})
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()  # Проверяем на наличие ошибок
            try:
                data = response.json()
                logging.info("Данные успешно извлечены из API.")
                return data
            except ValueError:
                logging.error("Ответ API не является JSON.")
                return None
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
        ) as e:
            logging.error(f"Ошибка при извлечении данных из API: {e}")
            return None

    def extract_from_csv(self, file_path: str) -> list | None:
        """Метод извлечения данных из CSV файла

        Args:
            file_path (str): путь к файлу CSV

        Returns:
            list | None: список строк из CSV файла или None в случае ошибки
        """
        logging.info(f"Извлечение данных из CSV: {file_path}")
        try:
            with open(file_path, mode="r", encoding="utf-8") as file:
                reader = csv.reader(file)
                data = [row for row in reader]
                logging.info("Данные успешно извлечены из CSV.")
                return data
        except FileNotFoundError:
            logging.error(f"Файл не найден: {file_path}")
            return None
        except Exception as e:
            logging.error(f"Ошибка при извлечении данных из CSV: {e}")
            return None

    # Add other data extraction methods here
