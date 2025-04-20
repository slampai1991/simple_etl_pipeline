import logging
import sqlite3
from main import load_config

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataLoader:
    def __init__(self):
        self.config = load_config()

    def load_to_sqlite(self, data, table_name):
        """
        Загрузка данных в базу данных SQLite.

        :param data: Список словарей с данными для загрузки.
        :param table_name: Имя таблицы, в которую будут загружены данные.
        """
        try:
            conn = sqlite3.connect('etl_pipeline.db')
            cursor = conn.cursor()

            # Создание таблицы, если она не существует
            columns = ', '.join([f"{key} TEXT" for key in data[0].keys()])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            cursor.execute(create_table_query)

            # Вставка данных в таблицу
            placeholders = ', '.join(['?' for _ in data[0].keys()])
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            for record in data:
                cursor.execute(insert_query, tuple(record.values()))

            conn.commit()
            conn.close()
            logging.info(f"Данные успешно загружены в таблицу SQLite: {table_name}")
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных в SQLite: {e}")

if __name__ == "__main__":
    loader = DataLoader()

    # Пример использования
    sample_data = [
        {"id": "1", "attribute": "value1"},
        {"id": "2", "attribute": "value2"}
    ]

    loader.load_to_sqlite(sample_data, "generic_table")