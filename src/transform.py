import logging
from main import load_config

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataTransformer:
    def __init__(self):
        self.config = load_config()

    def normalize_to_1nf(self, data):
        """
        Нормализация данных до 1НФ (Первая Нормальная Форма).

        :param data: Список словарей с данными для нормализации.
        :return: Список словарей с нормализованными данными.
        """
        normalized_data = []
        for record in data:
            for key, value in record.items():
                if isinstance(value, list):
                    for item in value:
                        new_record = record.copy()
                        new_record[key] = item
                        normalized_data.append(new_record)
                else:
                    normalized_data.append(record)
        logging.info("Data normalized to 1NF.")
        return normalized_data

    def normalize_to_2nf(self, data, primary_key, dependent_columns):
        """
        Нормализация данных до 2НФ (Вторая Нормальная Форма).

        :param data: Список словарей с данными для нормализации.
        :param primary_key: Поле, используемое в качестве первичного ключа.
        :param dependent_columns: Список зависимых столбцов для выделения в отдельную таблицу.
        :return: Кортеж из двух списков: основная таблица и зависимая таблица.
        """
        main_table = []
        dependent_table = []

        for record in data:
            main_record = {key: record[key] for key in record if key not in dependent_columns}
            dependent_record = {key: record[key] for key in dependent_columns}
            dependent_record[primary_key] = record[primary_key]

            main_table.append(main_record)
            dependent_table.append(dependent_record)

        logging.info("Data normalized to 2NF.")
        return main_table, dependent_table

if __name__ == "__main__":
    transformer = DataTransformer()

    # Example usage
    sample_data = [
        {"id": 1, "name": "John", "hobbies": ["reading", "swimming"], "city": "New York"},
        {"id": 2, "name": "Jane", "hobbies": ["dancing"], "city": "Los Angeles"}
    ]

    # Normalize to 1NF
    data_1nf = transformer.normalize_to_1nf(sample_data)
    logging.info(f"1NF Data: {data_1nf}")

    # Normalize to 2NF
    primary_key = "id"
    dependent_columns = ["city"]
    main_table, dependent_table = transformer.normalize_to_2nf(data_1nf, primary_key, dependent_columns)
    logging.info(f"Main Table: {main_table}")
    logging.info(f"Dependent Table: {dependent_table}")