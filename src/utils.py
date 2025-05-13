import csv
import sqlite3
import random
import faker
import logging
import os
import pymongo as mongo
from datetime import datetime, timedelta


class DataGenerator:
    # Генератор синтетических данных разных типов (логи, транзакции и т.д.)
    # и разных форматов (CSV, SQLite DB и т.д.)
    # В зависимости от конфигурации, он может генерировать данные в разных форматах
    # Сохраняет данные в заданных форматах локально
    # При необходимости можешь дописать нужные методы, или изменить логику существующих
    # В любом случае, не забудь внести изменения и в конфиг файл
    def __init__(self, config: dict):
        self.config = config  # Конфигурация для генерации данных
        self.faker = faker.Faker()  # Генератор случайных данных

    def generate_sqlite(self, db_name: str) -> None:
        """
        Метод генерации синтетических данных для SQLite DB.
        Генерирует данные в зависимости от конфигурации, указанной в yaml файле.

        Args:
            db_name (str): Имя БД SQLite
        """
        def get_product_names(num_rows: int) -> set:
            """
            Генерация уникальных названий товаров.
            Генерирует случайные комбинации товаров, модели и цвета.

            Args:
                num_rows (int): Количество строк для генерации.

            Returns:
                set: Множество уникальных названий товаров.
            """
            ...
            # Здесь можно использовать любые другие методы генерации (например, API или csv файлы)
            # Товары могут не совпадать с категориями, ведь это синтетические данные
            # В данном случае, просто совмещаем названия, модели и цвета в одну строку
            # В реальных данных естественно будет более сложная логика
            # Но мы не будем тратить время на это

            logging.info("Генерация уникальных названий товаров...")
            product_names = set()

            # Получаем списки из конфигурации
            products = self.config["word_lists"]["products"]
            models = self.config["word_lists"]["models"]
            colors = self.config["word_lists"]["colors"]

            while len(product_names) < num_rows:
                product_name = f"{random.choice(colors)} {random.choice(products)} {random.choice(models)}"
                product_names.add(product_name)

            return product_names

        def generate_data(table_name: str, num_rows: int) -> list:
            """
            Генерация данных для таблицы на основе ее имени и количества строк.
            Периодически добавляет None в данные для имитации отсутствующих значений.

            Args:
                table_name (str): Имя таблицы
                num_rows (int): Количество строк

            Returns:
                list: Список сгенерированных данных
            """
            data = []

            # Можно конечно использоать if-elif,
            # но я всегда хотел попробовать match-case и здесь это уместно
            match table_name:
                case "users":
                    for _ in range(num_rows):
                        data.append(
                            (
                                None,
                                self.faker.name(),
                                random.randint(18, 90),
                                self.faker.phone_number(),
                                self.faker.email(),
                                self.faker.country(),
                                (
                                    datetime.now()
                                    - timedelta(days=random.randint(0, 3650))
                                ).strftime("%Y-%m-%d"),
                            )
                        )
                case "transactions":
                    cursor.execute("SELECT id FROM users")
                    user_ids = [row[0] for row in cursor.fetchall()]
                    unique_combinations = (
                        set()
                    )  # Track unique (user_id, date) combinations

                    for _ in range(num_rows):
                        while True:
                            user_id = random.choice(user_ids)
                            date = (
                                datetime.now() - timedelta(days=random.randint(0, 365))
                            ).strftime("%Y-%m-%d %H:%M:%S")
                            if (user_id, date) not in unique_combinations:
                                unique_combinations.add((user_id, date))
                                break

                        data.append(
                            (
                                user_id,
                                round(random.uniform(10.0, 1000.0), 2),
                                date,
                                random.choice(
                                    self.config["word_lists"]["transaction_desc"]
                                ),
                                random.choice(
                                    ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]
                                ),
                            )
                        )
                case "products":
                    products = get_product_names(num_rows)
                    for _ in range(num_rows):
                        data.append(
                            (
                                None,
                                products.pop(),
                                random.choice(self.config["word_lists"]["categories"]),
                                round(random.uniform(1, 2500), 2),
                            )
                        )
                case "logs":
                    for _ in range(num_rows):
                        severity = (
                            random.choice(["INFO", "WARNING", "ERROR", "CRITICAL"])
                            .strip()
                            .upper()
                        )
                        data.append(
                            (
                                None,
                                severity,
                                random.choice(
                                    self.config["word_lists"]["log_messages"][severity]
                                ),
                                (
                                    datetime.now()
                                    - timedelta(days=random.randint(0, 365))
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                            )
                        )
                case "user_actions":
                    cursor.execute("SELECT id FROM users")
                    user_ids = [row[0] for row in cursor.fetchall()]

                    unique_combinations = (
                        set()
                    )  # Отслеживание уникальных (user_id, timestamp)

                    for _ in range(num_rows):
                        while True:
                            user_id = random.choice(user_ids)
                            timestamp = (
                                datetime.now() - timedelta(days=random.randint(0, 365))
                            ).strftime("%Y-%m-%d %H:%M:%S")
                            if (user_id, timestamp) not in unique_combinations:
                                unique_combinations.add((user_id, timestamp))
                                break

                        data.append(
                            (
                                user_id,
                                random.choice(self.config["word_lists"]["actions"]),
                                timestamp,
                            )
                        )
                case "orders":
                    cursor.execute("SELECT id FROM users")
                    user_ids = [row[0] for row in cursor.fetchall()]

                    cursor.execute("SELECT id FROM products")
                    product_ids = [row[0] for row in cursor.fetchall()]

                    for _ in range(num_rows):
                        data.append(
                            (
                                None,
                                random.choice(user_ids),
                                random.choice(product_ids),
                                (
                                    datetime.now()
                                    - timedelta(days=random.randint(0, 365))
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                random.choice(
                                    self.config["word_lists"]["order_status"]
                                ),
                                random.randint(1, 5),
                            )
                        )
                case _:
                    logging.warning(
                        f"Неизвестная таблица '{table_name}'. Пропуск генерации данных."
                    )
                    return []

            return data

        try:
            logging.info("Подключение к БД SQLite.")
            # Проверяем, существует ли директория для сохранения файла
            destination_path = self.config["data_destinations"]["sqlite"]
            if not os.path.exists(destination_path):
                os.makedirs(destination_path, exist_ok=True)
                logging.info(f"Создана директория {destination_path}")
            db_name = os.path.join(destination_path, db_name)
            conn = sqlite3.connect(db_name)
            cursor = conn.cursor()
            logging.info(f"Подключение к БД SQLite успешно.")

            logging.info("Создание таблиц и заполнение их данными.")
            # Создание таблиц на основе конфигурации
            for table in self.config["sqlite_tables"]:
                table_name = table["name"]
                columns = ", ".join(
                    [
                        f"{key} {value}"
                        for column in table["columns"]
                        for key, value in column.items()
                    ]
                )
                constraints = ", ".join(table.get("constraints", []))
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}{', ' + constraints if constraints else ''});"
                cursor.execute(create_table_query)
                logging.info(f"Таблица '{table_name}' успешно создана.")

            for table in self.config["sqlite_tables"]:
                table_name = table["name"]
                num_rows = table["num_rows"]
                logging.info(f"Генерация {num_rows} строк для таблицы '{table_name}'.")
                data = generate_data(table_name, num_rows)
                logging.info(f"Генерация данных для таблицы '{table_name}' завершена.")

                logging.info(f"Вставка данных в таблицу '{table_name}'...")
                if not data:
                    logging.warning(
                        f"Нет данных для генерации в таблице '{table_name}'."
                    )
                    continue

                placeholders = ", ".join(["?"] * len(data[0]))
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.executemany(insert_query, data)
                logging.info(
                    f"Данные для таблицы '{table_name}' успешно сгенерированы и вставлены."
                )

            conn.commit()
            logging.info("Данные успешно сохранены в БД SQLite.")
        except sqlite3.Error as e:
            logging.error(f"Ошибка при работе с БД SQLite: {e}")
            raise

    def generate_csv(self, csv_name: str) -> None:
        """
        Метод для генерации синтетических данных в CSV файле.
        Генерирует данные в зависимости от конфигурации, указанной в yaml файле.

        Args:
            csv_name (str): Имя генерируемого CSV файла.
        """
        try:
            logging.info("Генерация данных и запись в CSV файл.")
            with open(
                os.path.join(self.config["data_destinations"]["csv"], csv_name),
                "w+",
                newline="",
                encoding="utf-8",
            ) as csvfile:
                # Создание CSV writer объекта для записи данных
                writer = csv.writer(csvfile)
                # Запись заголовков
                writer.writerow(self.config["csv_config"]["headers"])

                # Генерация данных и их запись
                for _ in range(1, self.config["csv_config"]["num_rows"] + 1):
                    row = [
                        _,
                        self.faker.name(),
                        random.randint(18, 65),
                        self.faker.phone_number(),
                        self.faker.email(),
                        self.faker.country(),
                        (
                            datetime.now() - timedelta(days=random.randint(0, 3650))
                        ).strftime("%Y-%m-%d"),
                        random.choice(
                            ["HR", "Engineering", "Sales", "Marketing"]
                        ),  # DEPARTMENT
                        random.choice(
                            [
                                "Manager",
                                "Engineer",
                                "Analyst",
                                "Specialist",
                                "Consultant",
                                "Coordinator",
                                "Director",
                            ]
                        ),  # JOB TITLE
                        random.randint(30000, 150000),  # SALARY
                    ]
                    writer.writerow(row)

            logging.info(f"Данные успешно сохранены в файл '{csv_name}'.")
        except Exception as e:
            logging.error(f"Ошибка при генерации CSV: {e}")
            raise

    def generate_mongo(self, db_name: str) -> None:
        """
        Метод генерации синтетических данных для MongoDB.

        Args:
            db_name (str): Имя коллекции MongoDB.
        """

        def generate_user_data(num_records: int) -> list:
            """Генерирует данные для вставки в MongoDB

            Args:
                num_records (int): Количество генерируемых записей

            Returns:
                list: Результирующие данные
            """
            users = []
            for _ in range(num_records):
                user = {
                    "name": self.faker.name(),
                    "registration_date": str(
                        self.faker.date_between(start_date="-5y", end_date="today")
                    ),
                    "address": {
                        "street": self.faker.street_address(),
                        "city": self.faker.city(),
                        "zipcode": self.faker.zipcode(),
                    },
                    "transaction_history": [
                        {
                            "date": str(self.faker.date_this_year()),
                            "amount": round(random.uniform(10, 500), 2),
                        }
                        for _ in range(random.randint(1, 5))
                    ],
                    "is_active": random.choice([True, False]),
                }
                users.append(user)
            return users

        try:
            logging.info("Подключение к MongoDB.")
            client = mongo.MongoClient(
                host=self.config["mongo_config"]["host"],
                port=self.config["mongo_config"]["port"],
            )
            logging.info("Подключение к MongoDB успешно.")
            db = client[self.config["mongo_config"]["db_name"]]
            users_col = db[self.config["mongo_config"]["collection_name"]]
            logging.info("Создание коллекции и вставка данных.")
            users_col.insert_many(
                generate_user_data(self.config["mongo_config"]["num_records"])
            )
            logging.info("Данные успешно вставлены в MongoDB.")
        except Exception as e:
            logging.error(f"Ошибка при вставке данных в MongoDB: {e}")
            raise

    # По аналогии можно добавить другие методы: JSON, MongoDB, etc.


class DataValidator:
    pass


class DataProfiler:
    pass


class DataAuditor:
    pass
