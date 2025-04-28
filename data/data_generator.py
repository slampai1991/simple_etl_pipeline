import csv
import logging
import sqlite3
import random
import faker
import yaml
import json
import pymongo
from datetime import datetime, timedelta

# Настраиваем логирование в файл для генерации данных
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_generator_errors.log"),
        logging.StreamHandler(),
    ],
)


class DataGenerator:
    # Класс-генератор синтетических данных разных типов (логи ошибок, транзакции и т.д.)
    # и разных форматов (CSV, SQLite DB и т.д.)
    # В зависимости от конфигурации, он может генерировать данные в разных форматах
    # Сохраняет данные в заданных форматах локально
    def __init__(self, config: str):
        # config - путь к yaml файлу с настройками для генерации данных
        try:
            with open(config, "r", encoding="utf-8") as file:
                self.config = yaml.safe_load(file)

            if not self.config.get("mongo_connection_string"):
                logging.warning(
                    "MongoDB connection string not found in configuration. Using default 'mongodb://localhost:27017/'."
                )
        except FileNotFoundError:
            logging.error(f"Файл конфигурации '{config}' не был найден.")
            raise
        except yaml.YAMLError as e:
            logging.error(f"Ошибка при чтении конфигурации: {e}")
            raise

        self.faker = faker.Faker()  # Генератор случайных данных

    def generate_sqlite(self, db_name: str = "synthetic_ecommerce_data.db") -> None:
        """
        Метод для генерации синтетических данных в SQLite DB.
        Генерирует данные в зависимости от конфигурации, указанной в yaml файле.

        Args:
            db_name (str): Путь к файлу SQLite DB. По умолчанию "synthetic_data.db".
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
                    descriptions = [
                        "Payment for subscription",
                        "Refund for canceled subscription",
                        "Payment for product purchase",
                        "Refund for canceled product purchase",
                        "Payment for service",
                        "Refund for canceled service",
                        "Payment for product return",
                        "Payment for product exchange",
                    ]
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
                                random.choice(descriptions),
                                random.choice(
                                    ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]
                                ),
                            )
                        )
                case "products":
                    products = get_product_names(num_rows)
                    category_names = [
                        "Electronics",
                        "Furniture",
                        "Clothing",
                        "Kitchenware",
                        "Toys",
                        "Books",
                        "Sports",
                        "Beauty",
                        "Automotive",
                        "Gardening",
                        "Health",
                        "Office Supplies",
                        "Pet Supplies",
                        "Jewelry",
                        "Watches",
                        "Footwear",
                        "Bags",
                    ]
                    for _ in range(num_rows):
                        data.append(
                            (
                                None,
                                products.pop(),
                                random.choice(category_names),
                                round(random.uniform(1, 2500), 2),
                            )
                        )
                case "logs":
                    messages = {
                        "INFO": [
                            "Operation completed successfully.",
                            "User logged in.",
                            "Data saved to database.",
                            "File uploaded successfully.",
                            "Connection established.",
                        ],
                        "WARNING": [
                            "Low disk space.",
                            "High memory usage.",
                            "Unresponsive script.",
                            "Deprecated API usage.",
                            "Slow response time.",
                        ],
                        "ERROR": [
                            "File not found.",
                            "Database connection failed.",
                            "Invalid input data.",
                            "Permission denied.",
                            "Network error.",
                        ],
                        "CRITICAL": [
                            "System crash.",
                            "Data corruption detected.",
                            "Security breach.",
                            "Service unavailable.",
                            "Critical hardware failure.",
                        ],
                    }
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
                                random.choice(messages[severity]),
                                (
                                    datetime.now()
                                    - timedelta(days=random.randint(0, 365))
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                            )
                        )
                case "user_actions":
                    cursor.execute("SELECT id FROM users")
                    user_ids = [row[0] for row in cursor.fetchall()]
                    actions = [
                        "LOGIN",
                        "LOGOUT",
                        "PURCHASE",
                        "ADD_TO_CART",
                        "REMOVE_FROM_CART",
                        "SEARCH",
                        "VIEW_PRODUCT",
                        "CHECKOUT",
                        "UPDATE_PROFILE",
                        "SUBSCRIBE",
                    ]
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
                                random.choice(actions),
                                timestamp,
                            )
                        )
                case "orders":
                    cursor.execute("SELECT id FROM users")
                    user_ids = [row[0] for row in cursor.fetchall()]
                    cursor.execute("SELECT id FROM products")
                    product_ids = [row[0] for row in cursor.fetchall()]
                    statuses = [
                        "PENDING",
                        "SHIPPED",
                        "DELIVERED",
                        "CANCELLED",
                        "RETURNED",
                        "COMPLETED",
                    ]

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
                                random.choice(statuses),
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
            conn = sqlite3.connect(db_name)
            cursor = conn.cursor()

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

    def generate_csv(self, csv_name: str = "synthetic_employees_data.csv") -> None:
        """
        Метод для генерации синтетических данных в CSV файле.
        Генерирует данные в зависимости от конфигурации, указанной в yaml файле.

        Args:
            csv_name (str): Путь к файлу CSV. По умолчанию "synthetic_data.csv".
        """
        try:
            logging.info("Генерация данных в CSV формате.")
            with open(csv_name, "w", newline="", encoding="utf-8") as csvfile:
                # Создание CSV writer объекта для записи данных
                # Используем utf-8 для поддержки кириллицы и других символов
                writer = csv.writer(csvfile)
                # Запись заголовков
                writer.writerow(self.config["csv_file"]["headers"])

                for _ in range(1, self.config["csv_file"]["num_rows"] + 1):
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

    def generate_json(self, json_name: str = "synthetic_heroes_data.json") -> None:
        """
        Метод для генерации синтетических данных в JSON файле.
        Генерирует данные в зависимости от конфигурации, указанной в yaml файле.

        Args:
            json_name (str): Путь к файлу JSON. По умолчанию "synthetic_marvel_heroes_data.json".
        """

        def generate_hero(self) -> dict:
            """
            Генерация данных для одного героя.
            Использует списки из конфигурации для генерации случайных значений.
            """
            # Получаем списки из конфигурации
            aliases = self.config["word_lists"]["hero_aliases"]
            types = self.config["word_lists"]["hero_types"]
            teams = self.config["word_lists"]["hero_teams"]
            powers = self.config["word_lists"]["hero_powers"]
            weaknesses = self.config["word_lists"]["hero_weaknesses"]
            weapons = self.config["word_lists"]["hero_weapons"]

            hero = {
                "name": self.faker.name(),
                "age": random.randint(18, 90),
                "alias": f"{random.choice(aliases)} {random.choice(types)}",
                "team": random.choice(teams),
                "superpower": random.choice(powers),
                "weakness": random.choice(weaknesses),
                "weapon": random.choice(weapons),
                "origin": self.faker.city() + ", " + self.faker.country(),
                "first_appearance": (
                    datetime.now() - timedelta(days=random.randint(0, 36500))
                ).strftime("%Y-%m-%d"),
                "status": random.choice(["ACTIVE", "INACTIVE", "DECEASED", "UNKNOWN"]),
            }
            return hero

        try:
            logging.info("Генерация данных в JSON формате.")
            data = []

            for _ in range(1, self.config["json_file"]["num_rows"] + 1):
                hero = generate_hero(self)
                data.append(hero)

            with open(json_name, "w", encoding="utf-8") as jsonfile:
                json.dump(data, jsonfile, ensure_ascii=False, indent=4)

            logging.info(f"Данные успешно сохранены в файл '{json_name}'.")
        except Exception as e:
            logging.error(f"Ошибка при генерации JSON: {e}")
            raise

    def generate_mongo(self, db_name: str = "synthetic_mongo_data") -> None:
        """
        Метод для генерации синтетических данных в MongoDB.
        Генерирует данные в зависимости от конфигурации, указанной в yaml файле.

        Args:
            db_name (str): Имя базы данных MongoDB. По умолчанию "synthetic_mongo_data".
        """
        pass


obj = DataGenerator("config.yaml")
obj.generate_sqlite("synthetic_ecommerce_data.db")
obj.generate_csv("synthetic_employees_data.csv")
obj.generate_json("synthetic_heroes_data.json")
