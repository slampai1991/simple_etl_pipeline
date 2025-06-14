import csv
import sqlite3
import random
import logging
import os
from datetime import datetime, timedelta
from typing import Any
import faker
from pymongo import MongoClient


logger = logging.getLogger(__name__)


class DataGenerator:
    def __init__(self, gen_config: dict):
        self.cfg = gen_config  # Конфигурация для генерации данных
        self.faker = faker.Faker()  # Генератор случайных данных
        self.anomaly_cfg = self.cfg.get(
            "anomaly_cfg", {}
        )  # Конфигурация загрязнения данных

    def _inject_anomaly(
        self,
        value: Any,
        dtype: str,
        nullable: bool = True,
    ):
        """
        Функция для `загрязнения` генерируемых данных.

        Args:
            value (Any): Оригинальное значение, которое вероятностно будет заменено на аномальное значение.
            dtype (str): Тип данных значения. Необходим для выбора аномалий.
            column_name (str, optional): Название колонки, для которой генерируется значение.
                                          Необходим для выбора аномалий. Defaults to None.
            nullable (bool, optional): Флаг, указывающий, может ли значение быть `None`. Defaults to True.

        Returns:
            Загрязненное значение или оригинальное, если не было загрязнения.
        """
        probability = self.anomaly_cfg.get("probability", 0.1)
        if random.random() > probability:
            return value

        strategies = self.anomaly_cfg.get("strategies", {}).get(dtype, [])

        if strategies:
            return random.choice(strategies)

        return None if nullable else value

    @staticmethod
    def _get_product_names(
        num_rows: int, products: list, models: list, colors: list
    ) -> set[str]:
        """
        Генерация уникальных наименований товаров:
        случайные комбинации названий, модели и цвета.

        Args:
            num_rows (int): Количество строк для генерации.
            products (list): Список названий продуктов.
            models (list): Список моделей товаров.
            colors (list): Список цветов.

        Returns:
            set: Множество уникальных названий товаров.
        Raises:
            ValueError: если в аргументы передан хотя бы один пустой список
        """
        logger.info("Генерация уникальных названий товаров...")
        product_names = set()  # set для контроля уникальности названий

        if not all([products, models, colors]):
            raise ValueError("Списки products/models/colors не определены или пусты")

        while len(product_names) < num_rows:
            product_name = f"{random.choice(colors)} {random.choice(products)} {random.choice(models)}"
            product_names.add(product_name)

        logger.info(
            f"Названия товаров сгенерированы успешно. Уникальных названий: {len(product_names)}"
        )
        return product_names

    @staticmethod
    def _get_ids(cursor: sqlite3.Cursor) -> list[str]:
        """
        Вспомогательная функция для извлечения user_id из таблицы.
        Понадобится для осуществления constraints
        """
        cursor.execute("SELECT id FROM users")
        user_ids = [row[0] for row in cursor.fetchall()]
        return user_ids

    @staticmethod
    def _get_product_ids(cursor: sqlite3.Cursor) -> list[str]:
        """
        Вспомогательная функция для извлечения product_id из таблицы.
        Понадобится для осуществления constraints
        """
        cursor.execute("SELECT id FROM products")
        product_ids = [row[0] for row in cursor.fetchall()]
        return product_ids


class SQLiteGenerator(DataGenerator):
    def __init__(self, gen_config: dict):
        super().__init__(gen_config["sqlite"])
        self.word_lists = self.cfg.get("word_lists", {})

    def _generate_users(self, num_rows: int) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы users.

        Args:
            num_rows (int): Количество генерируемых записей.

        Returns:
            list[tuple[Any]]: Список сгенерированных записей.
        """

        logger.info(f"Генерация данных для таблицы `users`...")
        data = []

        for _ in range(num_rows):
            name = self._inject_anomaly(self.faker.name(), "TEXT")
            age = self._inject_anomaly(random.randint(18, 90), "INTEGER")
            phone = self._inject_anomaly(self.faker.phone_number(), "TEXT")
            email = self._inject_anomaly(self.faker.email(), "TEXT")
            country = self._inject_anomaly(self.faker.country(), "TEXT")
            reg_date = self._inject_anomaly(
                (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime(
                    "%Y-%m-%d"
                ),
                "DATE",
            )
            data.append((None, name, age, phone, email, country, reg_date))

        logger.info("Генерация данных для таблицы `users` завершена!")
        return data

    def _generate_products(self, num_rows: int) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы products.

        Args:
            num_rows (int): Количество генерируемых записей.

        Returns:
            list[tuple[Any]]: Список сгенерированных записей.
        """

        logger.info(f"Генерация данных для таблицы `products`...")

        data = []
        product_names = self._get_product_names(
            num_rows,
            products=self.word_lists.get("products", []),
            models=self.word_lists.get("models", []),
            colors=self.word_lists.get("colors", []),
        )

        for _ in range(num_rows):
            name = self._inject_anomaly(
                product_names.pop(),
                "TEXT",
            )
            category = self._inject_anomaly(
                random.choice(self.word_lists.get("categories")), "TEXT"
            )
            price = self._inject_anomaly(round(random.uniform(1, 2500), 2), "REAL")
            data.append((None, name, category, price))

        logger.info("Генерация данных для таблицы `products` завершена!")
        return data

    def _generate_logs(self, num_rows: int) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы logs.

        Args:
            num_rows (int): Количество генерируемых записей.

        Returns:
            list[tuple[Any]]: Список сгенерированных записей.
        """
        logger.info("Генерация данных для таблицы logs...")

        data = []
        severities = list(self.word_lists["log_messages"].keys())
        log_messages = self.word_lists["log_messages"]

        for _ in range(num_rows):
            severity = random.choice(severities)
            message = self._inject_anomaly(
                random.choice(log_messages.get(severity, ["default error"])),
                "TEXT",
            )
            timestamp = self._inject_anomaly(
                (datetime.now() - timedelta(days=random.randint(0, 365))).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "DATE",
            )
            severity = self._inject_anomaly(severity, "TEXT")
            data.append((None, severity, message, timestamp))

        logger.info("Генерация данных для таблицы `logs` завершена!")
        return data

    def _generate_transactions(
        self, num_rows: int, user_ids: list[str]
    ) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы transactions.

        Args:
            num_rows (int): Количество генерируемых записей.
            user_ids (list[str]): Список пользовательских id.

        Returns:
            list[tuple[Any]]: Список сгенерированных записей.
        """

        logger.info("Генерация данных для таблицы `transactions`...")

        data = []
        unique = set()  # Для контроля уникальности композитных PK
        statuses = self.word_lists.get("status", {})
        transaction_desc = self.word_lists.get("transaction_desc", {})

        for _ in range(num_rows):
            while True:
                uid = random.choice(user_ids)
                ts = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if (uid, ts) not in unique:
                    unique.add((uid, ts))
                    break
            amount = self._inject_anomaly(
                round(random.uniform(10.0, 1000.0), 2), "REAL"
            )
            description = self._inject_anomaly(
                random.choice(transaction_desc),
                "TEXT",
            )
            status = self._inject_anomaly(
                random.choice(statuses),
                "TEXT",
            )

            data.append((uid, amount, ts, description, status))

        logger.info("Генерация данных для таблицы `transactions` завершена!")
        return data

    def _generate_user_actions(
        self, num_rows: int, user_ids: list[str]
    ) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы transactions.

        Args:
            num_rows (int): Количество генерируемых записей.
            user_ids (list[str]): Список пользовательских id.

        Returns:
            list[tuple[Any]]: Список сгенерированных записей.
        """
        logger.info("Генерация данных для таблицы `user_actions`")

        data = []
        unique = set()  # Для контроля уникальности композитных PK
        actions = self.word_lists.get("actions", {})

        for _ in range(num_rows):
            while True:
                uid = random.choice(user_ids)
                ts = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if (uid, ts) not in unique:
                    unique.add((uid, ts))
                    break
            action = self._inject_anomaly(random.choice(actions), "TEXT")
            data.append((uid, action, ts))

        logger.info("Генерация данных для таблицы `user_actions` завершена!")
        return data

    def _generate_orders(
        self, num_rows: int, user_ids: list[str], product_ids: list[str]
    ) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы transactions.

        Args:
            num_rows (int): Количество генерируемых записей.
            user_ids (list[str]): Список пользовательских id.
            product_ids (list[str]): Список id товаров.

        Returns:
            list[tuple[Any]]: Список сгенерированных данных.
        """
        logger.info("Генерация данных для таблицы `orders`...")

        data = []
        order_stats = self.word_lists.get("order_status", {})

        for _ in range(num_rows):
            uid = random.choice(user_ids)
            pid = random.choice(product_ids)
            date = self._inject_anomaly(
                (datetime.now() - timedelta(days=random.randint(0, 365))).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "DATE",
            )
            status = self._inject_anomaly(
                random.choice(order_stats),
                "TEXT",
            )
            amount = self._inject_anomaly(random.randint(1, 2500), "REAL")
            data.append((None, uid, pid, date, status, amount))

        logger.info("Генерация данных для таблицы `orders` завершена!")
        return data

    def populate_table(
        self, table_name: str, data: list[tuple[Any]], cursor: sqlite3.Cursor
    ) -> None:
        """
        Метод для заполнения таблицы данными.

        Args:
            table_name (str): Имя таблицы.
            data (list[tuple[Any]]): Список сгенерированных данных.
            cursor (sqlite3.Cursor): Курсор для выполнения запросов к БД.
        """

        logger.info(f"Заполнение таблицы {table_name}...")

        for row in data:
            placeholders = ", ".join(["?" for _ in row])
            query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(query, row)

        logger.info(f"Таблица {table_name} успешно заполнена!")

    def create_db(self, db_name: str = "", db_path: str = "") -> None:
        """
        Создаёт базу данных SQLite с таблицами согласно конфигурации,
        генерирует синтетические данные и заполняет ими таблицы.

        Args:
            db_name (str): Имя БД SQLite.
        """
        if not db_name:
            db_name = self.cfg["db_name"]
            logger.info(
                f"Имя БД не указано. Будет использовано имя по умолчанию: {db_name}"
            )

        if not db_path:
            db_path = self.cfg["db_path"]
            logger.info(
                f"Путь для сохранения файла БД не указан. Будет использован путь по умолчанию {db_path}"
            )

        logger.info(f"Приступаю к созданию базы данных {db_name}")

        if not os.path.exists(db_path):
            os.mkdir(db_path)
            logger.info(f"Создана директория {db_path}")

        full_path = os.path.join(db_path, db_name)

        with sqlite3.connect(full_path) as conn:
            logger.info(f"Создана БД SQLite `{db_name}`. Приступаю к созданию таблиц.")

            cursor = conn.cursor()
            table_names = []
            count = 0

            for table in self.cfg["tables"]:
                table_name = table["name"]
                columns = table["columns"]
                constraints = table.get("constraints", [])
                num_rows = table.get("num_rows", 1000)

                table_names.append(table_name)

                try:
                    logger.info(f"Создаю таблицу {table_name}...")

                    columns_str = ", ".join(
                        [
                            f"{col['name']} {col['type']} {col.get('options', '')}"
                            for col in columns
                        ]
                    )
                    constraints_str = ", ".join(constraints)

                    query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {columns_str}
                        {',' + constraints_str if constraints_str else ''}
                    )
                    """
                    cursor.execute(query)

                    logger.info(f"Таблица {table_name} успешно создана!")
                    count += 1
                    logger.info("Приступаю к заполненю таблицы данными!")

                    method_name = getattr(self, f"_generate_{table_name}", None)
                    if callable(method_name):
                        if table_name in ["transactions", "user_actions"]:
                            user_ids = self._get_ids(cursor=cursor)
                            data = method_name(num_rows=num_rows, user_ids=user_ids)
                        elif table_name in ["orders"]:
                            user_ids = self._get_ids(cursor=cursor)
                            product_ids = self._get_product_ids(cursor=cursor)
                            data = method_name(
                                num_rows=num_rows,
                                user_ids=user_ids,
                                product_ids=product_ids,
                            )
                        else:
                            data = method_name(num_rows)

                        if not isinstance(data, list) or not all(
                            isinstance(row, tuple) for row in data
                        ):
                            raise TypeError(
                                f"_generate_{table_name} должен возвращать list[tuple[Any]], а не {type(data)}"
                            )

                        self.populate_table(table_name, data, cursor)

                except sqlite3.Error as e:
                    logger.error(f"Ошибка при создании таблицы {table_name}: {e}")

            logger.info(
                f"База данных `{db_name}` успешно создана: {full_path}.\n"
                f"Сформировано {count} таблиц: {', '.join(table_names)}.\n"
                f"Все таблицы успешно заполнены данными."
            )


class CSVGenerator(DataGenerator):
    def __init__(self, gen_config: dict):
        super().__init__(gen_config)

    def generate_csv(self, csv_name: str) -> None:
        """Метод для генерации синтетических данных для CSV файла.
        Параметры генерации указаны в yaml файле.

        Args:
            csv_name (str): Имя генерируемого CSV файла.
        """
        try:
            logger.info("Генерация данных и запись в CSV файл.")
            csv_path = os.path.join(self.cfg["data_destinations"]["csv"], csv_name)

            with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                headers = self.cfg["csv_config"]["headers"]
                writer.writerow(headers)

                for idx in range(1, self.cfg["csv_config"]["num_rows"] + 1):
                    raw_row = {
                        "id": idx,
                        "name": self.faker.name(),
                        "age": random.randint(18, 65),
                        "phone": self.faker.phone_number(),
                        "email": self.faker.email(),
                        "country": self.faker.country(),
                        "hire_date": (
                            datetime.now() - timedelta(days=random.randint(0, 3650))
                        ).strftime("%Y-%m-%d"),
                        "department": random.choice(
                            ["HR", "Engineering", "Sales", "Marketing"]
                        ),
                        "job_title": random.choice(
                            [
                                "Manager",
                                "Engineer",
                                "Analyst",
                                "Specialist",
                                "Consultant",
                                "Coordinator",
                                "Director",
                            ]
                        ),
                        "salary": random.randint(30000, 150000),
                    }

                    dirty_row = [
                        self._inject_anomaly(raw_row[col], col) for col in headers
                    ]
                    writer.writerow(dirty_row)

            logger.info(f"Данные успешно сохранены в файл '{csv_name}'.")
        except Exception as e:
            logger.error(f"Ошибка при генерации CSV: {e}")
            raise


class MongoGenerator(DataGenerator):
    def __init__(self, gen_config: dict):
        super().__init__(gen_config)

    def generate(self):
        pass
