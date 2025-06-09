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


class SQLiteGenerator(DataGenerator):
    def __init__(self, gen_config: dict):
        super().__init__(gen_config["sqlite"])

    @staticmethod
    def _get_ids(cursor: sqlite3.Cursor):
        """
        Вспомогательная функция для извлечения user_id из таблицы.
        Понадобится для осуществления constraints
        """
        user_ids = [row[0] for row in cursor.fetchall()]
        return user_ids

    @staticmethod
    def _get_product_names(num_rows: int, cfg: dict) -> set:
        """
        Генерация уникальных названий товаров:
        случайные комбинации названий, модели и цвета.

        Args:
            num_rows (int): Количество строк для генерации.

        Returns:
            set: Множество уникальных названий товаров.
        """
        logger.info("Генерация уникальных названий товаров...")
        product_names = set()  # set для контроля уникальности названий

        # Получаем списки из конфигурации
        products = cfg["products"]
        models = cfg["models"]
        colors = cfg["colors"]

        while len(product_names) < num_rows:
            product_name = f"{random.choice(colors)} {random.choice(products)} {random.choice(models)}"
            product_names.add(product_name)

        logger.info(
            f"Названия товаров сгенерированы успешно. Уникальных названий: {len(product_names)}"
        )
        return product_names

    def _generate_data(
        self, table_name: str, num_rows: int, cursor: sqlite3.Cursor
    ) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы на основе имени и количества строк.

        Args:
            table_name (str): Имя таблицы.
            num_rows (int): Количество генерируемых записей.
            cursor: Объект курсора SQLite для выполнения SQL-запросов к базе данных

        Returns:
            list: Список сгенерированных данных.
        """

        logger.info(f"Генерация данных для таблицы '{table_name}'...")
        data = []

        match table_name:
            case "users":
                for _ in range(num_rows):
                    name = self._inject_anomaly(self.faker.name(), "TEXT")
                    age = self._inject_anomaly(random.randint(18, 90), "INTEGER")
                    phone = self._inject_anomaly(self.faker.phone_number(), "TEXT")
                    email = self._inject_anomaly(self.faker.email(), "TEXT")
                    country = self._inject_anomaly(self.faker.country(), "TEXT")
                    reg_date = self._inject_anomaly(
                        (
                            datetime.now() - timedelta(days=random.randint(0, 3650))
                        ).strftime("%Y-%m-%d"),
                        "DATE",
                    )
                    data.append((None, name, age, phone, email, country, reg_date))

            case "products":
                product_names = self._get_product_names(num_rows, sqlite_cfg=sqlite_cfg)
                for _ in range(num_rows):
                    name = self._inject_anomaly(product_names.pop(), "TEXT")
                    category = self._inject_anomaly(
                        random.choice(self.cfg["word_lists"]["categories"]),
                        "TEXT",
                    )
                    price = self._inject_anomaly(
                        round(random.uniform(1, 2500), 2), "REAL"
                    )
                    data.append((None, name, category, price))

            case "logs":
                for _ in range(num_rows):
                    severity = self._inject_anomaly(
                        random.choice(["INFO", "WARNING", "ERROR", "CRITICAL"]),
                        "TEXT",
                    )
                    message = self._inject_anomaly(
                        random.choice(
                            self.cfg["word_lists"]["log_messages"].get(
                                severity, ["default error"]
                            )
                        ),
                        "TEXT",
                    )
                    timestamp = self._inject_anomaly(
                        (
                            datetime.now() - timedelta(days=random.randint(0, 365))
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                        "DATE",
                    )
                    data.append((None, severity, message, timestamp))

            case "transactions":
                cursor.execute("SELECT id FROM users")
                user_ids = [row[0] for row in cursor.fetchall()]
                unique = set()

                for _ in range(num_rows):
                    while True:
                        uid = random.choice(user_ids)
                        ts = (
                            datetime.now() - timedelta(days=random.randint(0, 365))
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        if (uid, ts) not in unique:
                            unique.add((uid, ts))
                            break
                    amount = self._inject_anomaly(
                        round(random.uniform(10.0, 1000.0), 2), "REAL"
                    )
                    description = self._inject_anomaly(
                        random.choice(self.cfg["word_lists"]["transaction_desc"]),
                        "TEXT",
                    )
                    status = self._inject_anomaly(
                        random.choice(["PENDING", "COMPLETED", "FAILED", "CANCELLED"]),
                        "TEXT",
                    )
                    data.append((uid, amount, ts, description, status))

            case "user_actions":
                cursor.execute("SELECT id FROM users")
                user_ids = [row[0] for row in cursor.fetchall()]
                unique = set()

                for _ in range(num_rows):
                    while True:
                        uid = random.choice(user_ids)
                        ts = (
                            datetime.now() - timedelta(days=random.randint(0, 365))
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        if (uid, ts) not in unique:
                            unique.add((uid, ts))
                            break
                    action = self._inject_anomaly(
                        random.choice(self.cfg["word_lists"]["actions"]), "TEXT"
                    )
                    data.append((uid, action, ts))

            case "orders":
                cursor.execute("SELECT id FROM users")
                user_ids = [row[0] for row in cursor.fetchall()]
                cursor.execute("SELECT id FROM products")
                product_ids = [row[0] for row in cursor.fetchall()]

                for _ in range(num_rows):
                    uid = random.choice(user_ids)
                    pid = random.choice(product_ids)
                    date = self._inject_anomaly(
                        (
                            datetime.now() - timedelta(days=random.randint(0, 365))
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                        "DATE",
                    )
                    status = self._inject_anomaly(
                        random.choice(
                            [
                                "PENDING",
                                "SHIPPED",
                                "DELIVERED",
                                "RETURNED",
                                "COMPLETED",
                                "CANCELLED",
                            ]
                        ),
                        "TEXT",
                    )
                    amount = self._inject_anomaly(random.randint(1, 2500), "REAL")
                    data.append((None, uid, pid, date, status, amount))

            case _:
                logger.warning(f"Неизвестная таблица '{table_name}'")
                return []

        return data

    def generate_db(self, db_name: str | None) -> None:
        """
        Метод генерации синтетических данных для SQLite DB.
        Параметры генерации указаны в yaml файле.

        Args:
            db_name (str): Имя БД SQLite. По умолчанию 'synthetic_sqlite_db'
        """

        conn = None
        try:
            logger.info("Подключение к БД SQLite.")
            # Проверяем, существует ли директория для сохранения файла
            path = self.cfg["db_path"]

            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)
                logging.info(f"Создана директория {path}")

            if db_name is None:
                db_name = self.cfg["sqlite_config"]["db_name"]
                logger.info(
                    f"Не передано название БД - db_name.\nБудет использовано значение по умолчанию - {sqlite_cfg['db_name']}"
                )

            db_path = os.path.join(path, str(db_name))

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            logger.info(f"Подключение к БД SQLite успешно.")

            logger.info("Создание таблиц и заполнение их данными.")
            # Создание таблиц на основе конфигурации
            for table in self.cfg["sqlite_config"]["sqlite_tables"]:
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
                logger.info(f"Таблица '{table_name}' успешно создана.")

            for table in self.cfg["sqlite_config"]["sqlite_tables"]:
                table_name = table["name"]
                num_rows = table["num_rows"]
                logger.info(f"Генерация {num_rows} строк для таблицы '{table_name}'.")
                data = _generate_data(table_name, num_rows, cursor)
                logger.info(f"Генерация данных для таблицы '{table_name}' завершена.")

                logger.info(f"Вставка данных в таблицу '{table_name}'...")
                if not data:
                    logger.warning(
                        f"Нет данных для генерации в таблице '{table_name}'."
                    )
                    continue

                placeholders = ", ".join(["?"] * len(data[0]))
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.executemany(insert_query, data)
                logger.info(
                    f"Данные для таблицы '{table_name}' успешно сгенерированы и вставлены."
                )

            conn.commit()
            logger.info("Данные успешно сохранены в БД SQLite.")
        except sqlite3.Error as e:
            logger.error(f"Ошибка при работе с БД SQLite {e}")
        finally:
            if conn:
                conn.close()
                logger.info("Соединение с БД SQLite закрыто.")


class CSVGenerator(DataGenerator):
    def __init__(self, gen_config: dict):
        super().__init__(gen_config)

    def generate_csv(self, csv_name: str) -> None:
        """Метод для генерации синтетических данных для CSV файла.
        Параметры генерации указаны в yaml файле.

        Args:
            csv_name (str): Имя генерируемого CSV файла.
        """

        def _inject_dirty_data(value, column_name: str):
            """Вставляет грязные данные с некоторой вероятностью."""

            if random.random() > 0.1:
                return value

            strategies = [
                lambda: "",  # Пустое значение
                lambda: "###ERROR###",  # Ошибочное значение
                lambda: str(value) * 10,  # Аномально длинное значение
                lambda: None,  # None как ошибка
            ]

            if column_name in ["age", "salary"]:
                strategies.append(
                    lambda: -random.randint(1, 100)
                )  # Отрицательное число

            return random.choice(strategies)()

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
                        _inject_dirty_data(raw_row[col], col) for col in headers
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
