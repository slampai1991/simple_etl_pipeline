import csv
import sqlite3
import random
import logging
import os
from datetime import datetime, timedelta
from typing import Any
import faker
import pandas as pd
from pymongo import MongoClient


logger = logging.getLogger(__name__)


# class DataGenerator:
#     """
#     Генератор синтетических данных разных типов (логи, транзакции etc.)
#     и разных форматов (CSV, SQLite DB etc.)
#     В зависимости от конфигурации, он может генерировать данные в разных форматах.
#     Сохраняет данные в заданных форматах локально.
#     При необходимости можно дописать нужные методы, или изменить логику существующих.
#     """

#     def __init__(self, config: dict):
#         self.cfg = config  # Конфигурация для генерации данных
#         self.faker = faker.Faker()  # Генератор случайных данных

#     def generate_sqlite(self, db_name: str | None = None) -> None:
#         """Метод генерации синтетических данных для SQLite DB.
#         Параметры генерации указаны в yaml файле.

#         Args:
#             db_name (str): Имя БД SQLite
#         """

#         def _get_product_names(num_rows: int) -> set:
#             """
#             Генерация уникальных названий товаров:
#             случайные комбинации названий, модели и цвета.

#             Args:
#                 num_rows (int): Количество строк для генерации.

#             Returns:
#                 set: Множество уникальных названий товаров.
#             """
#             # Товары могут не совпадать с категориями, ведь это синтетические данные
#             # В данном случае, просто совмещаем названия, модели и цвета в одну строку

#             logger.info("Генерация уникальных названий товаров...")
#             product_names = (
#                 set()
#             )  # Используем set для соблюдения уникальности product name

#             # Получаем списки из конфигурации
#             products = self.cfg["word_lists"]["products"]
#             models = self.cfg["word_lists"]["models"]
#             colors = self.cfg["word_lists"]["colors"]

#             while len(product_names) < num_rows:
#                 product_name = f"{random.choice(colors)} {random.choice(products)} {random.choice(models)}"
#                 product_names.add(product_name)

#             logger.info(
#                 f"Названия товаров сгенерированы успешно. Уникальных названий: {len(product_names)}"
#             )
#             return product_names

#         def _generate_data(table_name: str, num_rows: int, cursor) -> list:
#             """Генерация "грязных" данных для таблицы на основе имени и количества строк.

#             Args:
#                 table_name (str): Имя таблицы.
#                 num_rows (int): Количество генерируемых записей.
#                 cursor: Объект курсора SQLite для выполнения SQL-запросов к базе данных

#             Returns:
#                 list: Список сгенерированных данных.
#             """

#             def _maybe_dirty(value, dtype: str, nullable=True):
#                 """Инструмент для внедрения аномалий в данные.

#                 Args:
#                     value (any): Значение, которое будет внедрено в данные.
#                     dtype (str): Тип данных.
#                     nullable (bool, optional): Допуск значения None. По умолчанию True.

#                 Returns:
#                     any: Значение с аномалиями или None.
#                 """
#                 if random.random() > 0.1:
#                     return value  # 90% данных остаются корректными

#                 # 10% – грязные
#                 match dtype:
#                     case "TEXT":
#                         return random.choice(
#                             ["", None if nullable else "", "!!!", "123", "\x00"]
#                         )
#                     case "INTEGER":
#                         return (
#                             random.choice([None, -999, "NaN", 9999999999])
#                             if nullable
#                             else random.choice([-1, 200, "oops"])
#                         )
#                     case "REAL":
#                         return random.choice([None, -1.234, "abc", 99999999.9])
#                     case "DATE":
#                         return random.choice(
#                             ["not-a-date", "31-31-2020", None, "2050-01-01"]
#                         )
#                     case _:
#                         logger.info(
#                             f"Неизвестный тип данных '{dtype}'. Используется 'TEXT'."
#                         )

#                 return value

#             data = []

#             match table_name:
#                 case "users":
#                     for _ in range(num_rows):
#                         name = _maybe_dirty(self.faker.name(), "TEXT")
#                         age = _maybe_dirty(random.randint(1, 99), "INTEGER")
#                         phone = _maybe_dirty(self.faker.phone_number(), "TEXT")
#                         email = _maybe_dirty(self.faker.email(), "TEXT")
#                         country = _maybe_dirty(self.faker.country(), "TEXT")
#                         reg_date = _maybe_dirty(
#                             (
#                                 datetime.now() - timedelta(days=random.randint(0, 3650))
#                             ).strftime("%Y-%m-%d"),
#                             "DATE",
#                         )
#                         data.append((None, name, age, phone, email, country, reg_date))

#                 case "products":
#                     product_names = _get_product_names(num_rows)
#                     for _ in range(num_rows):
#                         name = _maybe_dirty(product_names.pop(), "TEXT")
#                         category = _maybe_dirty(
#                             random.choice(self.cfg["word_lists"]["categories"]),
#                             "TEXT",
#                         )
#                         price = _maybe_dirty(round(random.uniform(1, 2500), 2), "REAL")
#                         data.append((None, name, category, price))

#                 case "logs":
#                     for _ in range(num_rows):
#                         severity = _maybe_dirty(
#                             random.choice(["INFO", "WARNING", "ERROR", "CRITICAL"]),
#                             "TEXT",
#                         )
#                         message = _maybe_dirty(
#                             random.choice(
#                                 self.cfg["word_lists"]["log_messages"].get(
#                                     severity, ["default error"]
#                                 )
#                             ),
#                             "TEXT",
#                         )
#                         timestamp = _maybe_dirty(
#                             (
#                                 datetime.now() - timedelta(days=random.randint(0, 365))
#                             ).strftime("%Y-%m-%d %H:%M:%S"),
#                             "DATE",
#                         )
#                         data.append((None, severity, message, timestamp))

#                 case "transactions":
#                     cursor.execute("SELECT id FROM users")
#                     user_ids = [row[0] for row in cursor.fetchall()]
#                     unique = set()

#                     for _ in range(num_rows):
#                         while True:
#                             uid = random.choice(user_ids)
#                             ts = (
#                                 datetime.now() - timedelta(days=random.randint(0, 365))
#                             ).strftime("%Y-%m-%d %H:%M:%S")
#                             if (uid, ts) not in unique:
#                                 unique.add((uid, ts))
#                                 break
#                         amount = _maybe_dirty(
#                             round(random.uniform(10.0, 1000.0), 2), "REAL"
#                         )
#                         description = _maybe_dirty(
#                             random.choice(self.cfg["word_lists"]["transaction_desc"]),
#                             "TEXT",
#                         )
#                         status = _maybe_dirty(
#                             random.choice(
#                                 ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]
#                             ),
#                             "TEXT",
#                         )
#                         data.append((uid, amount, ts, description, status))

#                 case "user_actions":
#                     cursor.execute("SELECT id FROM users")
#                     user_ids = [row[0] for row in cursor.fetchall()]
#                     unique = set()

#                     for _ in range(num_rows):
#                         while True:
#                             uid = random.choice(user_ids)
#                             ts = (
#                                 datetime.now() - timedelta(days=random.randint(0, 365))
#                             ).strftime("%Y-%m-%d %H:%M:%S")
#                             if (uid, ts) not in unique:
#                                 unique.add((uid, ts))
#                                 break
#                         action = _maybe_dirty(
#                             random.choice(self.cfg["word_lists"]["actions"]), "TEXT"
#                         )
#                         data.append((uid, action, ts))

#                 case "orders":
#                     cursor.execute("SELECT id FROM users")
#                     user_ids = [row[0] for row in cursor.fetchall()]
#                     cursor.execute("SELECT id FROM products")
#                     product_ids = [row[0] for row in cursor.fetchall()]

#                     for _ in range(num_rows):
#                         uid = random.choice(user_ids)
#                         pid = random.choice(product_ids)
#                         date = _maybe_dirty(
#                             (
#                                 datetime.now() - timedelta(days=random.randint(0, 365))
#                             ).strftime("%Y-%m-%d %H:%M:%S"),
#                             "DATE",
#                         )
#                         status = _maybe_dirty(
#                             random.choice(
#                                 [
#                                     "PENDING",
#                                     "SHIPPED",
#                                     "DELIVERED",
#                                     "RETURNED",
#                                     "COMPLETED",
#                                     "CANCELLED",
#                                 ]
#                             ),
#                             "TEXT",
#                         )
#                         amount = _maybe_dirty(random.randint(1, 2500), "REAL")
#                         data.append((None, uid, pid, date, status, amount))

#                 case _:
#                     logger.warning(f"Неизвестная таблица '{table_name}'")
#                     return []

#             return data

#         conn = None
#         try:
#             logger.info("Подключение к БД SQLite.")
#             # Проверяем, существует ли директория для сохранения файла
#             path = self.cfg["sqlite_config"]["db_path"]

#             if not os.path.exists(path):
#                 os.makedirs(path, exist_ok=True)
#                 logging.info(f"Создана директория {path}")

#             if db_name is None:
#                 db_name = self.cfg["sqlite_config"]["db_name"]
#                 logger.info(
#                     f"Не передано название БД - db_name.\nБудет использовано значение по умолчанию - {self.cfg['sqlite_config']['db_name']}"
#                 )

#             db_path = os.path.join(path, str(db_name))

#             conn = sqlite3.connect(db_path)
#             cursor = conn.cursor()
#             logger.info(f"Подключение к БД SQLite успешно.")

#             logger.info("Создание таблиц и заполнение их данными.")
#             # Создание таблиц на основе конфигурации
#             for table in self.cfg["sqlite_config"]["sqlite_tables"]:
#                 table_name = table["name"]
#                 columns = ", ".join(
#                     [
#                         f"{key} {value}"
#                         for column in table["columns"]
#                         for key, value in column.items()
#                     ]
#                 )
#                 constraints = ", ".join(table.get("constraints", []))
#                 create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}{', ' + constraints if constraints else ''});"
#                 cursor.execute(create_table_query)
#                 logger.info(f"Таблица '{table_name}' успешно создана.")

#             for table in self.cfg["sqlite_config"]["sqlite_tables"]:
#                 table_name = table["name"]
#                 num_rows = table["num_rows"]
#                 logger.info(f"Генерация {num_rows} строк для таблицы '{table_name}'.")
#                 data = _generate_data(table_name, num_rows, cursor)
#                 logger.info(f"Генерация данных для таблицы '{table_name}' завершена.")

#                 logger.info(f"Вставка данных в таблицу '{table_name}'...")
#                 if not data:
#                     logger.warning(
#                         f"Нет данных для генерации в таблице '{table_name}'."
#                     )
#                     continue

#                 placeholders = ", ".join(["?"] * len(data[0]))
#                 insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
#                 cursor.executemany(insert_query, data)
#                 logger.info(
#                     f"Данные для таблицы '{table_name}' успешно сгенерированы и вставлены."
#                 )

#             conn.commit()
#             logger.info("Данные успешно сохранены в БД SQLite.")
#         except sqlite3.Error as e:
#             logger.error(f"Ошибка при работе с БД SQLite {e}")
#         finally:
#             if conn:
#                 conn.close()
#                 logger.info("Соединение с БД SQLite закрыто.")

#     def generate_csv(self, csv_name: str) -> None:
#         """Метод для генерации синтетических данных для CSV файла.
#         Параметры генерации указаны в yaml файле.

#         Args:
#             csv_name (str): Имя генерируемого CSV файла.
#         """

#         def _inject_dirty_data(value, column_name: str):
#             """Вставляет грязные данные с некоторой вероятностью."""

#             if random.random() > 0.1:
#                 return value

#             strategies = [
#                 lambda: "",  # Пустое значение
#                 lambda: "###ERROR###",  # Ошибочное значение
#                 lambda: str(value) * 10,  # Аномально длинное значение
#                 lambda: None,  # None как ошибка
#             ]

#             if column_name in ["age", "salary"]:
#                 strategies.append(
#                     lambda: -random.randint(1, 100)
#                 )  # Отрицательное число

#             return random.choice(strategies)()

#         try:
#             logger.info("Генерация данных и запись в CSV файл.")
#             csv_path = os.path.join(self.cfg["data_destinations"]["csv"], csv_name)

#             with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
#                 writer = csv.writer(csvfile)
#                 headers = self.cfg["csv_config"]["headers"]
#                 writer.writerow(headers)

#                 for idx in range(1, self.cfg["csv_config"]["num_rows"] + 1):
#                     raw_row = {
#                         "id": idx,
#                         "name": self.faker.name(),
#                         "age": random.randint(18, 65),
#                         "phone": self.faker.phone_number(),
#                         "email": self.faker.email(),
#                         "country": self.faker.country(),
#                         "hire_date": (
#                             datetime.now() - timedelta(days=random.randint(0, 3650))
#                         ).strftime("%Y-%m-%d"),
#                         "department": random.choice(
#                             ["HR", "Engineering", "Sales", "Marketing"]
#                         ),
#                         "job_title": random.choice(
#                             [
#                                 "Manager",
#                                 "Engineer",
#                                 "Analyst",
#                                 "Specialist",
#                                 "Consultant",
#                                 "Coordinator",
#                                 "Director",
#                             ]
#                         ),
#                         "salary": random.randint(30000, 150000),
#                     }

#                     dirty_row = [
#                         _inject_dirty_data(raw_row[col], col) for col in headers
#                     ]
#                     writer.writerow(dirty_row)

#             logger.info(f"Данные успешно сохранены в файл '{csv_name}'.")
#         except Exception as e:
#             logger.error(f"Ошибка при генерации CSV: {e}")
#             raise

#     # По аналогии можно добавить другие методы: JSON, MongoDB etc.


class DataGenerator:
    def __init__(self, gen_config: dict) -> None:
        self.cfg = gen_config
        self.faker = faker.Faker()
        self.anomaly_cfg = self.cfg.get("anomaly_config", {})

    def inject_anomaly(
        self, value: Any, dtype: str, column_name: str | None = None, nullable: bool = True
    ):
        probability = self.anomaly_cfg.get("probability", 0.1)
        if random.random() > probability:
            return value

        strategies = self.anomaly_cfg.get("strategies", {}).get(dtype, [])

        if dtype == "INTEGER" and column_name in {"age", "salary"}:
            strategies.append(-random.randint(1, 100))

        if strategies:
            return random.choice(strategies)

        return None if nullable else value

    def generate_csv(self, csv_name: str) -> None:
        try:
            logger.info("Генерация CSV данных.")
            csv_path = os.path.join(self.cfg["data_destinations"]["csv"], csv_name)
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)

            headers = self.cfg["csv_config"]["headers"]

            with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
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
                        self.inject_anomaly(
                            raw_row[col],
                            "INTEGER" if isinstance(raw_row[col], int) else "TEXT",
                            col,
                        )
                        for col in headers
                    ]
                    writer.writerow(dirty_row)

            logger.info(f"CSV-файл '{csv_name}' успешно создан.")
        except Exception as e:
            logger.error(f"Ошибка при генерации CSV: {e}")
            raise

    def generate_mongodb(self) -> None:
        try:
            mongo_cfg = self.cfg["mongo_config"]
            client = MongoClient(mongo_cfg["uri"])
            db = client[mongo_cfg["db_name"]]
            logger.info("Подключено к MongoDB.")

            for collection_conf in mongo_cfg["collections"]:
                col_name = collection_conf["name"]
                num_docs = collection_conf["num_docs"]
                schema = collection_conf["schema"]
                docs = []

                for _ in range(num_docs):
                    doc = {}
                    for field, dtype in schema.items():
                        value = self._generate_value_by_type(dtype)
                        value = self.inject_anomaly(value, dtype, field)
                        doc[field] = value
                    docs.append(doc)

                db[col_name].insert_many(docs)
                logger.info(f"Коллекция '{col_name}' заполнена {num_docs} документами.")

            client.close()
            logger.info("MongoDB генерация завершена.")
        except Exception as e:
            logger.error(f"Ошибка MongoDB: {e}")
            raise

    def _generate_value_by_type(self, dtype: str):
        match dtype:
            case "TEXT":
                return self.faker.word()
            case "EMAIL":
                return self.faker.email()
            case "INTEGER":
                return random.randint(1, 100)
            case "REAL":
                return round(random.uniform(0.0, 1000.0), 2)
            case "DATE":
                return (
                    datetime.now() - timedelta(days=random.randint(0, 3650))
                ).strftime("%Y-%m-%d")
            case "PHONE":
                return self.faker.phone_number()
            case _:
                return None
