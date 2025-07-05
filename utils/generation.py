"""
Генератор синтетических данных с возможностью внедрения аномалий для SQLite, CSV и MongoDB.

Классы:
-------
1. DataGenerator:
    Базовый класс генерации данных. Предоставляет методы загрязнения данных (_inject_anomaly)
    и утилиты для генерации названий продуктов, извлечения ID из БД и др.

2. SQLiteGenerator(DataGenerator):
    Генератор, создающий SQLite-базу с таблицами, описанными в конфигурации.
    Для каждой таблицы реализован метод генерации данных, в том числе с аномалиями.

    Основные методы:
    - _generate_users
    - _generate_products
    - _generate_logs
    - _generate_transactions
    - _generate_user_actions
    - _generate_orders
    - populate_table
    - create_db

3. CSVGenerator(DataGenerator):
    Генератор, создающий CSV-файл с синтетическими данными по схеме, указанной в конфигурации.

    Основной метод:
    - generate_csv

4. MongoGenerator(DataGenerator):
    Заготовка под генерацию для MongoDB. Метод `generate()` пока не реализован.

Особенности:
------------
- Поддержка загрязнения данных: вероятностная подмена значений, в том числе на None.
- Поддержка зависимости между таблицами (внешние ключи), включая уникальность по составному ключу.
- Конфигурируемая структура таблиц и типы данных.
- Поддержка логирования на всех этапах.

Использование:
--------------
Для запуска необходимо создать объект соответствующего класса (например, SQLiteGenerator)
и вызвать метод `create_db()` или `generate_csv()`.

Пример:
--------
    from utils.generation import SQLiteGenerator

    config = {...}  # Загрузка конфигурации из yaml/json
    sqlite_gen = SQLiteGenerator(config)
    sqlite_gen.create_db()
"""

import csv
import sqlite3
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Union, Optional
import faker

LoggerType = Union[logging.Logger, logging.LoggerAdapter]


class DataGenerator:
    """
    Базовый класс для генерации синтетических данных с возможностью внедрения аномалий.

    Предоставляет общую функциональность для генерации данных, включая методы для внедрения
    аномалий и вспомогательные утилиты.

    Атрибуты:
    ---------
    cfg : dict
        Конфигурация для генерации данных
    faker : Faker
        Объект Faker для генерации случайных данных
    anomaly_cfg : dict
        Конфигурация для внедрения аномалий в данные

    Методы:
    -------
    _inject_anomaly(value: Any, dtype: str, nullable: bool = True) -> Any
        Внедряет аномалии в данные с заданной вероятностью

    _get_product_names(num_rows: int, products: list, models: list, colors: list) -> set[str]
        Генерирует уникальные названия продуктов из комбинаций названий, моделей и цветов

    _get_ids(cursor: sqlite3.Cursor) -> list[str]
        Извлекает ID пользователей из таблицы users

    _get_product_ids(cursor: sqlite3.Cursor) -> list[str]
        Извлекает ID продуктов из таблицы products

    Особенности:
    -----------
    - Поддерживает конфигурируемое внедрение аномалий в данные
    - Предоставляет утилиты для генерации уникальных названий продуктов
    - Обеспечивает доступ к ID для поддержки внешних ключей
    - Использует Faker для генерации реалистичных случайных данных
    """

    def __init__(self, gen_config: dict, logger: Optional[LoggerType] = None):
        self.cfg = gen_config  # Конфигурация для генерации данных
        self.faker = faker.Faker()  # Генератор случайных данных
        self.anomaly_cfg = self.cfg.get(
            "anomaly_cfg", {}
        )  # Конфигурация загрязнения данных
        self.logger: LoggerType = logger or logging.getLogger(__name__)

    def _inject_anomaly(
        self,
        value: Any,
        dtype: str,
        nullable: bool = True,
    ):
        """
        Функция для `загрязнения` генерируемых данных.

        :param Any `value`: Оригинальное значение, которое вероятностно будет заменено на аномальное значение.
        :param str `dtype`: Тип данных значения. Необходим для выбора аномалий.
        :param bool `nullable`: Флаг, указывающий, может ли значение быть `None`. Defaults to True.
        :return `Any`: Загрязненное значение или оригинальное, если не было загрязнения.
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
        num_rows: int,
        products: list,
        models: list,
        colors: list,
        logger: Optional[LoggerType] = None,
    ) -> set[str]:
        """
        Генерация уникальных наименований товаров:
        случайные комбинации названий, модели и цвета.

        :param int `num_rows`: Количество строк для генерации.
        :param list `products`: Список названий продуктов.
        :param list `models`: Список моделей товаров.
        :param list `colors`: Список цветов.
        :param logger: логгер для сообщений (опционально)
        :raises `ValueError`: если в аргументы передан хотя бы один пустой список
        :return `set`: Множество уникальных названий товаров.
        """
        logger = logger or logging.getLogger(__name__)
        if logger:
            logger.info("Генерация уникальных названий товаров...")
        product_names = set()  # set для контроля уникальности названий

        if not all([products, models, colors]):
            raise ValueError("Списки products/models/colors не определены или пусты")

        while len(product_names) < num_rows:
            product_name = f"{random.choice(colors)} {random.choice(products)} {random.choice(models)}"
            product_names.add(product_name)

        if logger:
            logger.info(
                f"Названия товаров сгенерированы успешно. Уникальных названий: {len(product_names)}"
            )
        return product_names

    @staticmethod
    def _get_ids(cursor: sqlite3.Cursor) -> list[str]:
        """
        Вспомогательная функция для извлечения id из таблицы users.
        Понадобится для осуществления constraints.

        :param sqlite3.Cursor `cursor`: объект курсора соединения с БД
        """
        cursor.execute("SELECT id FROM users")
        user_ids = [row[0] for row in cursor.fetchall()]
        return user_ids

    @staticmethod
    def _get_product_ids(cursor: sqlite3.Cursor) -> list[str]:
        """
        Вспомогательная функция для извлечения id из таблицы products.
        Понадобится для осуществления constraints.

        :param sqlite3.Cursor `cursor`: объект курсора соединения с БД
        """
        cursor.execute("SELECT id FROM products")
        product_ids = [row[0] for row in cursor.fetchall()]
        return product_ids


class SQLiteGenerator(DataGenerator):
    """
    SQLiteGenerator - класс для генерации синтетических данных в SQLite базе данных.

    Наследуется от DataGenerator и предоставляет функционал для создания таблиц и заполнения их
    синтетическими данными с возможностью внедрения аномалий.

    Основные методы:
    ---------------
    _generate_users(num_rows: int) -> list[tuple[Any]]
        Генерирует данные для таблицы users с полями:
        id, name, age, phone, email, country, reg_date

    _generate_products(num_rows: int) -> list[tuple[Any]]
        Генерирует данные для таблицы products с полями:
        id, name, category, price

    _generate_logs(num_rows: int) -> list[tuple[Any]]
        Генерирует данные для таблицы logs с полями:
        id, severity, message, timestamp

    _generate_transactions(num_rows: int, user_ids: list[str]) -> list[tuple[Any]]
        Генерирует данные для таблицы transactions с полями:
        user_id, amount, timestamp, description, status

    _generate_user_actions(num_rows: int, user_ids: list[str]) -> list[tuple[Any]]
        Генерирует данные для таблицы user_actions с полями:
        user_id, action, timestamp

    _generate_orders(num_rows: int, user_ids: list[str], product_ids: list[str]) -> list[tuple[Any]]
        Генерирует данные для таблицы orders с полями:
        id, user_id, product_id, date, status, amount

    populate_table(table_name: str, data: list[tuple[Any]], cursor: sqlite3.Cursor) -> None
        Заполняет указанную таблицу сгенерированными данными

    create_db(db_name: Any = "", db_path: Any = "") -> None
        Создает SQLite базу данных с таблицами согласно конфигурации и заполняет их данными

    Особенности:
    -----------
    - Поддерживает внедрение аномалий в данные через метод _inject_anomaly
    - Обеспечивает уникальность составных ключей
    - Поддерживает внешние ключи между таблицами
    - Конфигурируемая структура таблиц через yaml/json конфиг
    - Логирование всех этапов генерации
    """

    def __init__(self, gen_config: dict, logger: Optional[LoggerType] = None):
        super().__init__(gen_config["sqlite"], logger=logger)
        self.word_lists = self.cfg.get("word_lists", {})
        self.logger: LoggerType = logger or logging.getLogger(__name__)

    def _generate_users(self, num_rows: int) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы users.

        :param int `num_rows`: Количество генерируемых записей.
        :return `list[tuple[Any]]`: Список сгенерированных записей.
        """

        self.logger.info(f"Генерация данных для таблицы `users`...")
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

        self.logger.info("Генерация данных для таблицы `users` завершена!")
        return data

    def _generate_products(self, num_rows: int) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы products.

        :param int `num_rows`: Количество генерируемых записей.
        :return `list[tuple[Any]]`: Список сгенерированных записей.
        """

        self.logger.info(f"Генерация данных для таблицы `products`...")

        data = []
        product_names = self._get_product_names(
            num_rows,
            products=self.word_lists.get("products", []),
            models=self.word_lists.get("models", []),
            colors=self.word_lists.get("colors", []),
            logger=self.logger,
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

        self.logger.info("Генерация данных для таблицы `products` завершена!")
        return data

    def _generate_logs(self, num_rows: int) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы logs.

        :param int `num_rows`: Количество генерируемых записей.
        :return `list[tuple[Any]]`: Список сгенерированных записей.
        """
        self.logger.info("Генерация данных для таблицы logs...")

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

        self.logger.info("Генерация данных для таблицы `logs` завершена!")
        return data

    def _generate_transactions(
        self, num_rows: int, user_ids: list[str]
    ) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы transactions.

        :param  int `num_rows`: Количество генерируемых записей.
        :param list[str] `user_ids`: Список пользовательских id.
        :return `list[tuple[Any]]`: Список сгенерированных записей.
        """

        self.logger.info("Генерация данных для таблицы `transactions`...")

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

        self.logger.info("Генерация данных для таблицы `transactions` завершена!")
        return data

    def _generate_user_actions(
        self, num_rows: int, user_ids: list[str]
    ) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы user_actions.

        :param int `num_rows`: Количество генерируемых записей.
        :param list[str] user_ids: Список пользовательских id.
        :return `list[tuple[Any]]`: Список сгенерированных записей.
        """
        self.logger.info("Генерация данных для таблицы `user_actions`")

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

        self.logger.info("Генерация данных для таблицы `user_actions` завершена!")
        return data

    def _generate_orders(
        self, num_rows: int, user_ids: list[str], product_ids: list[str]
    ) -> list[tuple[Any]]:
        """
        Генерация `грязных` данных для таблицы orders.

        :param int `num_rows`: Количество генерируемых записей.
        :param list[str] `user_ids`: Список пользовательских id.
        :param list[str] `product_ids`: Список id товаров.
        :return `list[tuple[Any]]`: Список сгенерированных данных.
        """
        self.logger.info("Генерация данных для таблицы `orders`...")

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
            delivery_address = self._inject_anomaly(self.faker.address(), "TEXT")
            data.append((None, uid, pid, date, status, amount, delivery_address))

        self.logger.info("Генерация данных для таблицы `orders` завершена!")
        return data

    def populate_table(
        self, table_name: str, data: list[tuple[Any]], cursor: sqlite3.Cursor
    ) -> None:
        """
        Метод для заполнения таблицы данными.

        :param str `table_name`: Имя таблицы.
        :param list[tuple[Any]] `data`: Список сгенерированных данных.
        :param sqlite3.Cursor `cursor`: Курсор для выполнения запросов к БД.
        """

        self.logger.info(f"Заполнение таблицы {table_name}...")

        for row in data:
            placeholders = ", ".join(["?" for _ in row])
            query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(query, row)

        self.logger.info(f"Таблица {table_name} успешно заполнена!")

    def create_db(self, db_name: Any = None, db_path: Any = None) -> None:
        """
        Создаёт базу данных SQLite с таблицами согласно конфигурации,
        генерирует синтетические данные и заполняет ими таблицы.

        :param str `db_name`: Имя БД SQLite.
        :param str `db_path`: Путь для сохранения файла БД.
        """
        self.logger.info("Начинаю создание БД...")

        if not db_name:
            db_name = self.cfg["db_name"]
            self.logger.info(
                f"Имя БД не указано. Будет использовано имя по умолчанию: {db_name}"
            )

        if not db_path:
            db_path = self.cfg["db_path"]
            self.logger.info(
                f"Путь для сохранения файла БД не указан. Будет использован путь по умолчанию {db_path}"
            )

        db_path = Path(db_path)
        if not db_path.exists():
            db_path.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Создана директория {db_path}")

        full_path = db_path / db_name

        with sqlite3.connect(full_path) as conn:
            self.logger.info(
                f"Создана БД SQLite `{db_name}`. Приступаю к созданию таблиц."
            )

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
                    self.logger.info(f"Создаю таблицу {table_name}...")

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

                    self.logger.info(f"Таблица {table_name} успешно создана!")
                    count += 1
                    self.logger.info("Приступаю к заполненю таблицы данными!")

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
                    self.logger.error(f"Ошибка при создании таблицы {table_name}: {e}")

            self.logger.info(
                f"База данных `{db_name}` успешно создана: {full_path}.\n"
                f"Сформировано {count} таблиц: {', '.join(table_names)}.\n"
                f"Все таблицы успешно заполнены данными."
            )


class CSVGenerator(DataGenerator):
    def __init__(self, csv_cfg: dict):
        super().__init__(csv_cfg)


class MongoGenerator(DataGenerator):
    def __init__(self, gen_config: dict):
        super().__init__(gen_config)
