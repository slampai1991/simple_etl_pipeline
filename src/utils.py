import csv
import sqlite3
import random
import faker
import logging
import os
import pandas as pd
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)


class DataGenerator:
    """
    Генератор синтетических данных разных типов (логи, транзакции и т.д.)
    и разных форматов (CSV, SQLite DB и т.д.)
    В зависимости от конфигурации, он может генерировать данные в разных форматах.
    Сохраняет данные в заданных форматах локально.
    При необходимости можешь дописать нужные методы, или изменить логику существующих.
    В любом случае, не забудь внести изменения и в конфиг файл.
    """

    def __init__(self, config: dict):
        self.config = config  # Конфигурация для генерации данных
        self.faker = faker.Faker()  # Генератор случайных данных

    def generate_sqlite(self, db_name: str | None = None) -> None:
        """Метод генерации синтетических данных для SQLite DB.
        Параметры генерации указаны в yaml файле.

        Args:
            db_name (str): Имя БД SQLite
        """

        def _get_product_names(num_rows: int) -> set:
            """Генерация уникальных названий товаров: случайные комбинации названий товаров, модели и цвета.

            Args:
                num_rows (int): Количество строк для генерации.

            Returns:
                set: Множество уникальных названий товаров.
            """
            # Товары могут не совпадать с категориями, ведь это синтетические данные
            # В данном случае, просто совмещаем названия, модели и цвета в одну строку

            logger.info("Генерация уникальных названий товаров...")
            product_names = (
                set()
            )  # Используем set для соблюдения уникальности product name

            # Получаем списки из конфигурации
            products = self.config["word_lists"]["products"]
            models = self.config["word_lists"]["models"]
            colors = self.config["word_lists"]["colors"]

            while len(product_names) < num_rows:
                product_name = f"{random.choice(colors)} {random.choice(products)} {random.choice(models)}"
                product_names.add(product_name)

            logger.info(
                f"Названия товаров сгенерированы успешно. Уникальных названий: {len(product_names)}"
            )
            return product_names

        def _generate_data(table_name: str, num_rows: int, cursor) -> list:
            """Генерация "грязных" данных для таблицы на основе имени и количества строк.

            Args:
                table_name (str): Имя таблицы.
                num_rows (int): Количество генерируемых записей.
                cursor: Объект курсора SQLite для выполнения SQL-запросов к базе данных

            Returns:
                list: Список сгенерированных данных.
            """

            def _maybe_dirty(value, dtype: str, nullable=True):
                """Инструмент для внедрения аномалий в данные.

                Args:
                    value (any): Значение, которое будет внедрено в данные.
                    dtype (str): Тип данных.
                    nullable (bool, optional): Допуск значения None. По умолчанию True.

                Returns:
                    any: Значение с аномалиями или None.
                """
                if random.random() > 0.1:
                    return value  # 90% данных остаются корректными

                # 10% – грязные
                match dtype:
                    case "TEXT":
                        return random.choice(
                            ["", None if nullable else "", "!!!", "123", "\x00"]
                        )
                    case "INTEGER":
                        return (
                            random.choice([None, -999, "NaN", 9999999999])
                            if nullable
                            else random.choice([-1, 200, "oops"])
                        )
                    case "REAL":
                        return random.choice([None, -1.234, "abc", 99999999.9])
                    case "DATE":
                        return random.choice(
                            ["not-a-date", "31-31-2020", None, "2050-01-01"]
                        )
                    case _:
                        logger.info(
                            f"Неизвестный тип данных '{dtype}'. Используется 'TEXT'."
                        )

                return value

            data = []

            match table_name:
                case "users":
                    for _ in range(num_rows):
                        name = _maybe_dirty(self.faker.name(), "TEXT")
                        age = _maybe_dirty(random.randint(1, 99), "INTEGER")
                        phone = _maybe_dirty(self.faker.phone_number(), "TEXT")
                        email = _maybe_dirty(self.faker.email(), "TEXT")
                        country = _maybe_dirty(self.faker.country(), "TEXT")
                        reg_date = _maybe_dirty(
                            (
                                datetime.now() - timedelta(days=random.randint(0, 3650))
                            ).strftime("%Y-%m-%d"),
                            "DATE",
                        )
                        data.append((None, name, age, phone, email, country, reg_date))

                case "products":
                    product_names = _get_product_names(num_rows)
                    for _ in range(num_rows):
                        name = _maybe_dirty(product_names.pop(), "TEXT")
                        category = _maybe_dirty(
                            random.choice(self.config["word_lists"]["categories"]),
                            "TEXT",
                        )
                        price = _maybe_dirty(round(random.uniform(1, 2500), 2), "REAL")
                        data.append((None, name, category, price))

                case "logs":
                    for _ in range(num_rows):
                        severity = _maybe_dirty(
                            random.choice(["INFO", "WARNING", "ERROR", "CRITICAL"]),
                            "TEXT",
                        )
                        message = _maybe_dirty(
                            random.choice(
                                self.config["word_lists"]["log_messages"].get(
                                    severity, ["default error"]
                                )
                            ),
                            "TEXT",
                        )
                        timestamp = _maybe_dirty(
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
                        amount = _maybe_dirty(
                            round(random.uniform(10.0, 1000.0), 2), "REAL"
                        )
                        description = _maybe_dirty(
                            random.choice(
                                self.config["word_lists"]["transaction_desc"]
                            ),
                            "TEXT",
                        )
                        status = _maybe_dirty(
                            random.choice(
                                ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]
                            ),
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
                        action = _maybe_dirty(
                            random.choice(self.config["word_lists"]["actions"]), "TEXT"
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
                        date = _maybe_dirty(
                            (
                                datetime.now() - timedelta(days=random.randint(0, 365))
                            ).strftime("%Y-%m-%d %H:%M:%S"),
                            "DATE",
                        )
                        status = _maybe_dirty(
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
                        amount = _maybe_dirty(round(random.uniform(1, 2500), 2), "REAL")
                        data.append((None, uid, pid, date, status, amount))

                case _:
                    logger.warning(f"Неизвестная таблица '{table_name}'")
                    return []

            return data

        conn = None
        try:
            logger.info("Подключение к БД SQLite.")
            # Проверяем, существует ли директория для сохранения файла
            destination_path = self.config["data_destinations"]["sqlite"]

            if not os.path.exists(destination_path):
                os.makedirs(destination_path, exist_ok=True)
                logging.info(f"Создана директория {destination_path}")

            if db_name is None:
                db_name = self.config["sqlite_config"]["db_name"]
                logger.info(
                    f"Не передано название БД - db_name.\nБудет использовано значение по умолчанию - {self.config['sqlite_config']['db_name']}"
                )

            db_path = os.path.join(destination_path, str(db_name))

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            logger.info(f"Подключение к БД SQLite успешно.")

            logger.info("Создание таблиц и заполнение их данными.")
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
                logger.info(f"Таблица '{table_name}' успешно создана.")

            for table in self.config["sqlite_tables"]:
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
            csv_path = os.path.join(self.config["data_destinations"]["csv"], csv_name)

            with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                headers = self.config["csv_config"]["headers"]
                writer.writerow(headers)

                for idx in range(1, self.config["csv_config"]["num_rows"] + 1):
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

    # По аналогии можно добавить другие методы: JSON, MongoDB, etc.


class DataValidator:
    """
    Класс для валидации DataFrame-данных:
    - Проверка ссылочной целостности
    - Проверка пользовательских ограничений
    """

    def __init__(self, validation_config: dict):
        self.fk_config = validation_config.get("foreign_keys", {})
        self.constraint_config = validation_config.get("constraints", {}).get(
            "rules", {}
        )
        self.composite_key_config = validation_config.get("composite_keys", {})

    def validate_foreign_keys(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        logger.info("Валидация внешних ключей...")

        for table, fks in self.fk_config.items():
            for fk_col, parent_table in fks.items():
                if table not in df_dict or parent_table not in df_dict:
                    logger.warning(
                        f"Пропущена проверка внешнего ключа: '{table}.{fk_col}' -> '{parent_table}'"
                    )
                    continue

                child_df = df_dict[table]
                parent_ids = set(df_dict[parent_table]["id"])
                before = len(child_df)

                child_df = child_df[child_df[fk_col].isin(parent_ids)]
                after = len(child_df)

                logger.info(
                    f"{table}: удалено {before - after} строк с невалидными '{fk_col}'"
                )
                df_dict[table] = child_df.reset_index(drop=True)

        return df_dict

    def validate_constraints(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        logger.info("Валидация пользовательских ограничений...")

        for table, df in df_dict.items():
            original_len = len(df)

            for column, condition in self.constraint_config.items():
                if column not in df.columns:
                    continue
                try:
                    # Проверка строковых или .isin правил
                    if "str.contains" in condition or ".isin(" in condition:
                        df = df[eval(f"df.{condition}")]
                    else:
                        df = df.query(condition)
                except Exception as e:
                    logger.warning(
                        f"Ошибка при применении ограничения '{condition}' к '{table}.{column}': {e}"
                    )

            logger.info(
                f"{table}: удалено {original_len - len(df)} строк по пользовательским условиям"
            )
            df_dict[table] = df.reset_index(drop=True)

        return df_dict

    def validate_composite_keys(
        self, data: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        comp_keys = self.composite_key_config.get("composite_keys", {})
        for table, keys_list in comp_keys.items():
            df = data.get(table)
            if df is None:
                continue
            for keys in keys_list:
                if df.duplicated(subset=keys).any():
                    logging.warning(
                        f"Нарушение составного ключа {keys} в таблице {table}"
                    )
        return data

    def run_all_validations(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        df_dict = self.validate_foreign_keys(df_dict)
        df_dict = self.validate_composite_keys(df_dict)
        df_dict = self.validate_constraints(df_dict)
        return df_dict


class DataProfiler:
    pass


class DataAuditor:
    pass
