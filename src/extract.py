import logging
import os
import sqlite3
import csv
from typing import Any
import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Keyword, DML


logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Класс содержащий методы извлечения данных.
    Пока реализованы только два метода: sqlite и csv экстракторы.
    """

    def __init__(self, config: dict):
        self.cfg = config

    def extract_csv(self, file_path: str) -> list:
        """
        Извлекает данные из CSV файла.

        Args:
            file_path (str): Расположение CSV-файла.

        Returns:
            list: Список строк из CSV файла.

        Raises:
            Exception: При ошибке чтения файла.
        """
        try:
            logger.info(f"Извлечение данных из файла '{file_path}'...")

            with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile, delimiter=",")
                # Проверяю, что первая строка - заголовки.
                # Так как по умолчанию проект работает с config-файлом,
                # я не буду пытаться реализовать какую-то универсальную логику.
                headers = next(reader)
                if headers == self.cfg["csv_config"]["headers"]:
                    data = list(reader)
                else:
                    data = [headers] + list(reader)

            logger.info(f"Данные успешно извлечены из файла '{file_path}'.")
            return data

        except Exception as e:
            logger.error(f"Ошибка при извлечении данных из CSV: {e}")
            raise

    def extract_sqlite(
        self,
        db_path: str | None = None,
        table_name: str | None = None,
        query: str | None = None,
        query_name: str | None = None,
        result_alias: str | None = None,
    ) -> dict[str, dict[str, list[str] | list[tuple[Any, ...]]]]:
        """
        Извлекает данные из SQLite базы данных.

        Метод поддерживает:
        - Пользовательский SQL-запрос с указанием `query` и необязательным `result_alias`.
        - Извлечение конкретной таблицы по `table_name`.
        - Извлечение всех таблиц, если ни `query`, ни `table_name` не указаны.

        Args:
            db_path (Optional[str]): Путь к файлу SQLite БД. Если None, берётся из конфига.
            table_name (Optional[str]): Имя таблицы для выборки. Если указано, возвращает только её данные.
            query (Optional[str]): Произвольный SQL-запрос для выполнения. Если указан, игнорирует `table_name`.
            result_alias (Optional[str]): Псевдоним для результатов произвольного запроса.
                Если не указан вместе с `query`, используется "result".

        Returns:
            Dict[str, Tuple[List[str], List[Tuple]]]:
                Словарь, где ключ — имя результата (имя таблицы или псевдоним),
                значение — кортеж из списка столбцов и списка строк с данными.

        Raises:
            sqlite3.Error: При ошибках подключения или выполнения SQL-запроса.
        """

        def _extract_tables(sql: str) -> list[str]:
            """
            Простая функция извлечения имён таблиц из SQL-запроса.
            Ищет токены после ключевых слов FROM и JOIN.
            """
            parsed = sqlparse.parse(sql)
            tables = set()
            for stmt in parsed:
                for token in stmt.tokens:
                    if token.ttype is Keyword and token.value.upper() in (
                        "FROM",
                        "JOIN",
                    ):
                        next_tok = stmt.token_next(
                            stmt.token_index(token), skip_ws=True, skip_cm=True
                        )
                        if isinstance(next_tok, IdentifierList):
                            for identifier in next_tok.get_identifiers():
                                tables.add(identifier.get_name())
                        elif isinstance(next_tok, Identifier):
                            tables.add(next_tok.get_name())
                        elif next_tok:
                            tables.add(str(next_tok))
            return list(tables)

        if not db_path:
            db_path = os.path.join(
                self.cfg["sqlite_config"]["db_path"],
                self.cfg["sqlite_config"]["db_name"],
            )

        results = {}

        try:
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()

                # Если запрос указан
                if query:
                    alias = result_alias or query_name or "result"
                    cursor.execute(query)
                    cols = [d[0] for d in cursor.description]
                    rows = cursor.fetchall()
                    tables = _extract_tables(query)
                    results[alias] = {"columns": cols, "rows": rows, "tables": tables}
                    return results

                # Если указана одна таблица
                if table_name:
                    select = f"SELECT * FROM {table_name}"  # безопаснее через parameters, но для таблиц ок
                    cursor.execute(select)
                    cols = [d[0] for d in cursor.description]
                    rows = cursor.fetchall()
                    results[table_name] = {
                        "columns": cols,
                        "rows": rows,
                        "tables": [table_name],
                    }
                    return results

                # Базовый сценарий: все таблицы
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                )
                tables_list = [r[0] for r in cursor.fetchall()]
                for tbl in tables_list:
                    cursor.execute(f"SELECT * FROM {tbl}")
                    cols = [d[0] for d in cursor.description]
                    rows = cursor.fetchall()
                    results[tbl] = {"columns": cols, "rows": rows, "tables": [tbl]}
                return results

        except sqlite3.Error as e:
            logger.error(f"Ошибка при извлечении: {e}")
            raise
