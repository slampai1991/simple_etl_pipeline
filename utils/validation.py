import ast
import logging
import re
import pandas as pd
from typing import Union, Optional


LoggerType = Union[logging.Logger, logging.LoggerAdapter]


class DataValidator:
    """
    Класс для валидации DataFrame-данных:
    - Проверка ссылочной целостности
    - Проверка пользовательских ограничений
    """

    def __init__(self, val_cfg: dict, logger: Optional[LoggerType] = None):
        self.fk_cfg = val_cfg.get("foreign_keys", {})
        self.const_cfg = val_cfg.get("constraints", {}).get("rules", {})
        self.ck_cfg = val_cfg.get("composite_keys", {})
        self.logger: LoggerType = logger or logging.getLogger(__name__)

    def _filter_df(self, df: pd.DataFrame, condition: str):
        """
        Применяет фильтр к DataFrame на основе заданного строкового условия.

        Поддерживаются следующие типы выражений:
            1. <column>.str.contains(<pattern>[, na=True|False])
            2. <column>.str.match(<pattern>[, na=True|False])
            3. <column>.isin([...])
            4. <column>.str.len() <op> <int> — поддерживаются операторы >, <, >=, <=, ==, !=
            5. Любое выражение, совместимое с DataFrame.query(..., engine="python")

        В случае некорректного выражения — возвращается оригинальный DataFrame и логируется предупреждение.

        Args:
            df (pd.DataFrame): DataFrame, к которому нужно применить фильтр.
            condition (str): Строковое выражение фильтрации.

        Returns:
            pd.DataFrame: Отфильтрованный DataFrame.
        """
        self.logger.debug(f"Применяется фильтр: {condition}")

        # Формируем паттерн
        m = re.fullmatch(
            r"(\w+)\.str\.contains\((r?['\"].+['\"])(?:,\s*na\s*=\s*(True|False))?\)",
            condition,
        )

        if m:
            col, pattern, na_flag = m.groups()
            na = (na_flag == "True") if na_flag is not None else False
            pat = ast.literal_eval(pattern)
            return df[df[col].str.contains(pat, na=na)]

        m = re.fullmatch(
            r"(\w+)\.str\.match\((r?['\"].+['\"])(?:,\s*na\s*=\s*(True|False))?\)",
            condition,
        )
        if m:
            col, pattern, na_flag = m.groups()
            na = (na_flag == "True") if na_flag is not None else False
            pat = ast.literal_eval(pattern)
            return df[df[col].str.match(pat, na=na)]

        m = re.fullmatch(r"(\w+)\.isin\((\[.+\])\)", condition)
        if m:
            col, list_literal = m.groups()
            values = ast.literal_eval(list_literal)
            return df[df[col].isin(values)]

        m = re.fullmatch(r"(\w+)\.str\.len\(\)\s*([><=!]+)\s*(\d+)", condition)
        if m:
            col, op, num = m.group(1), m.group(2), int(m.group(3))
            return df[eval(f"df['{col}'].str.len() {op} {num}")]

        # 3) Наконец fallback для любых остальных выражений через query
        try:
            return df.query(condition, engine="python")
        except Exception as e:
            self.logger.warning(f"Не удалось разобрать условие '{condition}': {e}")
            return df

    def validate_foreign_keys(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """
        Проверяет ссылочную целостность между таблицами в соответствии с конфигурацией внешних ключей.

        Проходит по всем таблицам и их внешним ключам, проверяя что значения внешних ключей
        присутствуют в родительских таблицах. Строки с невалидными внешними ключами удаляются.

        Args:
            df_dict (dict[str, pd.DataFrame]): Словарь с DataFrame-ами, где ключи - имена таблиц

        Returns:
            dict[str, pd.DataFrame]: Словарь с очищенными DataFrame-ами после валидации внешних ключей
        """
        self.logger.info("Валидация внешних ключей...")

        for table, fks in self.fk_cfg.items():
            for fk_col, parent_table in fks.items():
                if table not in df_dict or parent_table not in df_dict:
                    self.logger.warning(
                        f"Пропущена проверка внешнего ключа: '{table}.{fk_col}' -> '{parent_table}'\n"  
                        f"Причина: одна или обе таблицы отсутствуют в df_dict: {df_dict.keys()}"
                    )
                    continue

                child_df = df_dict[table]
                parent_ids = set(df_dict[parent_table]["id"])
                before = len(child_df)

                child_df = child_df[child_df[fk_col].isin(parent_ids)]
                after = len(child_df)

                self.logger.info(
                    f"{table}: удалено {before - after} строк с невалидными '{fk_col}'"
                )
                df_dict[table] = child_df.reset_index(drop=True)

        return df_dict

    def validate_constraints(self, df_dict):
        for table, df in df_dict.items():
            rules = self.const_cfg.get(table, [])
            for cond in rules:
                before = len(df)
                df = self._filter_df(df, cond)
                self.logger.info(
                    f"{table}: удалено {before - len(df)} строк по '{cond}'"
                )
            df_dict[table] = df.reset_index(drop=True)
        return df_dict

    def validate_composite_keys(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """
        Проверяет составные ключи в таблицах на уникальность.

        Проходит по всем таблицам и проверяет, что комбинации значений в указанных столбцах уникальны.
        Если находятся дубликаты по составному ключу, выводится предупреждение в лог.

        Args:
            data (dict[str, pd.DataFrame]): Словарь с DataFrame-ами, где ключи - имена таблиц

        Returns:
            dict[str, pd.DataFrame]: Исходный словарь с DataFrame-ами после проверки составных ключей
        """
        for table, keys_list in self.ck_cfg.items():
            df = df_dict.get(table)
            if df is None:
                continue
            for keys in keys_list:
                if df.duplicated(subset=keys).any():
                    self.logger.warning(f"Дубли {keys} в {table}")
        return df_dict

    def run_all_validations(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """
        Запускает комплексную валидацию набора данных - DataFrame - по следующим направлениям:

        1. Валидация внешних ключей:
           - Проверяет, что значения внешних ключей в дочерних таблицах
             присутствуют в родительских таблицах.
           - Строки с невалидными значениями внешних ключей удаляются.

        2. Валидация составных ключей:
           - Проверяет уникальность комбинаций колонок, определённых как составные ключи.
           - В случае обнаружения дубликатов генерирует предупреждения в лог.
           - Не удаляет дубликаты, а только информирует.

        3. Валидация пользовательских ограничений:
           - Применяет пользовательские правила фильтрации строк, заданные в конфигурации.
           - Строки, не удовлетворяющие условиям, удаляются.

        :param dict[str, pd.DataFrame] `df_dict`: Словарь с наборами данных для валидации,
                где ключ — имя таблицы, а значение — соответствующий pandas DataFrame.
        :return `dict[str, pd.DataFrame]`: Новый словарь c DataFrame после применения всех проверок и фильтраций.

        Примечания:
            - Для корректной работы необходима корректная конфигурация внешних ключей,
              составных ключей и ограничений, переданная при инициализации класса.
            - Валидация внешних ключей базируется на совпадении с колонкой 'id'
              родительской таблицы (может быть расширена для других ключей).
            - Валидация ограничений и фильтрация выполняется последовательно,
              что может приводить к каскадному удалению строк.
        """
        self.logger.info("Запуск комплексной валидации...")
        try:
            df_dict = self.validate_foreign_keys(df_dict)
            df_dict = self.validate_composite_keys(df_dict)
            df_dict = self.validate_constraints(df_dict)
            return df_dict
        except Exception as e:
            self.logger.error(f"Ошибка валидации: {e}")
            raise
