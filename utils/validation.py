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
    - Проверка составных ключей на уникальность
    - Применение пользовательских ограничений
    """

    def __init__(self, val_cfg: dict, logger: Optional[LoggerType] = None):
        """
        :param dict `val_cfg`: Конфигурация валидации (внешние ключи, составные ключи, ограничения)
        :param LoggerType | None `logger`: Пользовательский логгер (по умолчанию logging.getLogger(__name__))
        """
        self.fk_cfg = val_cfg.get("foreign_keys", {})
        self.const_cfg = val_cfg.get("constraints", {})
        self.ck_cfg = val_cfg.get("composite_keys", {})
        self.logger: LoggerType = logger or logging.getLogger(__name__)

    def _filter_df(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """
        Применяет безопасный фильтр к DataFrame на основе строкового выражения условия.

        Поддерживаемые выражения:
            - <column>.str.contains(<pattern>[, na=True|False])
            - <column>.str.match(<pattern>[, na=True|False])
            - <column>.isin([...])
            - Логические выражения, совместимые с pandas (and/or/not заменены на &,|,~)

        В случае ошибки — возвращает оригинальный DataFrame и логирует предупреждение.

        :param pd.DataFrame `df`: Входной DataFrame
        :param str `condition`: Условие фильтрации в строковом виде

        :return `pd.DataFrame`: Отфильтрованный DataFrame
        :raise `Exception`: при внутренней ошибке обработки выражения
        """
        self.logger.debug(f"Применяется фильтр: {condition!r}")

        safe_globals = {"__builtins__": {}, "pd": pd, "re": re}

        # Перевод логических операторов
        safe_condition = (
            condition.replace(" and ", " & ")
            .replace(" or ", " | ")
            .replace(" not ", " ~")
        )

        try:
            result = eval(
                safe_condition, safe_globals, {**{col: df[col] for col in df.columns}}
            )
            if isinstance(result, pd.Series) and result.dtype == bool:
                return df[result]
            else:
                raise ValueError("Результат выражения не является булевой маской")
        except Exception as e:
            self.logger.warning(f"Ошибка применения фильтра '{condition}': {e}")
            return df

    def validate_foreign_keys(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """
        Проверка внешних ключей и удаление строк с невалидными ссылками

        :param dict[str, pd.DataFrame] `df_dict`: Словарь с таблицами
        :return `dict[str, pd.DataFrame]`: Обновлённый словарь с очищенными таблицами
        """
        self.logger.info("Валидация внешних ключей...")

        for table, fks in self.fk_cfg.items():
            for fk_col, parent_key in fks.items():
                parent_table, parent_col = parent_key.split(".")
                if table not in df_dict or parent_table not in df_dict:
                    self.logger.warning(
                        f"Пропущена FK-проверка {table}.{fk_col} -> {parent_table}.{parent_col}"
                    )
                    continue

                child_df = df_dict[table]
                parent_ids = set(df_dict[parent_table][parent_col])
                before = len(child_df)
                child_df = child_df[child_df[fk_col].isin(parent_ids)]
                after = len(child_df)
                self.logger.info(
                    f"{table}: удалено {before - after} строк по внешнему ключу '{fk_col}'"
                )
                df_dict[table] = child_df.reset_index(drop=True)

        self.logger.info("Валидация внешних ключей завершена.")
        return df_dict

    def validate_constraints(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """
        Применение пользовательских ограничений фильтрации строк из конфигурации.

        :param dict[str, pd.DataFrame] `df_dict`: Таблицы для валидации
        :return `dict[str, pd.DataFrame]`: Таблицы после применения ограничений
        """
        for table, df in df_dict.items():
            rules = self.const_cfg.get(table, [])
            for cond in rules:
                before = len(df)
                df = self._filter_df(df, cond)
                self.logger.info(
                    f"{table}: удалено {before - len(df)} строк по правилу '{cond}'"
                )
            df_dict[table] = df.reset_index(drop=True)
        return df_dict

    def validate_composite_keys(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """
        Проверка уникальности составных ключей (дубликаты не удаляются).

        :param dict[str, pd.DataFrame] `df_dict`: Словарь таблиц
        :return `dict[str, pd.DataFrame]`: Таблицы без изменений (только лог предупреждений)
        """
        self.logger.info("Валидация составных ключей...")
        for table, keys_list in self.ck_cfg.items():
            df = df_dict.get(table)
            if df is None:
                continue
            for keys in keys_list:
                if df.duplicated(subset=keys).any():
                    self.logger.warning(
                        f"{table}: дублирующиеся записи по ключу {keys}"
                    )
        self.logger.info("Валидация составных ключей завершена.")
        return df_dict

    def run_all_validations(
        self, df_dict: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """
        Выполняет все этапы валидации данных:
          - Внешние ключи
          - Составные ключи
          - Ограничения

        :param dict[str, pd.DataFrame] `df_dict`: Исходные таблицы
        :return `dict[str, pd.DataFrame]`: Таблицы после всех валидаций
        :raise `Exception`: Любая ошибка при валидации
        """
        self.logger.info("Запуск полной валидации...")
        try:
            df_dict = self.validate_foreign_keys(df_dict)
            df_dict = self.validate_composite_keys(df_dict)
            df_dict = self.validate_constraints(df_dict)
            return df_dict
        except Exception as e:
            self.logger.error(f"Ошибка валидации: {e}")
            raise
