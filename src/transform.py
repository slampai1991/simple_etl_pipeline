import logging
from typing import Any, Union, Optional
import pandas as pd


LoggerType = Union[logging.Logger, logging.LoggerAdapter]


class DataTransformer:
    """
    Класс-обработчик данных.
    Содержит методы преобразования извлеченных данных.
    """

    def __init__(self, config: dict, logger: Optional[LoggerType] = None) -> None:
        self.config = config
        self.logger: LoggerType = logger or logging.getLogger(__name__)

    def _drop_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Удаляет строки с пропущенными значениями из DataFrame

        :param pd.DataFrame `df`: DataFrame для обработки
        :return pd.DataFrame: обработанный DataFrame
        """
        self.logger.info("Удаление null-значений...")
        try:
            df = df.dropna()
            return df
        except Exception as e:
            self.logger.warning(f"Ошибка удаления null-значений: {e}")

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Удаляет дубликаты в DataFrame.

        :param pd.DataFrame `df`: DataFrame для обработки
        :return pd.DataFrame: обработанный DataFrame
        """
        self.logger.info("Удаление дубликатов...")
        try:
            df = df.drop_duplicates()
            return df
        except Exception as e:
            self.logger.warning(f"Ошибка удаления дубликатов: {e}")

    def _standartize_timastamp(self, df: pd.DataFrame) -> pd.DataFrame:
         """
         Стандартизация timestamp'ов в DataFrame

         :param pd.DataFrame `df`: DataFrame для обработки.
         :return pd.DataFrame: обработанный DataFrame.
         """
         self.logger.info("Стандартизация timestamp'ов...")
         try:
             df["timestamp"] = pd.to_datetime(df["timestamp"])
             return df
         except Exception as e:
             self.logger.warning(f"Ошибка стандартизации timestamp'ов: {e}")

    def transform(
        self, data: dict[str, dict[str, list[str] | list[tuple[Any, ...]]]]
    ) -> dict[str, pd.DataFrame]:
        """
        Обрабатывает данные SQLite согласно конфигурации:
        - Удаление null-значений
        - Удаление дубликатов
        - Стандартизация timestamp'ов

        Можно добавить другие операции

        :param pd.DataFrame `df`: Словарь с таблицами и их данными (результат extract_sqlite)
        :return dict: Трансформированные данные в виде DataFrame по таблицам
        """

        cfg = self.config.get("transformation_config", {})
        transformed = {}
        dropna = cfg.get("dropna", False)
        drop_duplicates = cfg.get("drop_duplicates", False)

        for name, payload in data.items():
            self.logger.info(f"Трансформация таблицы '{name}'...")
            cols = payload.get("columns", [])
            rows = payload.get("rows", [])

            # Создаём DataFrame
            df = pd.DataFrame(rows, columns=cols)

            # Удаляем null-значения
            if dropna:
                self.logger.info(f"Удаление null-значений в таблице '{name}'")
                df = df.dropna()
                self.logger.info(
                    f"Удалено {len(rows) - len(df)} null-значений в таблице {name}"
                )
            # Удаляем дубликаты
            if drop_duplicates:
                self.logger.info(f"Удаляем дубликаты в таблице {name}")
                df = df.drop_duplicates()
                self.logger.info(
                    f"Удалено {len(rows) - len(df)} дубликатов в таблице {name}"
                )

            # Сохраняем результат
            transformed[name] = df

        return transformed
