import logging
import re
import pandas as pd


logger = logging.getLogger(__name__)


class DataProfiler:
    """
    Класс для профилирования данных.
    Позволяет анализировать основные характеристики DataFrame:
    - базовая статистика
    - распределения пропусков
    - уникальные значения
    - типы данных
    - корреляция
    """

    def __init__(self, profiling_config: dict | None = None) -> None:
        self.cfg = profiling_config or {}

    def _clean_control_chars(self, df: pd.DataFrame) -> pd.DataFrame:
        """Вспомогательная функция для очистки контрольных символов в DataFrame

        Args:
            df (pd.DataFrame): Исходный DataFraame

        Returns:
            pd.DataFrame: Очищенный DataFrame
        """

        def clean_value(x):
            if isinstance(x, str):
                return re.sub(r"[\x00-\x1F]+", "", x)
            return x

        return df.apply(lambda col: col.map(clean_value))

    def profile(self, df: pd.DataFrame) -> dict:
        """
        Проводит профилирование переданного DataFrame.

        Args:
            df (pd.DataFrame): Данные для анализа.

        Returns:
            dict: Результаты профилирования.
        """

        logger.info("Начинается профилирование данных.")

        df = self._clean_control_chars(df)
        profile_report = {}

        # Типы данных
        profile_report["dtypes"] = df.dtypes.apply(lambda x: str(x)).to_dict()

        # Статистика для числовых колонок
        profile_report["numeric_summary"] = df.describe().to_dict()

        # Статистика для категориальных колонок
        profile_report["categorical_summary"] = df.describe(
            include=["object", "category"]
        ).to_dict()

        # Кол-во пропусков
        profile_report["missing_values"] = df.isna().sum().to_dict()

        # Кол-во уникальных значений
        profile_report["unique_values"] = df.nunique().to_dict()

        # Корреляция (числовые только)
        if df.select_dtypes(include=["number"]).shape[1] > 1:
            profile_report["correlation"] = df.corr().to_dict()
        else:
            profile_report["correlation"] = {}

        logger.info("Профилирование данных завершено.")
        return profile_report

    def log_profile(self, profile: dict, table_name: str = "") -> None:
        """
        Логирует результаты профилирования в лог.

        Args:
            profile (dict): Результаты профилирования.
            table_name (str): Имя таблицы или датафрейма.
        """
        logger.info(f"Профилирование таблицы: {table_name}")
        for key, val in profile.items():
            logger.info(f"{key}: {val}")
