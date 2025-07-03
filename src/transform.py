import logging
from typing import Any, Union, Optional, Callable
import pandas as pd


LoggerType = Union[logging.Logger, logging.LoggerAdapter]


class DataTransformer:
    """
    Класс-обработчик данных.
    Содержит методы преобразования извлеченных данных.
    """

    def __init__(self, trans_cfg: dict, logger: Optional[LoggerType] = None) -> None:
        self.cfg = trans_cfg
        self.logger: LoggerType = logger or logging.getLogger(__name__)

        self._operations_map: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
            "dropna": self._dropna,
            "drop_duplicates": self._drop_duplicates,
            "normalize_columns": self._normalize_columns,
            "standardize_dates": self._standardize_dates,
        }

    def _dropna(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Удаляет строки с пропущенными значениями из DataFrame

        :param pd.DataFrame `df`: DataFrame для обработки
        :return `pd.DataFrame`: обработанный DataFrame
        """
        self.logger.info("Выполняется dropna...")
        before = len(df)
        df = df.dropna()
        after = len(df)
        self.logger.info(f"Удалено строк с null: {before - after}")
        return df

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Удаляет дубликаты в DataFrame.

        :param pd.DataFrame `df`: DataFrame для обработки
        :return `pd.DataFrame`: обработанный DataFrame
        """
        self.logger.info("Выполняется drop_duplicates...")
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
        self.logger.info(f"Удалено дубликатов: {before - after}")
        return df

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Выполняет нормализацию колонок DataFrame - приводит названия колонок к нижнему регистру,
        удаляет лишние пробелы, заменяет все специальные символы на нижнее подчеркивание.

        Пример:
            'First Name ' -> 'first_name'
            'Last-Name!' -> 'last_name'
            'EMAIL ADDRESS' -> 'email_address'

        :param pd.DataFrame `df`: DataFrame для обработки.
        :return `pd.DataFrame`: обработанный DataFrame.
        """
        self.logger.info("Выполняется normalize_columns...")
        try:
            df.columns = (
                df.columns.str.strip()
                .str.lower()
                .str.replace(r"[^\w]+", "_", regex=True)
            )
            self.logger.info("Колонки нормализованы")
        except Exception as e:
            self.logger.error(f"Ошибка при нормализации колонок: {e}", exc_info=True)
        return df

    def _dropna_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Удаление строк с NaT после стандартизации дат (вызывается в _standardize_dates).

        :param pd.DataFrame `df`: DataFrame для обработки.
        :return `pd.DataFrame`: обработанный DataFrame.
        """
        date_cols = [
            col for col in df.columns if "date" in col.lower() or "time" in col.lower()
        ]
        before = len(df)
        df = df.dropna(subset=date_cols)
        after = len(df)
        self.logger.info(
            f"Удалено строк с NaT в колонках даты/времени: {before - after}"
        )
        return df

    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Стандартизирует даты в DataFrame, преобразуя их в формат 'YYYY-MM-DD'.
        Ищет колонки, содержащие в названии 'date' или 'time' (без учета регистра) и
        пытается преобразовать их значения к стандартному формату даты.

        :param pd.DataFrame `df`: DataFrame для обработки.
        :return `pd.DataFrame`: обработанный DataFrame с преобразованными датами.
        """
        self.logger.info("Выполняется standardize_dates...")
        for col in df.columns:
            if "date" in col.lower() or "time" in col.lower():
                try:
                    # Конвертируем в datetime, при ошибках заменяем на NaT
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception as e:
                    self.logger.error(
                        f"Ошибка стандартизации даты в колонке {col}: {e}"
                    )

        df = self._dropna_dates(df)
        return df

    def transform_data(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """
        Выполняет последовательную трансформацию данных согласно настройкам в конфигурации.

        Метод применяет заданные операции трансформации к каждой таблице из входных данных.
        Операции применяются в порядке, определенном в конфигурации (cfg['operation_order']).
        Если трансформация отключена в конфигурации (cfg['enabled'] = False), возвращает
        исходные данные без изменений.

        :param dict[str, pd.DataFrame] `data`: Словарь DataFrame'ов для обработки,
                где ключ - название таблицы, значение - pandas DataFrame с данными
        :return `dict[str, pd.DataFrame]`: Словарь обработанных DataFrame'ов с теми же ключами
        """
        if not self.cfg.get("enabled", False):
            self.logger.warning("Трансформация отключена в конфигурации.")
            return data

        operations = self.cfg.get("operations", {})
        order = self.cfg.get("operation_order", [])

        transformed = {}

        for table_name, df in data.items():
            self.logger.info(f"Трансформация таблицы '{table_name}'")
            df_result = df.copy()

            for op_name in order:
                op_cfg = operations.get(op_name, {})
                if op_cfg.get("enabled", False):
                    op_func = self._operations_map.get(op_name)
                    if op_func:
                        df_result = op_func(df_result)
                    else:
                        self.logger.warning(f"Операция '{op_name}' не реализована.")
            transformed[table_name] = df_result

        return transformed
