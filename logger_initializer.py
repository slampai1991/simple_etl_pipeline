import hashlib
import logging
import logging.handlers
import re
from datetime import datetime
from collections.abc import MutableMapping
from typing import Any
from colorlog import ColoredFormatter
from pathlib import Path


class SensitiveDataFilter(logging.Filter):
    """
    Фильтр логов, маскирующий чувствительные данные по ключам и типам маскировки.
    Наследуется от logging.Filter для кастомной обработки логов.
    Поддерживает форматы: key=value, key = value, "key": "value", 'key': 'value'.
    """

    def __init__(
        self, sensitive_fields: dict[str, str], visible_chars: int = 5
    ) -> None:
        """
        :param dict[str, str] `sensitive_fields`: Словарь {ключ: уровень маскировки ('full' или 'partial')}.
        :param int `visible_chars`: Количество открытых символов при частичной маскировке. Defaults to 5
        """
        super().__init__()
        self.sensitive_fields = {
            k.lower(): v.lower() for k, v in sensitive_fields.items()
        }
        self.visible_chars = visible_chars

        # Регулярное выражение, поддерживающее разные форматы ключ-значение
        keys = "|".join(map(re.escape, self.sensitive_fields.keys()))
        self.pattern = re.compile(
            rf"""
            (["']?)             # Открывающая кавычка ключа
            (?P<key>{keys})     # Ключ
            \1                  # Закрывающая кавычка ключа
            \s*
            (?P<sep>=|:)        # Разделитель
            \s*
            (["'])?             # Открывающая кавычка значения (опционально)
            (?P<value>.+?)      # Значение (не жадное)
            (["'])?             # Закрывающая кавычка значения
            (?=\s|,|$)          # До пробела, запятой или конца строки
            """,
            re.IGNORECASE | re.VERBOSE,
        )

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Маскирует чувствительные данные в сообщении логгера.

        :param logging.LogRecord `record`: Объект LogRecord с сообщением лога.
        :return bool: True (для прохождения фильтра)
        """
        original = record.getMessage()

        def _mask(match: re.Match) -> str:
            """
            Функция для маскировки значения по ключу и типу маскировки.

            :param re.Match `match`: Объект Match, содержащий информацию о совпадении.
            :return `str`: Строка с маскированным значением.
            """

            key = match.group("key")
            sep = match.group("sep")
            value = match.group("value")

            quote_key_start = match.group(1) or ""
            quote_key_end = match.group(3) or ""
            quote_val_start = match.group(5) or ""
            quote_val_end = match.group(7) or ""

            mask_type = self.sensitive_fields.get(key.lower(), "full")

            if mask_type == "full":
                masked_value = "*" * len(value)
            else:  # partial
                match key:
                    case "email":
                        if "@" in value:
                            local, domain = value.split("@", 1)
                            if len(local) <= self.visible_chars:
                                masked_value = "*" * len(local) + "@" + domain
                            else:
                                masked_value = (
                                    local[: self.visible_chars]
                                    + "*" * (len(local) - self.visible_chars)
                                    + "@"
                                    + domain
                                )
                        else:
                            masked_value = value[: self.visible_chars] + "*" * (
                                len(value) - self.visible_chars
                            )

                    case _:
                        if len(value) <= self.visible_chars:
                            masked_value = "*" * len(value)
                        else:
                            masked_value = (
                                "*" * (len(value) - self.visible_chars)
                                + value[-self.visible_chars :]
                            )

            # Формируем строку с сохранением исходного формата и кавычек
            return f"{quote_key_start}{key}{quote_key_end}{sep} {quote_val_start}{masked_value}{quote_val_end}"

        record.msg = self.pattern.sub(_mask, original)
        record.args = ()
        return True


class DryRunAdapter(logging.LoggerAdapter):
    """
    Класс-адаптер для логирования, добавляющий маркер режима выполнения — [DRY] или [LIVE]
    """

    def __init__(self, logger: logging.Logger, dry_run: bool) -> None:
        """
        Инициализирует адаптер с указанием режима выполнения.

        :param logging.Logger `logger`: Базовый объект logging.Logger.
        :param bool `dry_run`: Флаг режима dry run (True — dry run, False — обычный режим).
        """
        super().__init__(logger, {})
        self.dry_run = dry_run

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> Any:
        """
        Модифицирует kwargs, добавляя в extra ключ 'dry_run' со значением 'DRY' или 'LIVE'.

        :param Any `msg`: Исходное сообщение.
        :param MutableMapping[str, Any] `kwargs`: Аргументы для логгирования (включая 'extra').
        :return `tuple`: Кортеж (msg, kwargs) с дополненным kwargs.
        """
        extra = kwargs.get("extra", {})
        extra["dry_run"] = "DRY" if self.dry_run else "LIVE"
        kwargs["extra"] = extra
        return msg, kwargs


class LoggerInitializer:
    """
    Класс для инициализации и настройки логгеров согласно YAML-конфигурации.

    Поддерживает:
      - Создание логгеров для отдельных стадий с ротацией файлов
      - Маскирование чувствительных данных
      - Добавление маркера [DRY]/[LIVE]
      - Логгирование результата загрузки данных в хранилища
    """

    def __init__(
        self, cfg: dict[str, dict] | dict = {}, bootstrap_mode: bool = False
    ) -> None:
        """

        :param dict[str, dict] `cfg`: Объединенный словарь с конфигурациями base_cfg и log_cfg
        :param bool `bootstrap_mode`: Флаг для запуска режима первичного логгирования. Defaults to False.
        """
        self.bootstrap_mode = bootstrap_mode

        self.base_cfg = cfg.get("base_cfg", {})
        self.log_cfg = cfg.get("log_cfg", {}) if not bootstrap_mode else {}

        self.pipeline_id = self.base_cfg.get("pipeline_id", "bootstrap")
        self.dry_run = self.base_cfg.get("dry_run", False)
        self.config_version = self.base_cfg.get("config_version", "bootstrap")

        self.date_str = datetime.now().strftime(
            self.log_cfg.get("variables", {}).get("date", "%Y-%m-%d")
        )
        self.hash_str = self._generate_hash()

        self.loggers = {}

        self.console_handler = self._setup_console_handler()

        if self.bootstrap_mode:
            self.bootstrap_logger = self._init_bootstrap_logger()
            root = logging.getLogger()
            root.setLevel(logging.INFO)
            root.addHandler(self._setup_console_handler())
            if bootstrap_mode:
                root.propagate = False
            self.bootstrap_logger.info("Запуск в bootstrap-режиме.")
        else:
            base_logger = logging.getLogger("ConfigLoader")
            base_logger.setLevel(logging.INFO)
            if not base_logger.hasHandlers():
                base_logger.addHandler(self.console_handler)
            base_logger.propagate = False
            base_logger.info(
                f"Конфигурация загружена: pipeline_id={self.pipeline_id}, config_version={self.config_version}"
            )

    def _generate_hash(self) -> str:
        """
        Генерирует короткий SHA1-хэш на основе pipeline_id и текущей даты.

        :return `str`: 8-символьный хэш.
        """
        base_str = f"{self.pipeline_id}_{self.date_str}"
        return hashlib.sha1(base_str.encode()).hexdigest()[:8]

    def _setup_console_handler(self) -> logging.Handler:
        """
        Настраивает и возвращает обработчик для вывода логов в консоль.

        Поддерживает:
        - Цветной вывод (если включен в конфигурации и доступен ColoredFormatter)
        - Кастомный формат логов из конфигурации
        - Отключение вывода в консоль (возвращает NullHandler)

        :returns logging.Handler: настроенный обработчик логов (StreamHandler или NullHandler)
        """
        if self.bootstrap_mode:
            formatter = logging.Formatter(
                "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            return handler

        log_format = self.log_cfg.get(
            "log_format",
            {
                "pattern": "%(asctime)s - [%(levelname)s] [%(dry_run)s] - %(name)s - %(message)s"
            },
        ).get("pattern")

        if self.log_cfg.get("log_to_console", False):
            if self.log_cfg.get("color_log", True) and ColoredFormatter:
                formatter = ColoredFormatter(
                    "%(log_color)s" + log_format,
                    reset=True,
                    log_colors={
                        "DEBUG": "cyan",
                        "INFO": "green",
                        "WARNING": "yellow",
                        "ERROR": "red",
                        "CRITICAL": "bold_red",
                    },
                )
            else:
                formatter = logging.Formatter(log_format)
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            return handler
        return logging.NullHandler()

    def _init_bootstrap_logger(self) -> logging.Logger:
        """
        Инициализирует временный логгер для вывода сообщений до загрузки полной конфигурации логгирования.
        Используется на этапе загрузки и валидации конфигураций.

        :return `logging.Logger`: Логгер с минимальной конфигурацией.
        """
        stage_name = "BOOTSTRAP"
        level = "INFO"

        logger = logging.getLogger(stage_name)
        logger.setLevel(level)
        if not logger.handlers:
            handler = self._setup_console_handler()
            formatter = logging.Formatter(
                f"%(asctime)s - [{stage_name}] - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.propagate = False
        return logger

    def init_logger(self, stage_name: str) -> logging.LoggerAdapter | None:
        """
        Инициализирует и возвращает адаптированный логгер для указанной стадии.

        :param str `stage_name`: Имя стадии (должно соответствовать ключу в log_config['stages']).
        :return `LoggerAdapter` | `None`: Адаптированный логгер или None, если стадия отключена.
        """
        if stage_name in self.loggers:
            return self.loggers[stage_name]

        stages = self.log_cfg.get("stages", {})
        stage_conf = stages.get(stage_name, {})

        if not stage_conf.get("enabled", False):
            return None

        log_dir = stage_conf.get("log_dir", self.log_cfg.get("log_path", "logs/"))
        Path(log_dir).mkdir(parents=True, exist_ok=True)

        log_file_template = stage_conf.get("log_file", "{date}_{hash}_stage.log")
        log_file_name = log_file_template.format(
            date=self.date_str, pipeline_id=self.pipeline_id, hash=self.hash_str
        )
        log_file_path = Path(log_dir) / log_file_name

        logger = logging.getLogger(stage_name)
        if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
            logger.addHandler(self.console_handler)

        logger.setLevel(
            getattr(
                logging, self.log_cfg.get("log_level", "INFO").upper(), logging.INFO
            )
        )
        logger.propagate = False

        log_format = self.log_cfg["log_format"].get(
            "pattern",
            "%(asctime)s - [%(levelname)s] [%(dry_run)s] - %(name)s - %(message)s",
        )
        formatter = logging.Formatter(log_format)

        # Настраиваем файловый обработчик с ротацией, если включена
        rotation_cfg = self.log_cfg.get("log_rotation", {})
        if rotation_cfg.get("enabled", False):
            when = rotation_cfg.get("when", "midnight")
            interval = rotation_cfg.get("interval", 1)
            backup_count = rotation_cfg.get("backup_count", 7)

            file_handler = logging.handlers.TimedRotatingFileHandler(
                log_file_path,
                when=when,
                interval=interval,
                backupCount=backup_count,
                encoding="utf-8",
            )
        else:
            file_handler = logging.FileHandler(log_file_path, encoding="utf-8")

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Добавляем фильтр для маскировки чувствительных данных, если включено
        if self.log_cfg.get("sanitize_sensitive_data", False):
            raw_fields = self.log_cfg.get("sensitive_fields", {})
            if isinstance(raw_fields, dict):
                sensitive_fields_map = raw_fields
            else:
                # Для обратной совместимости: если указали список, считаем, что все 'full'
                sensitive_fields_map = {key: "full" for key in raw_fields}
            logger.addFilter(SensitiveDataFilter(sensitive_fields_map))

        # Оборачиваем в адаптер, чтобы добавить [DRY]/[LIVE]
        adapted_logger = DryRunAdapter(logger, dry_run=self.dry_run)

        adapted_logger.info(
            "Работа в режиме DRY_RUN — запись данных будет пропущена."
            if self.dry_run
            else "Режим LIVE — данные будут записаны."
        )

        self.loggers[stage_name] = adapted_logger
        return adapted_logger

    def log_loading_result(
        self,
        logger: logging.LoggerAdapter,
        source_name: str,
        enabled: bool,
        success: bool | None = None,
        error: Exception | None = None,
        tables_loaded: int = 0,
        rows_loaded: int = 0,
    ) -> None:
        """
        Унифицированное логгирование результатов загрузки данных в хранилище.

        :param logging.LoggerAdapter `logger`: Адаптированный логгер для стадии loading.
        :param str `source_name`: Имя источника (например, "SQLite", "Postgres").
        :param bool `enabled`: Флаг, включена ли загрузка в конфигурации.
        :param bool | None `success`: True если загрузка прошла успешно, False если была ошибка, None если непонятно.
        :param Exception | None `error`: Объект Exception, если произошла ошибка.
        :param int `tables_loaded`: Число успешно загруженных таблиц, по умолчанию = 0
        :param int `rows_loaded`: Число успешно загруженных строк.
        """
        if not enabled:
            logger.warning(
                f"Загрузка в {source_name} пропущена (отключено в конфигурации)."
            )
            return

        if self.dry_run:
            logger.info(
                f"Dry Run — загрузка в {source_name} не выполнялась "
                f"(таблиц: {tables_loaded}, строк: {rows_loaded})."
            )
            return

        if success:
            logger.info(
                f"Загрузка в {source_name} завершена успешно: "
                f"{tables_loaded} таблиц, {rows_loaded} строк."
            )
        elif error:
            logger.error(f"Ошибка при загрузке в {source_name}: {error}")
        else:
            logger.warning(f"Результат загрузки в {source_name} не определён.")
