import hashlib
import logging
import logging.handlers
import os
import re
from datetime import datetime
from collections.abc import MutableMapping
from typing import Any


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
        :param sensitive_fields: Словарь {ключ: уровень маскировки ('full' или 'partial')}.
        :param visible_chars: Количество открытых символов при частичной маскировке.
        """
        super().__init__()
        self.sensitive_fields = {
            k.lower(): v.lower() for k, v in sensitive_fields.items()
        }
        self.visible_chars = visible_chars

        # Регулярное выражение, поддерживающее разные форматы ключ-значение
        # Ключ в кавычках или без, пробелы и разделитель (= или :)
        self.pattern = re.compile(
            r"""
            (["']?)(?P<key>{keys})(["']?)  # Ключ в кавычках или нет
            \s*                            # Любые пробелы
            (?P<sep>=|:)                   # Разделитель = или :
            \s*                            # Любые пробелы
            (["']?)                        # Открывающая кавычка значения (если есть)
            (?P<value>[^"'\s,]+)           # Значение (без пробелов, кавычек, запятых)
            (["']?)                        # Закрывающая кавычка значения (если есть)
            """.format(
                keys="|".join(map(re.escape, self.sensitive_fields.keys()))
            ),
            re.IGNORECASE | re.VERBOSE,
        )

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Маскирует чувствительные данные в сообщении логгера.

        :param record: Объект LogRecord с сообщением лога.
        :return: True (для прохождения фильтра)
        """
        original = record.getMessage()

        def _mask_value(match: re.Match) -> str:
            """
            Функция для маскировки значения по ключу и типу маскировки.

            :param match: Объект Match, содержащий информацию о совпадении.
            :return: Строка с маскированным значением.
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
                if len(value) <= self.visible_chars:
                    masked_value = "*" * len(value)
                else:
                    masked_value = value[: self.visible_chars] + "*" * (
                        len(value) - self.visible_chars
                    )

            # Формируем строку с сохранением исходного формата и кавычек
            return f"{quote_key_start}{key}{quote_key_end}{sep} {quote_val_start}{masked_value}{quote_val_end}"

        filtered = self.pattern.sub(_mask_value, original)
        record.msg = filtered
        record.args = ()
        return True


class DryRunAdapter(logging.LoggerAdapter):
    """
    LoggerAdapter, добавляющий в каждое сообщение логов маркер [DRY] или [LIVE].
    """

    def __init__(self, logger: logging.Logger, dry_run: bool) -> None:
        """
        :param logger: Базовый объект logging.Logger.
        :param dry_run: Флаг режима dry run (True — dry run, False — обычный режим).
        """
        super().__init__(logger, {})
        self.dry_run = dry_run

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> Any:
        """
        Модифицирует kwargs, добавляя в extra ключ 'dry_run' со значением 'DRY' или 'LIVE'.

        :param msg: Исходное сообщение.
        :param kwargs: Аргументы для логгирования (включая 'extra').
        :return: Кортеж (msg, kwargs) с дополненным kwargs.
        """
        extra = kwargs.get("extra", {})
        extra["dry_run"] = "DRY" if self.dry_run else "LIVE"
        kwargs["extra"] = extra
        return msg, kwargs


class LoggerInitializer:
    """
    Класс для инициализации и настройки логгеров согласно YAML-конфигурации.

    Поддерживает:
      - Валидацию наличия обязательных ключей (pipeline_id, config_version, log_config)
      - Создание логгеров для отдельных стадий с ротацией файлов
      - Маскирование чувствительных данных
      - Добавление маркера [DRY]/[LIVE]
      - Логгирование результата загрузки данных в хранилища
    """

    REQUIRED_KEYS = ["pipeline_id", "config_version", "log_config", "dry_run"]

    def __init__(self, config: dict[str, Any]) -> None:
        """
        :param config: Словарь конфигурации, загруженный из base_config.yaml
        """
        self.config = config

        self.pipeline_id: str = config["pipeline_id"]
        self.dry_run: bool = config.get("dry_run", False)
        self.log_config: dict[str, Any] = config["log_config"]

        self.date_str: str = datetime.now().strftime(
            self.log_config.get("variables", {}).get("date", "%Y-%m-%d")
        )
        self.hash_str: str = self._generate_hash()
        self.loggers: dict[str, logging.LoggerAdapter] = {}

        # Базовый логгер для записи о загрузке конфигурации
        base_logger = logging.getLogger("ConfigLoader")
        base_logger.setLevel(logging.INFO)
        if not base_logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(
                logging.Formatter("%(asctime)s - [%(levelname)s] - %(message)s")
            )
            base_logger.addHandler(console_handler)
        base_logger.info(
            f"Конфигурация загружена: pipeline_id={self.pipeline_id}, "
            f"config_version={config['config_version']}"
        )

    def _generate_hash(self) -> str:
        """
        Генерирует короткий SHA1-хэш на основе pipeline_id и текущей даты.

        :return: 8-символьный хэш.
        """
        base_str = f"{self.pipeline_id}_{self.date_str}"
        return hashlib.sha1(base_str.encode()).hexdigest()[:8]

    def init_logger(self, stage_name: str) -> logging.LoggerAdapter | None:
        """
        Инициализирует и возвращает адаптированный логгер для указанной стадии.

        :param stage_name (str): Имя стадии (должно соответствовать ключу в log_config['stages']).
        :return: LoggerAdapter, либо None если стадия отключена.
        """
        stages: dict[str, Any] = self.log_config.get("stages", {})
        stage_conf: dict[str, Any] = stages.get(stage_name, {})

        if not stage_conf.get("enabled", False):
            return None

        log_dir: str = stage_conf.get(
            "log_dir", self.log_config.get("log_path", "logs/")
        )
        os.makedirs(log_dir, exist_ok=True)

        log_file_template: str = stage_conf.get("log_file", "{date}_{hash}_stage.log")
        log_file_name: str = log_file_template.format(
            date=self.date_str, pipeline_id=self.pipeline_id, hash=self.hash_str
        )
        log_file_path: str = os.path.join(log_dir, log_file_name)

        logger: logging.Logger = logging.getLogger(stage_name)
        level_name: str = self.log_config.get("log_level", "INFO").upper()
        logger.setLevel(getattr(logging, level_name, logging.INFO))
        logger.propagate = False

        log_format: str = self.log_config.get(
            "log_format",
            "%(asctime)s - [%(levelname)s] [%(dry_run)s] - %(name)s - %(message)s",
        )
        formatter = logging.Formatter(log_format)

        # Настраиваем файловый обработчик с ротацией, если включена
        rotation_cfg: dict[str, Any] = self.log_config.get("log_rotation", {})
        if rotation_cfg.get("enabled", False):
            when: str = rotation_cfg.get("when", "midnight")
            interval: int = rotation_cfg.get("interval", 1)
            backup_count: int = rotation_cfg.get("backup_count", 7)
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

        # Настраиваем консольный вывод, если включён
        if self.log_config.get("log_to_console", False):
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # Добавляем фильтр для маскировки чувствительных данных, если включено
        if self.log_config.get("sanitize_sensitive_data", False):
            raw_fields = self.log_config.get("sensitive_fields", {})
            if isinstance(raw_fields, dict):
                sensitive_fields_map: dict[str, str] = raw_fields
            else:
                # Для обратной совместимости: если указали список, считаем, что все 'full'
                sensitive_fields_map = {key: "full" for key in raw_fields}
            logger.addFilter(SensitiveDataFilter(sensitive_fields_map))

        # Оборачиваем в адаптер, чтобы добавить [DRY]/[LIVE]
        adapted_logger = DryRunAdapter(logger, dry_run=self.dry_run)

        if self.dry_run:
            adapted_logger.info(
                "Работа в режиме DRY_RUN — запись данных будет пропущена."
            )
        else:
            adapted_logger.info("Режим LIVE — данные будут записаны.")

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

        :param logger: Адаптированный логгер для стадии loading.
        :param source_name: Имя источника (например, "SQLite", "Postgres").
        :param enabled: Флаг, включена ли загрузка в конфигурации.
        :param success: True если загрузка прошла успешно, False если была ошибка, None если непонятно.
        :param error: Объект Exception, если произошла ошибка.
        :param tables_loaded: Число успешно загруженных таблиц.
        :param rows_loaded: Число успешно загруженных строк.
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
