import yaml
import os
import logging

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


class ConfigLoader:
    """
    Класс для загрузки, объединения и валидации конфигурационных файлов
    модульного ETL-процесса.

    Основные возможности:
    - Загрузка базовой конфигурации (base_config.yaml).
    - Загрузка конфигураций отдельных этапов (extraction, transformation, validation и т.д.)
      из отдельных YAML-файлов в директории конфигураций.
    - Рекурсивное объединение конфигураций, где приоритет отдается конфигурациям этапов.
    - Валидация наличия обязательных ключей и соответствия типов данных.

    Пример использования:
        loader = ConfigLoader(cfg_root="cfg")
        combined_cfg = loader.load_stage_config("extraction", required_keys=["extraction_config"])
        if loader.validate_config(combined_cfg):
            print("Конфигурация загружена и валидирована")
    """

    validation_schema = {
        "pipeline_id": str,
        "config_version": str,
        "dry_run": bool,
        "log_config": dict,
        "generation_config": dict,
        "extraction_config": dict,
        "transformation_config": dict,
        "validation_config": dict,
        "profiling_config": dict,
        "loading_config": dict,
        "analytics_config": dict,
    }

    def __init__(
        self, cfg_root: str = "cfg", base_config_name: str = "base_config.yaml"
    ):
        """
        Инициализация загрузчика конфигураций.
        Загружает базовую конфигурацию при создании объекта.
        
        Args:
            cfg_root (str): Путь к директории с конфигурационными файлами.
            base_config_name (str): Имя базового файла конфигурации.

        """
        self.cfg_root = cfg_root
        self.base_config_path = os.path.join(cfg_root, base_config_name)
        self.base_config = self._load_yaml(self.base_config_path)

    def _load_yaml(self, path: str) -> dict | None:
        """
        Загружает YAML-файл с конфигурацией.

        Args:
            path (str): Путь к YAML-файлу.

        Returns:
            dict | None: Словарь с конфигурацией или None при ошибках (файл не найден, ошибка парсинга).
        """
        if not path.endswith((".yml", ".yaml")):
            path += ".yaml"

        if not os.path.exists(path):
            logging.warning(f"Файл конфигурации не найден: {path}")
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            logging.info(f"Конфигурация успешно загружена: {path}")
            return config
        except Exception as e:
            logging.error(f"Ошибка при загрузке конфигурации из {path}: {e}")
            return None

    def _merge_configs(
        self, base: dict | None = None, override: dict | None = None
    ) -> dict:
        """
        Рекурсивно объединяет две конфигурации с приоритетом у override.

        Args:
            base (dict | None): Базовая конфигурация.
            override (dict | None): Конфигурация для переопределения.

        Returns:
            dict: Результирующая объединённая конфигурация.
        """
        if not base:
            return override or {}
        if not override:
            return base

        merged = base.copy()
        for k, v in override.items():
            if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
                merged[k] = self._merge_configs(merged[k], v)
            else:
                merged[k] = v
        return merged

    def load_stage_config(self, stage: str, required_keys: list | None = None) -> dict:
        """
        Загружает конфигурацию конкретного этапа, объединяя её с базовой.

        Args:
            stage (str): Имя этапа, например, "extraction".
            required_keys (list[str] | None): Список обязательных ключей, которые должны
                                              присутствовать в итоговой конфигурации.

        Returns:
            dict: Итоговая объединённая конфигурация для этапа.
        """
        stage_config_path = os.path.join(self.cfg_root, f"{stage}_config.yaml")
        stage_config = self._load_yaml(stage_config_path) or {}

        combined_config = self._merge_configs(self.base_config, stage_config)

        if required_keys:
            missing_keys = [key for key in required_keys if key not in combined_config]
            if missing_keys:
                logging.warning(
                    f"Отсутствуют обязательные ключи в конфигурации этапа '{stage}': {missing_keys}"
                )

        return combined_config

    def validate_config(self, config: dict | None, schema: dict | None = None) -> bool:
        """
        Валидирует конфигурацию по заданной схеме ключей и типов.

        Args:
            config (dict): Конфигурация для проверки.
            schema (dict | None): Схема, где ключ — имя ключа конфигурации,
                                 значение — ожидаемый тип (default: validation_schema).

        Returns:
            bool: True, если все ключи присутствуют и типы верны, иначе False.
        """
        if not schema:
            schema = self.validation_schema
        
        if not config:
            config = {}

        valid = True

        for key, expected_type in schema.items():
            if key not in config:
                logging.error(f"Отсутствует обязательный ключ конфигурации: '{key}'")
                valid = False
            else:
                if not isinstance(config[key], expected_type):
                    # Специальная обработка для bool (иногда YAML парсит по-другому)
                    if expected_type == bool and isinstance(config[key], bool):
                        continue
                    logging.error(
                        f"Ключ '{key}' имеет неверный тип: ожидался {expected_type.__name__}, "
                        f"получен {type(config[key]).__name__}"
                    )
                    valid = False

        return valid


if __name__ == "__main__":
    loader = ConfigLoader(cfg_root="cfg")

    if loader.validate_config(loader.base_config):
        logging.info("Конфигурация base_config.yaml загружена и валидирована.")

    stages = [
        "extraction",
        "transformation",
        "validation",
        "profiling",
        "loading",
        "analytics",
        "generation",
        "log",
    ]

    for stage in stages:
        cfg = loader.load_stage_config(stage, required_keys=[f"{stage}_config"])
        if cfg and loader.validate_config(cfg):
            logging.info(f"Конфигурация этапа '{stage}' загружена и валидирована.")
        else:
            logging.warning(f"Проблемы с конфигурацией этапа '{stage}'.")
