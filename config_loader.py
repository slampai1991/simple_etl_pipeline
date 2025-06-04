import yaml
import os
import logging

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


class ConfigLoader:
    """
    Загрузчик конфигураций для модульного ETL-процесса.

    Поддерживает:
    - Загрузку базового файла конфигурации (обычно base_config.yaml)
    - Загрузку конфигураций по этапам (extraction, transformation и т.д.)
    - Рекурсивное объединение конфигураций с приоритетом у этапных настроек
    - Проверку обязательных ключей в итоговой конфигурации

    Пример использования:
    >>> loader = ConfigLoader(cfg_root="cfg")
    >>> full_cfg = loader.load_stage_config("transformation", required_keys=["transformation"])
    """

    def __init__(
        self, cfg_root: str = "cfg", base_config_name: str = "base_config.yaml"
    ):
        self.cfg_root = cfg_root
        self.base_config_path = os.path.join(cfg_root, base_config_name)
        self.base_config = self._load_yaml(self.base_config_path)

    def _load_yaml(self, path: str) -> dict | None:
        """
        Загружает YAML-файл, если он существует. Возвращает None в случае ошибки.

        Args:
            path (str): Путь к файлу конфигурации.

        Returns:
            dict | None: Конфигурация в виде словаря или None при ошибке.
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
        Рекурсивно объединяет две конфигурации (base и override), где override имеет приоритет.

        Args:
            base (dict | None): Базовая конфигурация.
            override (dict | None): Конфигурация, переопределяющая базовую.

        Returns:
            dict: Объединённая конфигурация.
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

    def load_stage_config(
        self, stage: str, required_keys: list | None = None
    ) -> dict:
        """
        Загружает конфигурацию конкретного этапа (например, extraction),
        объединяя её с базовой конфигурацией.

        Args:
            stage (str): Имя этапа (например, "transformation").
            required_keys (list[str] | None): Ключи, которые должны присутствовать в итоговой конфигурации.

        Returns:
            dict: Объединённая конфигурация для заданного этапа.
        """
        stage_config_path = os.path.join(self.cfg_root, stage, f"{stage}_config.yaml")
        stage_config = self._load_yaml(stage_config_path) or {}

        combined_config = self._merge_configs(self.base_config, stage_config)

        if required_keys:
            missing_keys = [key for key in required_keys if key not in combined_config]
            if missing_keys:
                logging.warning(
                    f"Отсутствуют обязательные ключи в конфигурации этапа '{stage}': {missing_keys}"
                )

        return combined_config


# if __name__ == '__main__':
#     loader = ConfigLoader()
#     full_cfg = loader.load_stage_config("generation", required_keys=["generation"])
#     print(full_cfg)
 