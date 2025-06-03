import yaml
import os
import logging
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

class ConfigLoader:
    def __init__(self, cfg_root: str = "cfg", base_config_name: str = "base_config.yml"):
        self.cfg_root = cfg_root
        self.base_config_path = os.path.join(cfg_root, base_config_name)
        self.base_config = self._load_yaml(self.base_config_path)

    def _load_yaml(self, path: str) -> Optional[Dict]:
        if not os.path.exists(path):
            logging.warning(f"Config file not found: {path}")
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            logging.info(f"Loaded config: {path}")
            return config
        except Exception as e:
            logging.error(f"Failed to load config {path}: {e}")
            return None

    def _merge_configs(self, base: Dict, override: Dict) -> Dict:
        """Рекурсивно объединяет два словаря, приоритет — у override."""
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

    def load_stage_config(self, stage: str, required_keys: Optional[list] = None) -> Dict:
        """Загрузить конфиг для конкретного этапа с объединением базового и проверкой ключей."""
        stage_config_path = os.path.join(self.cfg_root, stage, f"{stage}_config.yml")
        stage_config = self._load_yaml(stage_config_path) or {}

        combined_config = self._merge_configs(self.base_config, stage_config)

        if required_keys:
            missing_keys = [key for key in required_keys if key not in combined_config]
            if missing_keys:
                logging.warning(f"Missing required keys in combined config for stage '{stage}': {missing_keys}")

        return combined_config

# Пример использования:
if __name__ == "__main__":
    loader = ConfigLoader()
    # Например, для этапа extraction ожидаем наличие ключа "api" в конфиге
    extraction_cfg = loader.load_stage_config("extraction", required_keys=["api"])
    print(extraction_cfg)
