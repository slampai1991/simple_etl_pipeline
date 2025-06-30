"""
cfg_tool.py

Модуль для загрузки и валидации YAML-конфигураций ETL-пайплайна.
Содержит два класса:
  - ConfigLoader: отвечает за чтение и кэширование YAML-файлов
  - ConfigValidator: проводит валидацию по JSON Schema

Пример использования:
  from cfg_tool import load_schema, ConfigLoader, ConfigValidator

  CFG_DIR = Path("cfg/")
  SCHEMA_PATH = CFG_DIR / "schema/cfg_validation_schema.yaml"

  schema = load_schema(SCHEMA_PATH)
  
  cfg_loader = ConfigLoader()
  cfg_validator = ConfigValidator(schema=schema)
  cfg_checker = ConfigChecker(loader=cfg_loader, validator=cfg_validator, cfg_dir=CFG_DIR)
  
  cfg_checker.validate_all()

SOON ->
`Также может быть использован как CLI:
  python cfg_tool.py --file cfg/base_cfg.yaml
  python cfg_tool.py --all

CLI-параметры:
  --file <path>        валидировать один файл
  --all                валидировать все файлы в директории cfg/
  --cfg-dir <dir>      директория с конфигами (по умолчанию cfg/)
  --schema <path>      путь к файлу схемы (по умолчанию cfg/schema/cfg_validation_schema.yaml)`
"""

import yaml
import argparse
import logging
from pathlib import Path
from jsonschema import Draft7Validator, ValidationError
from referencing import Registry, Resource

logger = logging.getLogger(__name__)


def load_schema(schema_path: Path) -> dict:
    """
    Загружает JSON Schema из YAML-файла.

    :param pathlib.Path `schema_path`: путь к файлу схемы
    :returns `dict`: JSON Schema как словарь
    """
    with open(schema_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class ConfigLoader:
    """
    Класс для загрузки и кэширования YAML-конфигураций.
    """

    def __init__(self):
        self._cache: dict[Path, dict] = {}  # Инициализация пустого кэша

    def load_config(self, path: Path) -> dict:
        """
        Загружает YAML-конфиг и кэширует результат.

        :param pathlib.Path `path`: путь к YAML-файлу
        :returns `dict`: содержимое конфига
        :raises `yaml.YAMLError`: при ошибке разбора YAML
        :raises `FileNotFoundError`: если файл не существует
        """
        resolved_path = path.resolve()
        if resolved_path in self._cache:
            logger.info(f"Загружена сохраненная конфигурация: {resolved_path.stem}")
            return self._cache[resolved_path]

        try:
            with open(resolved_path, "r", encoding="utf8") as f:
                data = yaml.safe_load(f)
            self._cache[resolved_path] = data
            logger.info(f"Конфиг успешно загружен: {resolved_path}")
            return data
        except yaml.YAMLError as e1:
            logger.error(f"Ошибка при загрузке файла конфигурации: {e1}")
            raise
        except FileNotFoundError as e2:
            logger.error(f"Файл конфигурации не найден: {e2}")
            raise


class ConfigValidator:
    """
    Класс для валидации конфигураций по JSON Schema.
    """

    def __init__(self, schema: dict):
        """
        :param dict `schema`: загруженная JSON Schema
        """
        self.schema = schema
        # Создаем Resource объект из схемы для использования в валидации
        self.resource = Resource.from_contents(schema)
        # Регистрируем схему в реестре с URI "cfg://base" для разрешения ссылок
        self.registry = Registry().with_resource("cfg://base", self.resource)

    def validate(self, config: dict, config_name: str) -> None:
        """
        Валидирует YAML-конфигурацию по схеме.

        :param dict `config`: данные из YAML-файла
        :param str `config_name`: ключ схемы (обычно stem от имени файла)
        :raises `ValidationError`: при ошибках валидации
        :raises `KeyError`: если нет схемы для указанного config_name
        """
        if config_name not in self.schema:
            raise KeyError(f"Схема для '{config_name}' не найдена")

        full_schema = {
            "definitions": self.schema.get("definitions", {}),
            **self.schema[config_name],
        }

        validator = Draft7Validator(schema=full_schema, registry=self.registry)
        errors = sorted(validator.iter_errors(config), key=lambda e: list(e.path))

        if errors:
            msgs = []
            for err in errors:
                path_str = ".".join(map(str, err.path)) or "<root>"
                msgs.append(f"{path_str}: {err.message}")
            raise ValidationError("\n".join(msgs))


class ConfigChecker:
    """
    Класс для запуска валидации одного или всех YAML-конфигов в директории.
    """

    def __init__(self, loader: ConfigLoader, validator: ConfigValidator, cfg_dir: Path):
        """
        :param ConfigLoader `loader`: загрузчик конфигов
        :param ConfigValidator `validator`: валидатор конфигов
        :param pathlib.Path `cfg_dir`: директория с конфигурациями
        """
        self.loader = loader
        self.validator = validator
        self.cfg_dir = cfg_dir

    def validate_file(self, path: Path) -> bool:
        """
        Валидирует один файл и логирует результат.

        :param pathlib.Path `path`: путь к YAML-файлу
        :returns `bool`: True если успешно, иначе False
        """
        try:
            config = self.loader.load_config(path)
            self.validator.validate(config, path.stem)
            logger.info(f"[OK] {path}")
            return True
        except (ValidationError, KeyError, yaml.YAMLError) as e:
            logger.error(f"[ERROR] {path}: {e}")
            return False

    def validate_all(self) -> str:
        """
        Валидирует все YAML-файлы в директории.

        :returns `str`: результат валидации (успех или количество ошибок)
        """
        files = list(self.cfg_dir.glob("*.yaml"))
        failures = 0

        for f in files:
            if not self.validate_file(f):
                failures += 1

        if failures:
            return f"{failures} файлов не прошли валидацию."
        return "Все конфиги валидны."
