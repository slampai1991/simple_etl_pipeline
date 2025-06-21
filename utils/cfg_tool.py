"""
cfg_tool.py

Объединённый модуль CLI для загрузки и валидации конфигурационных файлов ETL-пайплайна.
Содержит два класса:
  - ConfigLoader: загрузка и основная проверка через JSON Schema
  - ConfigValidator: детальная валидация одного или группы файлов

Пример использования:
  python cfg_tool.py --file cfg/base_cfg.yaml
  python cfg_tool.py --all

Опции:
  --file <path>        валидировать один файл
  --all                валидировать все файлы в директории cfg/
  --cfg-dir <dir>      директория с конфигами (по умолчанию cfg/)
  --schema <path>      путь к файлу схемы (по умолчанию cfg/schema/cfg_validation_schema.yaml)
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
    Загрузка полной JSON Schema из YAML-файла.

    :param pathlib.Path `schema_path`: путь к cfg_validation_schema.yaml
    :return dict: загруженная схема
    """
    with open(schema_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class ConfigLoader:
    """
    Класс для базовой загрузки и валидации конфигурации.
    Использует referencing 0.36.2 и Draft7 для $ref.
    """

    def __init__(self, schema: dict):
        """
        :param dict `schema`: Словарь JSON Schema, загруженный из файла
        """
        self.schema = schema
        self.resource = Resource.from_contents(self.schema)
        self.registry = Registry().with_resource("cfg://base", self.resource)
        self._cache = {}  # Кэш загруженных конфигураций {filename: cfg_data}

    def load_config(self, path: Path) -> dict:
        """
        Загружает YAML и выполняет первичную валидацию по JSON Schema.
        Загруженный результат кэшируется, и при повторном обращении возвращает кэшированный результат

        :param pathlib.Path `path`: Путь к YAML-файлу
        :return dict: Данные конфига
        :raises ValidationError: при ошибках валидации
        :raises KeyError: если схема не найдена
        """
        if path in self._cache:
            return self._cache[path.stem]

        config_name = path.stem
        data = yaml.safe_load(path.read_text(encoding="utf-8"))

        if config_name not in self.schema:
            raise KeyError(f"Схема для '{config_name}' не найдена")

        # Формируем полную под-схему для данного конфига
        full_schema = {
            "definitions": self.schema.get("definitions", {}),
            **self.schema[config_name],
        }

        # Валидатор с нашим реестром
        validator = Draft7Validator(schema=full_schema, registry=self.registry)

        # Собираем все ошибки и сортируем по пути
        errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))

        if errors:
            msgs = []
            for err in errors:
                path_str = ".".join(map(str, err.path)) or "<root>"
                msgs.append(f"{path_str}: {err.message}")
            raise ValidationError("\n".join(msgs))

        self._cache[path.stem] = data
        return data

    def get_all_cached_configs(self) -> dict[str, dict]:
        """
        Возвращает кэшированные конфиги

        :return dict[str, dict]: Словарь вида {cfg_name: config_dict}
        """
        return {path.stem: data for path, data in self._cache.items()}


class ConfigValidator:
    """
    Позволяет валидировать один файл или всю директорию.
    """

    def __init__(self, loader: ConfigLoader, cfg_dir: Path):
        """
        :param ConfigLoader `loader`: экземпляр загрузчика ConfigLoader
        :param pathlib.Path `cfg_dir`: директория с YAML-конфигами
        """
        self.loader = loader
        self.cfg_dir = cfg_dir

    def validate_file(self, path: Path) -> bool:
        """
        Валидирует один файл и печатает результат.

        :param pathlib.Path `path`: путь к файлу
        :return bool: True, если успешно, иначе False
        """
        try:
            self.loader.load_config(path)
            logger.info(f"[OK] {path}")
            return True
        except (ValidationError, KeyError) as e:
            logger.error(f"[ERROR] {path}: {e}")
            return False

    def validate_all(self) -> str:
        """
        Валидирует все .yaml-файлы в cfg_dir.

        :return str: Сообщение об успешности валидации, либо сообщение с количеством ошибок.
        """
        files = list(self.cfg_dir.glob("*.yaml"))
        failures = 0
        for f in files:
            if not self.validate_file(f):
                failures += 1

        if failures:
            return f"{failures} файлов не прошли валидацию."

        return "Все конфиги валидны."


def run_cli():
    """
    CLI для загрузки и валидации конфигов ETL-пайплайна.
    """
    # CLI-парсер
    parser = argparse.ArgumentParser(
        description="CLI для загрузки и валидации YAML-конфигов ETL-пайплайна",
    )
    parser.add_argument("--file", type=Path, help="Одиночный файл для валидации")
    parser.add_argument(
        "--all", action="store_true", help="Валидировать все файлы в директории"
    )
    parser.add_argument(
        "--cfg-dir", type=Path, default=Path("cfg"), help="Директория с конфигами"
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path("cfg/schema/cfg_validation_schema.yaml"),
        help="Путь к файлу JSON Schema",
    )

    args = parser.parse_args()

    schema = load_schema(args.schema)
    loader = ConfigLoader(schema)
    validator = ConfigValidator(loader, args.cfg_dir)

    if args.all:
        result = validator.validate_all()
        logger.info(result)
        if "не прошли" in result:
            exit(1)
    elif args.file:
        success = validator.validate_file(args.file)
        if not success:
            exit(1)
    else:
        parser.error("Укажите --file или --all")


if __name__ == "__main__":
    run_cli()
