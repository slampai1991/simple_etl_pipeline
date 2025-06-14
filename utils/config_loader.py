"""
config_tools.py

Объединённый модуль CLI для загрузки и валидации конфигурационных файлов ETL-пайплайна.
Содержит два класса:
  - ConfigLoader: загрузка и основная проверка через JSON Schema
  - ConfigValidator: детальная валидация одного или группы файлов

Пример использования:
  python config_tools.py --file cfg/base_cfg.yaml
  python config_tools.py --all

Опции:
  --file <path>        валидировать один файл
  --all                валидировать все файлы в директории cfg/
  --cfg-dir <dir>      директория с конфигами (по умолчанию cfg/)
  --schema <path>      путь к файлу схемы (по умолчанию cfg/schema/cfg_validation_schema.yaml)
"""

import yaml
import glob
import argparse
from pathlib import Path
from jsonschema import Draft7Validator, RefResolver, ValidationError


def load_schema(schema_path: Path) -> dict:
    """
    Загрузка полной JSON Schema из YAML-файла.

    :param schema_path: Путь к файлу cfg_validation_schema.yaml
    :return: Словарь с загруженной схемой
    """
    with open(schema_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class ConfigLoader:
    """
    Класс для базовой загрузки и валидации конфигурации.
    Использует jsonschema для проверки соответствия базовым правилам.
    """

    def __init__(self, schema: dict):
        """
        :param schema: Словарь JSON Schema, загруженный из файла
        """
        self.schema = schema
        # Резольвер для ссылок на definitions
        self.resolver = RefResolver.from_schema(
            {"definitions": schema.get("definitions", {})}
        )

    def load_config(self, path: Path) -> dict:
        """
        Загружает YAML и выполняет первичную валидацию по JSON Schema.

        :param path: Путь к YAML-файлу
        :return: Словарь с загруженными данными конфига
        :raises ValidationError: при ошибках валидации
        :raises KeyError: если схема для данного типа конфига не найдена
        """
        config_name = path.stem
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if config_name not in self.schema:
            raise KeyError(f"Схема для '{config_name}' не найдена")
        # Собираем специфичную схему
        full_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            **self.schema["definitions"],
            **{config_name: self.schema[config_name]},
        }
        validator = Draft7Validator(full_schema, resolver=self.resolver)
        errors = sorted(validator.iter_errors(data), key=lambda e: e.path)
        if errors:
            msgs = []
            for err in errors:
                path_str = ".".join(map(str, err.path)) or "<root>"
                msgs.append(f"{path_str}: {err.message}")
            raise ValidationError("\n".join(msgs))
        return data


class ConfigValidator:
    """
    CLI-обёртка для ConfigLoader.
    Позволяет валидировать одиночные файлы или все файлы в директории.
    """

    def __init__(self, loader: ConfigLoader, cfg_dir: Path):
        """
        :param loader: экземпляр ConfigLoader
        :param cfg_dir: директория с YAML-конфигами
        """
        self.loader = loader
        self.cfg_dir = cfg_dir

    def validate_file(self, path: Path) -> bool:
        """
        Валидирует один файл и печатает результат.

        :param path: Путь к файлу
        :return: True, если успешно, иначе False
        """
        try:
            self.loader.load_config(path)
            print(f"[OK] {path}")
            return True
        except (ValidationError, KeyError) as e:
            print(f"[ERROR] {path}: {e}")
            return False

    def validate_all(self) -> None:
        """
        Валидирует все .yaml файлы в cfg_dir.
        Завершает процесс с кодом 1, если есть ошибки.
        """
        files = list(self.cfg_dir.glob("*.yaml"))
        failures = 0
        for f in files:
            if not self.validate_file(f):
                failures += 1
        if failures:
            print(f"{failures} файлов не прошли валидацию.")
            exit(1)
        print("Все конфиги валидны.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="CLI для загрузки и валидации YAML-конфигов ETL-пайплайна"
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
        help="Путь к JSON Schema",
    )
    args = parser.parse_args()

    schema = load_schema(args.schema)
    loader = ConfigLoader(schema)
    validator = ConfigValidator(loader, args.cfg_dir)

    if args.all:
        validator.validate_all()
    elif args.file:
        success = validator.validate_file(args.file)
        if not success:
            exit(1)
    else:
        parser.error("Укажите --file или --all")
