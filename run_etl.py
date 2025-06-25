# # import yaml
# # import logging
# # from pathlib import Path
# # from analytics import analysis, visualization
# # from utils import validation, profiling
# # from utils.cfg_tool import ConfigLoader, ConfigValidator, load_schema
# # from utils.generation import SQLiteGenerator
# # from src import extract, transform, load

# # logging.basicConfig(
# #     level=logging.INFO,
# #     format="%(asctime)s - %(levelname)s - %(message)s",
# #     datefmt="%H:%M:%S",
# # )

# # CONFIG_DIR = Path("cfg/")
# # SCHEMA_PATH = CONFIG_DIR / "schema/cfg_validation_schema.yaml"


# # def validate_configs():
# #     schema = load_schema(SCHEMA_PATH)
# #     loader = ConfigLoader(schema)
# #     validator = ConfigValidator(loader, CONFIG_DIR)

# #     result = validator.validate_all()
# #     print(result)

# #     if "не прошли" in result:
# #         raise ValueError("Ошибка валидации конфигурационных файлов")


# # def load_yaml(path: Path):
# #     return yaml.safe_load(path.read_text(encoding="utf-8"))


# # def main():
# #     """
# #     Запуск всего проекта. По умолчанию все настроено под SQLite.
# #     """
# #     print("\n📦 Запуск ETL-пайплайна\n")

# #     # === Валидация конфигов ===
# #     try:
# #         validate_configs()
# #     except Exception as e:
# #         logging.error(f"Валидация конфигов завершилась с ошибкой: {e}")
# #         return

# #     base_cfg = load_yaml(CONFIG_DIR / "base_cfg.yaml")
# #     gen_cfg = load_yaml(CONFIG_DIR / "generation_cfg.yaml")
# #     ext_cfg = load_yaml(CONFIG_DIR / "extraction_cfg.yaml")
# #     tr_cfg = load_yaml(CONFIG_DIR / "transformation_cfg.yaml")
# #     val_cfg = load_yaml(CONFIG_DIR / "validation_cfg.yaml")
# #     prof_cfg = load_yaml(CONFIG_DIR / "profiling_cfg.yaml")
# #     load_cfg = load_yaml(CONFIG_DIR / "load_cfg.yaml")
# #     anl_cfg = load_yaml(CONFIG_DIR / "analytics_cfg.yaml")

# #     # === Генерация базы данных ===
# #     gen_input = input("Создать тестовую БД? (y/n): ").strip().lower()
# #     match gen_input:
# #         case "y":
# #             db_name = input(
# #                 "Введите имя БД (Enter для использования из конфига): "
# #             ).strip()
# #             db_path = input(
# #                 "Введите путь для сохранения БД (Enter для использования из конфига): "
# #             ).strip()
# #             if not db_path:
# #                 db_path = gen_cfg["sqlite"]["db_path"]
# #             SQLiteGenerator(gen_cfg["sqlite"]).create_db(db_name=db_name)
# #         case "n":
# #             db_path = input(
# #                 "Введите путь к существующей БД (Enter для использования из конфига): "
# #             ).strip()
# #             if not db_path:
# #                 db_path = ext_cfg["sqlite"]["db_path"]
# #         case _:
# #             pass

# #     # === Извлечение данных ===
# #     print("\n🚛 Извлечение данных...")
# #     extractor = extract.SQLiteExtractor(sqlite_cfg=ext_cfg["sqlite"])
# #     raw_data = extractor.extract()

# #     # === Трансформация (с выбором операций) ===
# #     print("\n🧪 Доступные трансформации: drop_nulls, normalize, и т.д.")
# #     t_ops = input(
# #         "Введите через запятую нужные операции (или оставьте пустым для пропуска): "
# #     ).strip()
# #     if t_ops:
# #         operations = [op.strip() for op in t_ops.split(",")]
# #         transformer = transform.DataTransformer(tr_cfg)
# #         transformer.transform(extractor.extract(), operations)

# #     # === Валидация ===
# #     print("\n✔️ Валидация данных (жестко задана через конфиг)...")
# #     validate.run(base_cfg, db_path)

# #     # === Профилирование (по желанию) ===
# #     prof_input = input("Профилировать данные? (y/n): ").strip().lower()
# #     if prof_input == "y":
# #         path = input(
# #             "Путь для сохранения профиля (Enter — использовать конфиг): "
# #         ).strip()
# #         profile_path = path or base_cfg["profile"]["output_path"]
# #         profile.run(db_path, profile_path)

# #     # === Загрузка (по желанию или по конфигу) ===
# #     print("\n📥 Загрузка данных в целевое хранилище...")
# #     target_path = input(
# #         "Введите путь для загрузки (Enter — использовать конфиг): "
# #     ).strip()
# #     target_path = target_path or base_cfg["load"]["target_path"]
# #     load.run(db_path, target_path)

# #     # === Аналитика (по желанию) ===
# #     if input("Выполнить аналитику? (y/n): ").strip().lower() == "y":
# #         if input("Ввести SQL-запрос вручную? (y/n): ").strip().lower() == "y":
# #             query = input("Введите SQL-запрос: ")
# #             analyze.run_custom_query(db_path, query)
# #         else:
# #             print("Запросы из конфига:")
# #             for i, q in enumerate(base_cfg["analytics"]["queries"]):
# #                 print(f"{i}: {q['name']}")
# #             q_idx = int(input("Выберите номер запроса: "))
# #             analyze.run_configured_query(
# #                 db_path, base_cfg["analytics"]["queries"][q_idx]
# #             )

# #     # === Визуализация (по желанию) ===
# #     if input("Визуализировать результаты? (y/n): ").strip().lower() == "y":
# #         visualize.run(db_path)

# #     print("\n✅ Пайплайн завершён успешно.")


# # if __name__ == "__main__":
# #     try:
# #         main()
# #     except Exception as e:
# #         logging.exception("❌ Ошибка выполнения пайплайна:")


# """
# main.py

# Основной запуск для проверки работоспособности logger_initializer и cfg_tool.
# Можно запускать напрямую из VS Code без CLI.
# """

# import logging
# from pathlib import Path

# from logger_initializer import init_logger
# from cfg_tool import load_schema, ConfigLoader, ConfigValidator

# def main():
#     # Инициализация логгера
#     init_logger(level=logging.DEBUG)
#     logger = logging.getLogger(__name__)
#     logger.info("Инициализация логгера завершена.")

#     # Параметры
#     cfg_dir = Path("cfg")
#     schema_path = Path("cfg/schema/cfg_validation_schema.yaml")

#     # Проверка наличия схемы
#     if not schema_path.exists():
#         logger.error(f"Файл схемы не найден: {schema_path}")
#         return

#     # Загрузка схемы
#     try:
#         schema = load_schema(schema_path)
#     except Exception as e:
#         logger.exception(f"Ошибка при загрузке схемы: {e}")
#         return

#     # Инициализация загрузчика и валидатора
#     loader = ConfigLoader(schema)
#     validator = ConfigValidator(loader, cfg_dir)

#     # Запуск валидации всех конфигов
#     result = validator.validate_all()
#     logger.info(result)

# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from utils.cfg_tool import ConfigLoader, ConfigValidator
from logger_initializer import LoggerInitializer

def parse_args():
    parser = argparse.ArgumentParser(
        description="Основной скрипт ETL-пайплайна: загрузка конфигов, их валидация и инициализация логирования"
    )
    parser.add_argument(
        "--base",
        "-b",
        type=Path,
        required=True,
        help="Путь к base_cfg.yaml",
    )
    parser.add_argument(
        "--log",
        "-l",
        type=Path,
        required=True,
        help="Путь к log_cfg.yaml",
    )
    parser.add_argument(
        "--schema",
        "-s",
        type=Path,
        default=Path("cfg/schema/cfg_validation_schema.yaml"),
        help="Путь к схеме JSON Schema",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Запуск в bootstrap-режиме (игнорирует log_cfg)",
    )
    return parser.parse_args()

def main():
    args = parse_args()

    # 1. Загрузка схемы и подготовка валидатора
    schema = load_schema(args.schema)
    validator = ConfigValidator(schema)

    # 2. Загрузка и валидация base_cfg
    loader = ConfigLoader()
    try:
        base_cfg = loader.load_config(args.base)
        validator.validate(base_cfg, args.base.stem)
    except Exception as e:
        print(f"[ERROR] При обработке base_cfg: {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Загрузка и валидация log_cfg (если не bootstrap)
    if args.bootstrap:
        log_cfg = {}
    else:
        try:
            log_cfg = loader.load_config(args.log)
            validator.validate(log_cfg, args.log.stem)
        except Exception as e:
            print(f"[ERROR] При обработке log_cfg: {e}", file=sys.stderr)
            sys.exit(1)

    # 4. Инициализация логирования
    cfg = {"base_cfg": base_cfg, "log_cfg": log_cfg}
    logger_init = LoggerInitializer(cfg, bootstrap_mode=args.bootstrap)

    # 5. Пример инициализации логгеров для стадий
    stages = log_cfg.get("stages", {}) if not args.bootstrap else {}
    loggers = {}
    for stage in stages:
        lg = logger_init.init_logger(stage)
        if lg:
            loggers[stage] = lg

    # 6. Демонстрация логирования результата загрузки
    # (пример: берем источники из base_cfg и логируем)
    sources = base_cfg.get("sources", [])
    loading_logger = loggers.get("loading")
    if loading_logger:
        for src in sources:
            name = src.get("name", "<unknown>")
            enabled = src.get("enabled", False)
            success = src.get("success", None)
            error = src.get("error", None)
            tables = src.get("tables_loaded", 0)
            rows = src.get("rows_loaded", 0)
            logger_init.log_loading_result(
                loading_logger, name, enabled, success, error, tables, rows
            )

    # TODO: здесь разместить остальную логику пайплайна

if __name__ == "__main__":
    from utils.cfg_tool import load_schema
    main()
