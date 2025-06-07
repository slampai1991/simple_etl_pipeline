import yaml
import os
import logging
import datetime
import pprint as pp
from analytics import analysis, visualization
from src import extract, transform, load
from utils import generation, validation, profiling

try:
    with open("config.yaml", "r", encoding="utf8") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    print("Файл конфигурации не найден.")
    exit(1)


log_dir = config["log_config"]["log_path"]
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

log_filename = os.path.join(
    log_dir, f"{__name__}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

try:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(),
        ],
    )
except PermissionError:
    print(f"Отказано в доступе при создании файла {log_filename}")
    print("По умолчанию будет использоваться только вывод в консоль")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


try:
    # datagen = utils.DataGenerator(config=config)
    # datagen.generate_sqlite()

    extractor = extract.DataExtractor(config=config)
    transformer = transform.DataTransformer(config=config)
    validator = validation.DataValidator(validation_config=config["validation_config"])
    profiler = profiling.DataProfiler(profiling_config=config["profiling_config"])
    loader = load.SQLiteLoader(load_config=config["load_config"]["sqlite"])

    raw_data = extractor.extract_sqlite(query='SELECT id, name, action, timestamp action_date FROM users u JOIN user_actions ua ON u.id = ua.user_id')
    transfromed_data = transformer.transform(raw_data)
    validated_data = validator.run_all_validations(transfromed_data)
    pp.pprint(validated_data)
    print(transfromed_data == validated_data)
    loader.load_all(validated_data)
    logging.info("Скрипт успешно выполнен")

except Exception as e:
    logging.error(f"Ошибка при выполнении скрипта: {e}")
    raise
