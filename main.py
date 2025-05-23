import yaml
import os
import logging
import datetime
import src.extract as extract
import src.transform as transform
import src.load as load
import src.utils as utils

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
    datagen = utils.DataGenerator(config)
    datagen.generate_sqlite()
    extractor = extract.DataExtractor(config)
    logs = extractor.extract_sqlite(query="SELECT * FROM logs")
    transformer = transform.DataTransformer(config)
    cleaned_data = transformer.transform_sqlite(logs)
    validator = utils.DataValidator(config["validation"])
    cleaned_data = validator.run_all_validations(cleaned_data)
    # loader = load.PostgresLoader(config)
    # loader.load_dataframes(cleaned_data)
except Exception as e:
    logging.error(f"Ошибка при выполнении скрипта: {e}")
    raise
