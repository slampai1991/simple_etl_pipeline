import yaml
import os
import logging
import datetime
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


# Example usage
# try:
#     logging.info("Начало работы программы.")
#     data_generator = utils.DataGenerator(config)
#     data_generator.generate_sqlite(config["sqlite_config"]["db_name"])
#     data_generator.generate_csv(config["csv_config"]["filename"])
#     data_generator.generate_mongo(config["mongo_config"]["db_name"])
#     logging.info("Конец работы программы.")
# except Exception as e:
#     logging.error(f"Произошла ошибка: {e}")
#     raise e
