import yaml
import os
import logging
import datetime
import pprint as pp
from analytics import analysis, visualization
from src import extract, transform, load
from utils import generation, validation, profiling


try:
    with open("cfg/generation_config.yaml", "r", encoding="utf8") as f:
        config = yaml.safe_load(f)
    datagen = generation.SQLiteGenerator(gen_config=config["sqlite"])
    datagen.generate_db()

except Exception as e:
    logging.error(f"Ошибка при выполнении скрипта: {e}")
    raise
