import yaml
import logging
from utils import generation

with open("cfg/generation_cfg.yaml", "r", encoding="utf8") as f:
    cfg = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

try:
    datagen = generation.SQLiteGenerator(gen_config=cfg)
    datagen.create_db()
except Exception as e:
    logging.error(f"Ошибка при создании базы данных: {e}")
