import yaml
import logging
from utils import generation
from src import extract

with open("cfg/extraction_cfg.yaml", "r", encoding="utf8") as f:
    cfg = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

try:
    datagen = generation.SQLiteGenerator(gen_config=cfg)
    datagen.create_db()

    # extractor = extract.DataExtractor(load_cfg=cfg)
    # raw_data = extractor.extract(source='sqlite')
except Exception as e:
    raise e