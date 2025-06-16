import yaml
import logging
from utils.generation import SQLiteGenerator
from src import extract


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

try:
    datagen = SQLiteGenerator({})
except Exception as e:
    raise e