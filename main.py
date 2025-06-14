import yaml
import logging
from utils import generation
from src import extract


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

try:
    pass
except Exception as e:
    raise e