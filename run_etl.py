import yaml
import logging
from pathlib import Path
from logger_initializer import LoggerInitializer
from utils.cfg_tool import load_schema, ConfigChecker, ConfigLoader, ConfigValidator


boot_logger = LoggerInitializer(bootstrap_mode=True).bootstrap_logger

CFG_DIR = Path("cfg/")
SCHEMA_PATH = CFG_DIR / "schema/cfg_validation_schema.yaml"

schema = load_schema(SCHEMA_PATH)

cfg_loader = ConfigLoader()
cfg_validator = ConfigValidator(schema=schema)
cfg_checker = ConfigChecker(loader=cfg_loader, validator=cfg_validator, cfg_dir=CFG_DIR)

cfg_checker.validate_all()
