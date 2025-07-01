import yaml
import logging
from pathlib import Path
from logger_initializer import LoggerInitializer
from utils.cfg_tool import load_schema, ConfigChecker, ConfigLoader, ConfigValidator
from utils import generation, validation, profiling


boot_logger = LoggerInitializer(bootstrap_mode=True).bootstrap_logger

CFG_DIR = Path("cfg/")
SCHEMA_PATH = CFG_DIR / "schema/cfg_validation_schema.yaml"

schema = load_schema(SCHEMA_PATH)

cfg_loader = ConfigLoader()
cfg_validator = ConfigValidator(schema=schema)
cfg_checker = ConfigChecker(loader=cfg_loader, validator=cfg_validator, cfg_dir=CFG_DIR)

cfg_checker.validate_all()

base_cfg = cfg_loader.load_config(CFG_DIR / "base_cfg.yaml")
log_cfg = cfg_loader.load_config(CFG_DIR / "log_cfg.yaml")

# Generation (optional)
gen_logger = LoggerInitializer(
    cfg={"base_cfg": base_cfg, "log_cfg": log_cfg}
).init_logger(stage_name="generation")

gen_cfg = cfg_loader.load_config(CFG_DIR / "generation_cfg.yaml")
data_gen = generation.SQLiteGenerator(gen_config=gen_cfg, logger=gen_logger)

data_gen.create_db()
