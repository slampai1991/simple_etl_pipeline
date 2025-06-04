import hashlib
import logging
import logging.handlers
import os
from datetime import datetime

class LoggerInitializer:
    def __init__(self, config):
        self.config = config
        self.pipeline_id = config.get("pipeline_id", "pipeline")
        self.dry_run = config.get("dry_run", False)
        self.log_config = config.get("log_config", {})
        self.date_str = datetime.now().strftime(self.log_config.get("variables", {}).get("date", "%Y-%m-%d"))
        self.hash_str = self._generate_hash()
        self.loggers = {}

    def _generate_hash(self):
        # Пример хэширования pipeline_id и времени для уникальности
        base_str = f"{self.pipeline_id}_{self.date_str}"
        return hashlib.sha1(base_str.encode()).hexdigest()[:8]

    def init_logger(self, stage_name):
        stages = self.log_config.get("stages", {})
        stage_conf = stages.get(stage_name, {})
        if not stage_conf.get("enabled", False):
            return None  # stage logging disabled

        log_dir = stage_conf.get("log_dir", self.log_config.get("log_path", "logs/"))
        os.makedirs(log_dir, exist_ok=True)

        log_file_template = stage_conf.get("log_file", "{date}_{hash}_stage.log")
        log_file_name = log_file_template.format(
            date=self.date_str,
            pipeline_id=self.pipeline_id,
            hash=self.hash_str
        )
        log_file_path = os.path.join(log_dir, log_file_name)

        logger = logging.getLogger(stage_name)
        logger.setLevel(getattr(logging, self.log_config.get("log_level", "INFO").upper(), logging.INFO))
        logger.propagate = False  # чтобы избежать дублирования сообщений

        formatter = logging.Formatter(self.log_config.get("log_format",
                                                          "%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

        # File handler с ротацией
        if self.log_config.get("log_rotation", {}).get("enabled", False):
            when = self.log_config["log_rotation"].get("when", "midnight")
            interval = self.log_config["log_rotation"].get("interval", 1)
            backup_count = self.log_config["log_rotation"].get("backup_count", 7)

            file_handler = logging.handlers.TimedRotatingFileHandler(
                log_file_path,
                when=when,
                interval=interval,
                backupCount=backup_count,
                encoding='utf-8'
            )
        else:
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8')

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        if self.log_config.get("log_to_console", False):
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # Если dry_run — уровень логирования понижаем, чтобы минимизировать запись
        if self.dry_run:
            logger.info("Dry run enabled - no data will be written.")
        
        self.loggers[stage_name] = logger
        return logger


# Пример использования
if __name__ == "__main__":
    import yaml

    with open("base_config.yaml", "r") as f:
        config = yaml.safe_load(f)

    logger_init = LoggerInitializer(config)

    # Инициализация логгера для стадии extraction
    extraction_logger = logger_init.init_logger("extraction")
    if extraction_logger:
        extraction_logger.info("Extraction stage started")
        # ...
        extraction_logger.info("Extraction stage finished")
