import logging

from mini_pipeline.common.config import config

log_level = config.logging.get("level", "INFO")
log_format = config.logging.get("format", "[%(asctime)s] %(levelname)s - %(name)s - %(message)s")

logging.basicConfig(level=log_level, format=log_format)

logger = logging.getLogger("app")
