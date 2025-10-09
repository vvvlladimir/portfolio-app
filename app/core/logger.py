import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from app.core.config import settings

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] — %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger("app")

logger.setLevel(settings.LOG_LEVEL)
logging.getLogger("sqlalchemy.engine").setLevel(settings.SQL_LOG_LEVEL)
logging.getLogger("uvicorn.access").setLevel(settings.UVICORN_LOG_LEVEL)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
logger.addHandler(console_handler)

file_handler = RotatingFileHandler(
    LOG_DIR / "app.log",
    maxBytes=5_000_000,
    backupCount=5,
    encoding="utf-8"
)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
logger.addHandler(file_handler)

logger.propagate = False

