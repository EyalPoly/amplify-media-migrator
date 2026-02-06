import logging
import os
import sys
from pathlib import Path
from typing import Optional

DEFAULT_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
DEFAULT_LOG_DIR = Path.home() / ".amplify-media-migrator" / "logs"


def setup_logging(
    level: Optional[str] = None,
    log_file: Optional[Path] = None,
    log_format: str = DEFAULT_LOG_FORMAT,
) -> logging.Logger:
    resolved_level = level or os.environ.get("LOG_LEVEL", "INFO")
    root_logger = logging.getLogger("amplify_media_migrator")
    root_logger.setLevel(getattr(logging, resolved_level.upper(), logging.INFO))

    root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(console_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format))
        root_logger.addHandler(file_handler)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"amplify_media_migrator.{name}")
