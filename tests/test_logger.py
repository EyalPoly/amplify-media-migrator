import logging
from pathlib import Path

import pytest

from amplify_media_migrator.utils.logger import (
    setup_logging,
    get_logger,
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_DIR,
)


@pytest.fixture(autouse=True)
def _clean_logger():
    logger = logging.getLogger("amplify_media_migrator")
    logger.handlers.clear()
    yield
    logger.handlers.clear()


class TestSetupLogging:
    def test_returns_root_logger(self):
        logger = setup_logging()
        assert logger.name == "amplify_media_migrator"

    def test_sets_log_level(self):
        logger = setup_logging(level="DEBUG")
        assert logger.level == logging.DEBUG

    def test_sets_log_level_case_insensitive(self):
        logger = setup_logging(level="warning")
        assert logger.level == logging.WARNING

    def test_adds_console_handler(self):
        logger = setup_logging()
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    def test_clears_existing_handlers(self):
        logger = setup_logging()
        logger.addHandler(logging.StreamHandler())
        assert len(logger.handlers) == 2

        logger = setup_logging()
        assert len(logger.handlers) == 1

    def test_adds_file_handler(self, tmp_path: Path):
        log_file = tmp_path / "test.log"
        logger = setup_logging(log_file=log_file)

        assert len(logger.handlers) == 2
        assert isinstance(logger.handlers[0], logging.StreamHandler)
        assert isinstance(logger.handlers[1], logging.FileHandler)

    def test_creates_log_directory(self, tmp_path: Path):
        log_file = tmp_path / "nested" / "dir" / "test.log"
        setup_logging(log_file=log_file)

        assert log_file.parent.exists()

    def test_writes_to_log_file(self, tmp_path: Path):
        log_file = tmp_path / "test.log"
        logger = setup_logging(level="INFO", log_file=log_file)

        logger.info("Test message")

        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text()
        assert "Test message" in content
        assert "INFO" in content

    def test_custom_format(self):
        custom_format = "%(levelname)s: %(message)s"
        logger = setup_logging(log_format=custom_format)

        formatter = logger.handlers[0].formatter
        assert formatter is not None
        assert formatter._fmt == custom_format


class TestGetLogger:
    def test_returns_child_logger(self):
        logger = get_logger("test_module")
        assert logger.name == "amplify_media_migrator.test_module"

    def test_returns_different_loggers_for_different_names(self):
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")
        assert logger1 is not logger2
        assert logger1.name != logger2.name

    def test_returns_same_logger_for_same_name(self):
        logger1 = get_logger("same_module")
        logger2 = get_logger("same_module")
        assert logger1 is logger2

    def test_inherits_from_root_logger(self):
        setup_logging(level="DEBUG")
        child_logger = get_logger("child")
        assert child_logger.getEffectiveLevel() == logging.DEBUG


class TestDefaults:
    def test_default_log_format(self):
        assert "%(asctime)s" in DEFAULT_LOG_FORMAT
        assert "%(levelname)s" in DEFAULT_LOG_FORMAT
        assert "%(name)s" in DEFAULT_LOG_FORMAT
        assert "%(message)s" in DEFAULT_LOG_FORMAT

    def test_default_log_dir(self):
        expected = Path.home() / ".amplify-media-migrator" / "logs"
        assert DEFAULT_LOG_DIR == expected


class TestLogLevels:
    @pytest.mark.parametrize(
        "level,expected",
        [
            ("DEBUG", logging.DEBUG),
            ("INFO", logging.INFO),
            ("WARNING", logging.WARNING),
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
        ],
    )
    def test_all_log_levels(self, level: str, expected: int):
        logger = setup_logging(level=level)
        assert logger.level == expected


class TestEnvVarLogLevel:
    def test_reads_log_level_from_env(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "WARNING")
        logger = setup_logging()
        assert logger.level == logging.WARNING

    def test_explicit_level_overrides_env(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "WARNING")
        logger = setup_logging(level="DEBUG")
        assert logger.level == logging.DEBUG

    def test_defaults_to_info_without_env(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        logger = setup_logging()
        assert logger.level == logging.INFO

    def test_invalid_level_falls_back_to_info(self):
        logger = setup_logging(level="NONEXISTENT")
        assert logger.level == logging.INFO
