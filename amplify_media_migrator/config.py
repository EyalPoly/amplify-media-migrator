import json
import logging
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import click

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    pass


@dataclass
class GoogleDriveConfig:
    folder_id: str = ""
    credentials_path: str = "~/.amplify-media-migrator/google_credentials.json"
    token_path: str = "~/.amplify-media-migrator/google_token.json"


@dataclass
class CognitoConfig:
    user_pool_id: str = ""
    client_id: str = ""
    identity_pool_id: str = ""
    username: str = ""


@dataclass
class AmplifyConfig:
    api_endpoint: str = ""
    storage_bucket: str = ""


@dataclass
class AWSConfig:
    region: str = "us-east-1"
    cognito: CognitoConfig = field(default_factory=CognitoConfig)
    amplify: AmplifyConfig = field(default_factory=AmplifyConfig)


@dataclass
class MigrationConfig:
    concurrency: int = 10
    retry_attempts: int = 3
    retry_delay_seconds: int = 5
    chunk_size_mb: int = 8
    default_media_public: bool = False


@dataclass
class Config:
    google_drive: GoogleDriveConfig = field(default_factory=GoogleDriveConfig)
    aws: AWSConfig = field(default_factory=AWSConfig)
    migration: MigrationConfig = field(default_factory=MigrationConfig)

    def validate(self) -> None:
        errors: List[str] = []
        if self.migration.concurrency <= 0:
            errors.append("migration.concurrency must be > 0")
        if self.migration.retry_attempts < 0:
            errors.append("migration.retry_attempts must be >= 0")
        if self.migration.retry_delay_seconds < 0:
            errors.append("migration.retry_delay_seconds must be >= 0")
        if self.migration.chunk_size_mb <= 0:
            errors.append("migration.chunk_size_mb must be > 0")
        if errors:
            raise ConfigurationError(
                "Configuration validation failed:\n"
                + "\n".join(f"  - {e}" for e in errors)
            )


def validate_config(config: Config) -> None:
    config.validate()


def config_to_dict(config: Config) -> dict:
    return asdict(config)


def config_from_dict(data: dict) -> Config:
    gd_data = data.get("google_drive", {})
    google_drive = GoogleDriveConfig(
        folder_id=gd_data.get("folder_id", ""),
        credentials_path=gd_data.get(
            "credentials_path", "~/.amplify-media-migrator/google_credentials.json"
        ),
        token_path=gd_data.get(
            "token_path", "~/.amplify-media-migrator/google_token.json"
        ),
    )

    aws_data = data.get("aws", {})
    cognito_data = aws_data.get("cognito", {})
    cognito = CognitoConfig(
        user_pool_id=cognito_data.get("user_pool_id", ""),
        client_id=cognito_data.get("client_id", ""),
        identity_pool_id=cognito_data.get("identity_pool_id", ""),
        username=cognito_data.get("username", ""),
    )

    amplify_data = aws_data.get("amplify", {})
    amplify = AmplifyConfig(
        api_endpoint=amplify_data.get("api_endpoint", ""),
        storage_bucket=amplify_data.get("storage_bucket", ""),
    )

    aws = AWSConfig(
        region=aws_data.get("region", "us-east-1"),
        cognito=cognito,
        amplify=amplify,
    )

    mig_data = data.get("migration", {})
    migration = MigrationConfig(
        concurrency=mig_data.get("concurrency", 10),
        retry_attempts=mig_data.get("retry_attempts", 3),
        retry_delay_seconds=mig_data.get("retry_delay_seconds", 5),
        chunk_size_mb=mig_data.get("chunk_size_mb", 8),
        default_media_public=mig_data.get("default_media_public", False),
    )

    return Config(google_drive=google_drive, aws=aws, migration=migration)


class ConfigManager:
    """Manages configuration loading, saving, and access for the media migrator."""

    DEFAULT_CONFIG_DIR = Path.home() / ".amplify-media-migrator"
    DEFAULT_CONFIG_FILE = "config.json"

    def __init__(self, config_path: Optional[Path] = None) -> None:
        self._config_path = config_path or (
            self.DEFAULT_CONFIG_DIR / self.DEFAULT_CONFIG_FILE
        )
        self._config: Optional[Config] = None

    @property
    def config_path(self) -> Path:
        return self._config_path

    @property
    def config(self) -> Config:
        if self._config is None:
            self._config = Config()
        return self._config

    def ensure_config_dir(self) -> None:
        self._config_path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Config:
        if not self._config_path.exists():
            raise ConfigurationError(
                f"Configuration file not found: {self._config_path}\n"
                "Run 'amplify-media-migrator config' to create one."
            )

        try:
            raw = self._config_path.read_text(encoding="utf-8")
        except OSError as e:
            raise ConfigurationError(f"Failed to read configuration file: {e}") from e

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON in configuration file {self._config_path}: {e}"
            ) from e

        config = config_from_dict(data)

        aws_region = os.environ.get("AWS_REGION")
        if aws_region:
            config.aws.region = aws_region
            logger.debug("Overriding aws.region from AWS_REGION environment variable")

        api_endpoint = os.environ.get("AMPLIFY_API_ENDPOINT")
        if api_endpoint:
            config.aws.amplify.api_endpoint = api_endpoint
            logger.debug(
                "Overriding aws.amplify.api_endpoint from "
                "AMPLIFY_API_ENDPOINT environment variable"
            )

        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_path:
            config.google_drive.credentials_path = credentials_path
            logger.debug(
                "Overriding google_drive.credentials_path from "
                "GOOGLE_APPLICATION_CREDENTIALS environment variable"
            )

        config.validate()
        self._config = config
        logger.info("Configuration loaded from %s", self._config_path)
        return config

    def save(self) -> None:
        self.ensure_config_dir()
        if self._config is None:
            self._config = Config()
        data = config_to_dict(self._config)
        try:
            self._config_path.write_text(
                json.dumps(data, indent=2) + "\n", encoding="utf-8"
            )
        except OSError as e:
            raise ConfigurationError(f"Failed to write configuration file: {e}") from e
        logger.info("Configuration saved to %s", self._config_path)

    def get(self, key: str, default: Any = None) -> Any:
        if self._config is None:
            try:
                self.load()
            except ConfigurationError:
                self._config = Config()
        segments = key.split(".")
        obj: Any = self._config
        for segment in segments:
            if not hasattr(obj, segment):
                return default
            obj = getattr(obj, segment)
        return obj

    def set(self, key: str, value: Any) -> None:
        if self._config is None:
            try:
                self.load()
            except ConfigurationError:
                self._config = Config()

        segments = key.split(".")
        if len(segments) < 1:
            raise ConfigurationError(f"Invalid configuration key: {key}")

        obj: Any = self._config
        for segment in segments[:-1]:
            if not hasattr(obj, segment):
                raise ConfigurationError(
                    f"Invalid configuration key: {key} "
                    f"(unknown segment '{segment}')"
                )
            obj = getattr(obj, segment)

        final = segments[-1]
        if not hasattr(obj, final):
            raise ConfigurationError(
                f"Invalid configuration key: {key} " f"(unknown segment '{final}')"
            )
        setattr(obj, final, value)

    def update(self, key: str, value: Any) -> None:
        self.set(key, value)

    def get_or_prompt(self, key: str, prompt_text: str, is_secret: bool = False) -> str:
        existing = self.get(key)
        if existing:
            value = click.prompt(prompt_text, default=existing, hide_input=is_secret)
        else:
            value = click.prompt(prompt_text, hide_input=is_secret)
        self.set(key, value)
        return str(value)

    def exists(self) -> bool:
        return self._config_path.exists()
