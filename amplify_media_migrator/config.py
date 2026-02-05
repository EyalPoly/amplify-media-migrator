from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Any


@dataclass
class GoogleDriveConfig:
    folder_id: str = ""
    credentials_path: str = "~/.amplify-media-migrator/google_credentials.json"
    token_path: str = "~/.amplify-media-migrator/google_token.json"


@dataclass
class CognitoConfig:
    user_pool_id: str = ""
    client_id: str = ""
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


class ConfigManager:
    DEFAULT_CONFIG_DIR = Path.home() / ".amplify-media-migrator"
    DEFAULT_CONFIG_FILE = "config.json"

    def __init__(self, config_path: Optional[Path] = None) -> None:
        self._config_path = config_path or (self.DEFAULT_CONFIG_DIR / self.DEFAULT_CONFIG_FILE)
        self._config: Optional[Config] = None

    @property
    def config_path(self) -> Path:
        return self._config_path

    def load(self) -> Config:
        raise NotImplementedError

    def save(self) -> None:
        raise NotImplementedError

    def get(self, key: str, default: Any = None) -> Any:
        raise NotImplementedError

    def set(self, key: str, value: Any) -> None:
        raise NotImplementedError

    def update(self, key: str, value: Any) -> None:
        raise NotImplementedError

    def get_or_prompt(self, key: str, prompt: str, is_secret: bool = False) -> str:
        raise NotImplementedError

    def ensure_config_dir(self) -> None:
        raise NotImplementedError

    def exists(self) -> bool:
        return self._config_path.exists()