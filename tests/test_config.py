import json
from pathlib import Path
from unittest.mock import patch

import pytest

from amplify_media_migrator.config import (
    AmplifyConfig,
    AWSConfig,
    CognitoConfig,
    Config,
    ConfigManager,
    ConfigurationError,
    GoogleDriveConfig,
    MigrationConfig,
    config_from_dict,
    config_to_dict,
    validate_config,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def sample_config_dict():
    return {
        "google_drive": {
            "folder_id": "1ABC_test_folder",
            "credentials_path": "/tmp/test_creds.json",
            "token_path": "/tmp/test_token.json",
        },
        "aws": {
            "region": "eu-west-1",
            "cognito": {
                "user_pool_id": "eu-west-1_TestPool",
                "client_id": "test_client_id",
                "identity_pool_id": "eu-west-1:test-identity-pool-id",
                "username": "test@example.com",
            },
            "amplify": {
                "api_endpoint": "https://test.appsync-api.eu-west-1.amazonaws.com/graphql",
                "storage_bucket": "test-media-bucket",
            },
        },
        "migration": {
            "max_workers": 5,
            "retry_attempts": 2,
            "retry_delay_seconds": 10,
            "chunk_size_mb": 16,
            "default_media_public": True,
            "adaptive_concurrency": True,
            "min_workers": 4,
            "initial_workers": None,
            "max_inflight_buffer_mb": 512,
            "window_seconds": 10.0,
        },
        "prefix_disambiguation": {
            "enabled": False,
            "discriminator_field": "",
            "prefixes": {},
        },
    }


@pytest.fixture
def config_file(tmp_path, sample_config_dict):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(sample_config_dict))
    return path


@pytest.fixture
def manager(config_file):
    return ConfigManager(config_path=config_file)


class TestConfigDataClasses:
    def test_google_drive_defaults(self):
        gd = GoogleDriveConfig()
        assert gd.folder_id == ""
        assert (
            gd.credentials_path == "~/.amplify-media-migrator/google_credentials.json"
        )
        assert gd.token_path == "~/.amplify-media-migrator/google_token.json"

    def test_cognito_defaults(self):
        cognito = CognitoConfig()
        assert cognito.user_pool_id == ""
        assert cognito.client_id == ""
        assert cognito.username == ""

    def test_aws_defaults(self):
        aws = AWSConfig()
        assert aws.region == "us-east-1"
        assert isinstance(aws.cognito, CognitoConfig)
        assert isinstance(aws.amplify, AmplifyConfig)

    def test_migration_defaults(self):
        migration = MigrationConfig()
        assert migration.max_workers == 50
        assert migration.retry_attempts == 3
        assert migration.retry_delay_seconds == 5
        assert migration.chunk_size_mb == 8
        assert migration.default_media_public is False

    def test_full_config_composition(self):
        config = Config(
            google_drive=GoogleDriveConfig(folder_id="abc"),
            aws=AWSConfig(
                region="us-west-2",
                cognito=CognitoConfig(user_pool_id="pool_1"),
                amplify=AmplifyConfig(storage_bucket="my-bucket"),
            ),
            migration=MigrationConfig(max_workers=20),
        )
        assert config.google_drive.folder_id == "abc"
        assert config.aws.region == "us-west-2"
        assert config.aws.cognito.user_pool_id == "pool_1"
        assert config.aws.amplify.storage_bucket == "my-bucket"
        assert config.migration.max_workers == 20


class TestConfigSerialization:
    def test_config_to_dict_structure(self):
        config = Config(
            google_drive=GoogleDriveConfig(folder_id="folder_123"),
            aws=AWSConfig(region="ap-southeast-1"),
            migration=MigrationConfig(max_workers=15),
        )
        result = config_to_dict(config)
        assert result["google_drive"]["folder_id"] == "folder_123"
        assert result["aws"]["region"] == "ap-southeast-1"
        assert result["migration"]["max_workers"] == 15
        assert "cognito" in result["aws"]
        assert "amplify" in result["aws"]

    def test_config_from_dict_creates_correct_objects(self, sample_config_dict):
        config = config_from_dict(sample_config_dict)
        assert isinstance(config, Config)
        assert isinstance(config.google_drive, GoogleDriveConfig)
        assert isinstance(config.aws, AWSConfig)
        assert isinstance(config.aws.cognito, CognitoConfig)
        assert isinstance(config.aws.amplify, AmplifyConfig)
        assert isinstance(config.migration, MigrationConfig)
        assert config.google_drive.folder_id == "1ABC_test_folder"
        assert config.aws.cognito.username == "test@example.com"
        assert config.migration.chunk_size_mb == 16

    def test_round_trip_serialization(self, sample_config_dict):
        config = config_from_dict(sample_config_dict)
        result = config_to_dict(config)
        assert result == sample_config_dict

    def test_partial_dict_uses_defaults(self):
        partial = {"aws": {"region": "us-west-2"}}
        config = config_from_dict(partial)
        assert config.aws.region == "us-west-2"
        assert config.google_drive.folder_id == ""
        assert (
            config.google_drive.credentials_path
            == "~/.amplify-media-migrator/google_credentials.json"
        )
        assert config.migration.max_workers == 50
        assert config.aws.cognito.user_pool_id == ""


class TestConfigValidation:
    def test_valid_config_passes(self):
        config = Config()
        validate_config(config)

    def test_max_workers_zero_fails(self):
        config = Config(migration=MigrationConfig(max_workers=0))
        with pytest.raises(ConfigurationError):
            validate_config(config)

    def test_max_workers_negative_fails(self):
        config = Config(migration=MigrationConfig(max_workers=-1))
        with pytest.raises(ConfigurationError):
            validate_config(config)

    def test_retry_attempts_negative_fails(self):
        config = Config(migration=MigrationConfig(retry_attempts=-1))
        with pytest.raises(ConfigurationError):
            validate_config(config)

    def test_retry_delay_negative_fails(self):
        config = Config(migration=MigrationConfig(retry_delay_seconds=-1))
        with pytest.raises(ConfigurationError):
            validate_config(config)

    def test_chunk_size_zero_fails(self):
        config = Config(migration=MigrationConfig(chunk_size_mb=0))
        with pytest.raises(ConfigurationError):
            validate_config(config)

    def test_chunk_size_negative_fails(self):
        config = Config(migration=MigrationConfig(chunk_size_mb=-5))
        with pytest.raises(ConfigurationError):
            validate_config(config)

    def test_error_message_is_descriptive(self):
        config = Config(migration=MigrationConfig(max_workers=-1))
        with pytest.raises(ConfigurationError, match="max_workers"):
            validate_config(config)


class TestConfigManagerFileOps:
    def test_ensure_config_dir_creates_directory(self, tmp_path):
        nested = tmp_path / "a" / "b" / "config.json"
        mgr = ConfigManager(config_path=nested)
        mgr.ensure_config_dir()
        assert nested.parent.exists()

    def test_exists_returns_false_when_missing(self, tmp_path):
        mgr = ConfigManager(config_path=tmp_path / "nonexistent.json")
        assert mgr.exists() is False

    def test_exists_returns_true_when_present(self, config_file):
        mgr = ConfigManager(config_path=config_file)
        assert mgr.exists() is True

    def test_save_creates_file_with_valid_json(self, tmp_path):
        path = tmp_path / "output.json"
        mgr = ConfigManager(config_path=path)
        _ = mgr.config
        mgr.save()
        assert path.exists()
        data = json.loads(path.read_text())
        assert "google_drive" in data
        assert "aws" in data
        assert "migration" in data

    def test_save_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "nested" / "deep" / "config.json"
        mgr = ConfigManager(config_path=path)
        _ = mgr.config
        mgr.save()
        assert path.exists()

    def test_load_reads_config_correctly(self, manager, sample_config_dict):
        config = manager.load()
        assert (
            config.google_drive.folder_id
            == sample_config_dict["google_drive"]["folder_id"]
        )
        assert config.aws.region == sample_config_dict["aws"]["region"]
        assert (
            config.migration.max_workers
            == sample_config_dict["migration"]["max_workers"]
        )

    def test_load_raises_on_missing_file(self, tmp_path):
        mgr = ConfigManager(config_path=tmp_path / "missing.json")
        with pytest.raises(ConfigurationError):
            mgr.load()

    def test_load_raises_on_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{not valid json!!!")
        mgr = ConfigManager(config_path=path)
        with pytest.raises(ConfigurationError):
            mgr.load()

    def test_save_and_load_round_trip(self, tmp_path):
        path = tmp_path / "roundtrip.json"
        mgr1 = ConfigManager(config_path=path)
        _ = mgr1.config
        mgr1.config.google_drive.folder_id = "roundtrip_folder"
        mgr1.config.aws.region = "ap-northeast-1"
        mgr1.config.migration.max_workers = 42
        mgr1.save()

        mgr2 = ConfigManager(config_path=path)
        config = mgr2.load()
        assert config.google_drive.folder_id == "roundtrip_folder"
        assert config.aws.region == "ap-northeast-1"
        assert config.migration.max_workers == 42


class TestConfigManagerDotNotation:
    def test_get_top_level_key(self, manager):
        manager.load()
        result = manager.get("aws")
        assert isinstance(result, AWSConfig)

    def test_get_nested_key(self, manager, sample_config_dict):
        manager.load()
        assert (
            manager.get("aws.cognito.user_pool_id")
            == sample_config_dict["aws"]["cognito"]["user_pool_id"]
        )
        assert manager.get("aws.region") == sample_config_dict["aws"]["region"]

    def test_get_missing_key_returns_default(self, manager):
        manager.load()
        assert manager.get("nonexistent", "fallback") == "fallback"
        assert manager.get("aws.nonexistent", None) is None

    def test_set_updates_value(self, manager):
        manager.load()
        manager.set("aws.region", "sa-east-1")
        assert manager.get("aws.region") == "sa-east-1"

    def test_set_nested_value(self, manager):
        manager.load()
        manager.set("aws.cognito.username", "new_user@test.com")
        assert manager.get("aws.cognito.username") == "new_user@test.com"

    def test_update_is_alias_for_set(self, manager):
        manager.load()
        manager.update("migration.max_workers", 99)
        assert manager.get("migration.max_workers") == 99


class TestConfigEnvOverrides:
    def test_aws_region_override(self, manager, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-west-2")
        config = manager.load()
        assert config.aws.region == "us-west-2"

    def test_amplify_api_endpoint_override(self, manager, monkeypatch):
        monkeypatch.setenv(
            "AMPLIFY_API_ENDPOINT",
            "https://override.appsync-api.us-east-1.amazonaws.com/graphql",
        )
        config = manager.load()
        assert (
            config.aws.amplify.api_endpoint
            == "https://override.appsync-api.us-east-1.amazonaws.com/graphql"
        )

    def test_google_credentials_path_override(self, manager, monkeypatch):
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/custom/path/creds.json")
        config = manager.load()
        assert config.google_drive.credentials_path == "/custom/path/creds.json"

    def test_multiple_overrides_applied(self, manager, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-central-1")
        monkeypatch.setenv("AMPLIFY_API_ENDPOINT", "https://multi.test.com/graphql")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/multi/creds.json")
        config = manager.load()
        assert config.aws.region == "eu-central-1"
        assert config.aws.amplify.api_endpoint == "https://multi.test.com/graphql"
        assert config.google_drive.credentials_path == "/multi/creds.json"


class TestConfigManagerPrompts:
    def test_prompts_with_existing_value_as_default(self, manager):
        manager.load()
        existing = manager.get("aws.region")
        with patch(
            "amplify_media_migrator.config.click.prompt", return_value=existing
        ) as mock_prompt:
            result = manager.get_or_prompt("aws.region", "Enter region")
            mock_prompt.assert_called_once_with(
                "Enter region", default=existing, hide_input=False
            )
            assert result == existing

    def test_prompts_when_value_is_empty(self, tmp_path):
        path = tmp_path / "empty_config.json"
        path.write_text(
            json.dumps(
                {
                    "google_drive": {
                        "folder_id": "",
                        "credentials_path": "",
                        "token_path": "",
                    },
                    "aws": {
                        "region": "",
                        "cognito": {
                            "user_pool_id": "",
                            "client_id": "",
                            "username": "",
                        },
                        "amplify": {"api_endpoint": "", "storage_bucket": ""},
                    },
                    "migration": {
                        "max_workers": 10,
                        "retry_attempts": 3,
                        "retry_delay_seconds": 5,
                        "chunk_size_mb": 8,
                        "default_media_public": False,
                    },
                }
            )
        )
        mgr = ConfigManager(config_path=path)
        mgr.load()
        with patch(
            "amplify_media_migrator.config.click.prompt",
            return_value="prompted_value",
        ) as mock_prompt:
            result = mgr.get_or_prompt("aws.region", "Enter AWS region")
            mock_prompt.assert_called_once()
            assert result == "prompted_value"

    def test_secret_flag_passed_to_prompt(self, tmp_path):
        path = tmp_path / "secret_config.json"
        path.write_text(
            json.dumps(
                {
                    "google_drive": {
                        "folder_id": "",
                        "credentials_path": "",
                        "token_path": "",
                    },
                    "aws": {
                        "region": "",
                        "cognito": {
                            "user_pool_id": "",
                            "client_id": "",
                            "username": "",
                        },
                        "amplify": {"api_endpoint": "", "storage_bucket": ""},
                    },
                    "migration": {
                        "max_workers": 10,
                        "retry_attempts": 3,
                        "retry_delay_seconds": 5,
                        "chunk_size_mb": 8,
                        "default_media_public": False,
                    },
                }
            )
        )
        mgr = ConfigManager(config_path=path)
        mgr.load()
        with patch(
            "amplify_media_migrator.config.click.prompt",
            return_value="secret_val",
        ) as mock_prompt:
            mgr.get_or_prompt(
                "aws.cognito.client_id", "Enter client ID", is_secret=True
            )
            mock_prompt.assert_called_once_with("Enter client ID", hide_input=True)


class TestConfigManagerEdgeCases:
    def test_save_with_no_config_creates_default(self, tmp_path):
        path = tmp_path / "new_config.json"
        mgr = ConfigManager(config_path=path)
        mgr.save()
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["migration"]["max_workers"] == 50

    def test_get_without_loaded_config_falls_back(self, tmp_path):
        path = tmp_path / "missing.json"
        mgr = ConfigManager(config_path=path)
        result = mgr.get("migration.max_workers")
        assert result == 50

    def test_set_without_loaded_config_falls_back(self, tmp_path):
        path = tmp_path / "missing.json"
        mgr = ConfigManager(config_path=path)
        mgr.set("migration.max_workers", 42)
        assert mgr.get("migration.max_workers") == 42

    def test_set_invalid_segment_raises(self, manager):
        manager.load()
        with pytest.raises(ConfigurationError, match="unknown segment"):
            manager.set("aws.nonexistent.value", "x")

    def test_set_invalid_final_segment_raises(self, manager):
        manager.load()
        with pytest.raises(ConfigurationError, match="unknown segment"):
            manager.set("aws.nonexistent_field", "x")


class TestConfigManagerProperties:
    def test_config_path_returns_path(self, config_file):
        mgr = ConfigManager(config_path=config_file)
        assert mgr.config_path == config_file

    def test_default_config_path(self):
        mgr = ConfigManager()
        expected = Path.home() / ".amplify-media-migrator" / "config.json"
        assert mgr.config_path == expected

    def test_config_property_lazy_loads_default(self):
        mgr = ConfigManager(config_path=Path("/tmp/nonexistent_for_test.json"))
        config = mgr.config
        assert isinstance(config, Config)
        assert config.migration.max_workers == 50


def test_prefix_disambiguation_round_trip():
    from amplify_media_migrator.config import config_from_dict, config_to_dict

    data = {
        "prefix_disambiguation": {
            "enabled": True,
            "discriminator_field": "countryId",
            "prefixes": {"": "c-med", "E": "c-red", "S": "*"},
        }
    }
    cfg = config_from_dict(data)
    assert cfg.prefix_disambiguation.enabled is True
    assert cfg.prefix_disambiguation.discriminator_field == "countryId"
    assert cfg.prefix_disambiguation.prefixes["S"] == "*"
    assert config_to_dict(cfg)["prefix_disambiguation"]["prefixes"][""] == "c-med"


def test_prefix_disambiguation_defaults_disabled():
    from amplify_media_migrator.config import config_from_dict

    cfg = config_from_dict({})
    assert cfg.prefix_disambiguation.enabled is False
    assert cfg.prefix_disambiguation.prefixes == {}


def test_prefix_disambiguation_validation_requires_fields():
    from amplify_media_migrator.config import (
        Config,
        PrefixDisambiguationConfig,
        ConfigurationError,
    )

    cfg = Config()
    cfg.prefix_disambiguation = PrefixDisambiguationConfig(
        enabled=True, discriminator_field="", prefixes={}
    )
    with pytest.raises(ConfigurationError):
        cfg.validate()


def test_prefix_disambiguation_rejects_multiple_catch_all():
    from amplify_media_migrator.config import (
        Config,
        PrefixDisambiguationConfig,
        ConfigurationError,
    )

    cfg = Config()
    cfg.prefix_disambiguation = PrefixDisambiguationConfig(
        enabled=True, discriminator_field="countryId", prefixes={"E": "*", "S": "*"}
    )
    with pytest.raises(ConfigurationError):
        cfg.validate()


class TestAdaptiveConcurrencyConfig:
    def test_defaults(self) -> None:
        m = MigrationConfig()
        assert m.adaptive_concurrency is True
        assert m.min_workers == 4
        assert m.initial_workers is None
        assert m.max_inflight_buffer_mb == 512
        assert m.window_seconds == 10.0

    def test_from_dict_reads_adaptive_fields(self) -> None:
        cfg = config_from_dict(
            {
                "migration": {
                    "adaptive_concurrency": False,
                    "min_workers": 2,
                    "initial_workers": 8,
                    "max_inflight_buffer_mb": 256,
                    "window_seconds": 5.0,
                }
            }
        )
        assert cfg.migration.adaptive_concurrency is False
        assert cfg.migration.min_workers == 2
        assert cfg.migration.initial_workers == 8
        assert cfg.migration.max_inflight_buffer_mb == 256
        assert cfg.migration.window_seconds == 5.0

    def test_validate_rejects_min_workers_below_one(self) -> None:
        cfg = Config()
        cfg.migration.min_workers = 0
        with pytest.raises(Exception):
            cfg.validate()

    def test_validate_rejects_min_above_max(self) -> None:
        cfg = Config()
        cfg.migration.max_workers = 4
        cfg.migration.min_workers = 8
        with pytest.raises(Exception):
            cfg.validate()

    def test_legacy_concurrency_key_maps_to_max_workers(self) -> None:
        cfg = config_from_dict({"migration": {"concurrency": 32}})
        assert cfg.migration.max_workers == 32

    def test_max_workers_key_wins_over_legacy_concurrency(self) -> None:
        cfg = config_from_dict({"migration": {"max_workers": 16, "concurrency": 99}})
        assert cfg.migration.max_workers == 16
