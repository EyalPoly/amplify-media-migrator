"""Amplify Media Migrator - Migrate media files from Google Drive to AWS Amplify Storage."""

import importlib.metadata

try:
    __version__ = importlib.metadata.version("amplify-media-migrator")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

from .config import ConfigManager
from .migration.engine import MigrationEngine
from .migration.progress import ProgressTracker, FileStatus
from .migration.mapper import FilenameMapper, ParsedFilename
from .auth import AuthenticationProvider, CognitoAuthProvider, GoogleDriveAuthProvider
from .sources.google_drive import GoogleDriveClient
from .targets.amplify_storage import AmplifyStorageClient
from .targets.graphql_client import GraphQLClient

__all__ = [
    "ConfigManager",
    "MigrationEngine",
    "ProgressTracker",
    "FileStatus",
    "FilenameMapper",
    "ParsedFilename",
    "AuthenticationProvider",
    "CognitoAuthProvider",
    "GoogleDriveAuthProvider",
    "GoogleDriveClient",
    "AmplifyStorageClient",
    "GraphQLClient",
]
