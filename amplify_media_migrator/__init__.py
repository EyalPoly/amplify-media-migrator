"""Amplify Media Migrator - Migrate media files from Google Drive to AWS Amplify Storage."""

__version__ = "0.1.0"

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
