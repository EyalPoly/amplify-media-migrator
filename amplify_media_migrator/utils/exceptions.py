"""Custom exception classes for the amplify-media-migrator."""

from typing import Any, List, Optional


class MigratorError(Exception):
    """Base exception class for all migrator errors."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class ConfigurationError(MigratorError):
    """Raised when configuration is invalid or missing."""

    def __init__(self, message: str, config_key: Optional[str] = None) -> None:
        self.config_key = config_key
        super().__init__(message)


class AuthenticationError(MigratorError):
    """Raised when authentication fails (Cognito or Google)."""

    def __init__(self, message: str, provider: Optional[str] = None) -> None:
        self.provider = provider
        super().__init__(message)


class RateLimitError(MigratorError):
    """Raised when API rate limit is exceeded. This error is retryable."""

    def __init__(
        self, message: str, retry_after: Optional[float] = None
    ) -> None:
        self.retry_after = retry_after
        super().__init__(message)

    @property
    def is_retryable(self) -> bool:
        return True


class DownloadError(MigratorError):
    """Raised when a Google Drive download fails."""

    def __init__(
        self,
        message: str,
        file_id: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> None:
        self.file_id = file_id
        self.filename = filename
        super().__init__(message)


class UploadError(MigratorError):
    """Raised when an S3 upload fails."""

    def __init__(
        self,
        message: str,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
    ) -> None:
        self.bucket = bucket
        self.key = key
        super().__init__(message)


class GraphQLError(MigratorError):
    """Raised when a GraphQL API call fails."""

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        errors: Optional[List[Any]] = None,
    ) -> None:
        self.operation = operation
        self.errors: List[Any] = errors or []
        super().__init__(message)


class ObservationNotFoundError(MigratorError):
    """Raised when no observation exists for a given sequentialId."""

    def __init__(self, message: str, sequential_id: Optional[int] = None) -> None:
        self.sequential_id = sequential_id
        super().__init__(message)


class InvalidFilenameError(MigratorError):
    """Raised when a filename doesn't match any valid pattern."""

    def __init__(self, message: str, filename: Optional[str] = None) -> None:
        self.filename = filename
        super().__init__(message)