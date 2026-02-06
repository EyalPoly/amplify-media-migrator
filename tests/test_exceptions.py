import pytest

from amplify_media_migrator.utils.exceptions import (
    MigratorError,
    ConfigurationError,
    AuthenticationError,
    RateLimitError,
    DownloadError,
    UploadError,
    GraphQLError,
    ObservationNotFoundError,
    InvalidFilenameError,
)


class TestMigratorError:
    def test_message_stored(self):
        error = MigratorError("test error")
        assert error.message == "test error"
        assert str(error) == "test error"

    def test_inherits_from_exception(self):
        error = MigratorError("test")
        assert isinstance(error, Exception)


class TestConfigurationError:
    def test_message_and_config_key(self):
        error = ConfigurationError("missing key", config_key="aws.region")
        assert error.message == "missing key"
        assert error.config_key == "aws.region"

    def test_config_key_optional(self):
        error = ConfigurationError("invalid config")
        assert error.config_key is None

    def test_inherits_from_migrator_error(self):
        error = ConfigurationError("test")
        assert isinstance(error, MigratorError)


class TestAuthenticationError:
    def test_message_and_provider(self):
        error = AuthenticationError("token expired", provider="cognito")
        assert error.message == "token expired"
        assert error.provider == "cognito"

    def test_provider_optional(self):
        error = AuthenticationError("auth failed")
        assert error.provider is None

    def test_inherits_from_migrator_error(self):
        error = AuthenticationError("test")
        assert isinstance(error, MigratorError)


class TestRateLimitError:
    def test_message_and_retry_after(self):
        error = RateLimitError("rate limited", retry_after=30.0)
        assert error.message == "rate limited"
        assert error.retry_after == 30.0

    def test_retry_after_optional(self):
        error = RateLimitError("rate limited")
        assert error.retry_after is None

    def test_is_retryable_property(self):
        error = RateLimitError("rate limited")
        assert error.is_retryable is True

    def test_inherits_from_migrator_error(self):
        error = RateLimitError("test")
        assert isinstance(error, MigratorError)


class TestDownloadError:
    def test_message_and_file_info(self):
        error = DownloadError(
            "download failed", file_id="abc123", filename="test.jpg"
        )
        assert error.message == "download failed"
        assert error.file_id == "abc123"
        assert error.filename == "test.jpg"

    def test_file_info_optional(self):
        error = DownloadError("download failed")
        assert error.file_id is None
        assert error.filename is None

    def test_inherits_from_migrator_error(self):
        error = DownloadError("test")
        assert isinstance(error, MigratorError)


class TestUploadError:
    def test_message_and_s3_info(self):
        error = UploadError(
            "upload failed", bucket="my-bucket", key="media/123/test.jpg"
        )
        assert error.message == "upload failed"
        assert error.bucket == "my-bucket"
        assert error.key == "media/123/test.jpg"

    def test_s3_info_optional(self):
        error = UploadError("upload failed")
        assert error.bucket is None
        assert error.key is None

    def test_inherits_from_migrator_error(self):
        error = UploadError("test")
        assert isinstance(error, MigratorError)


class TestGraphQLError:
    def test_message_and_operation(self):
        error = GraphQLError("query failed", operation="createMedia")
        assert error.message == "query failed"
        assert error.operation == "createMedia"

    def test_with_errors_list(self):
        errors_list = [{"message": "error1"}, {"message": "error2"}]
        error = GraphQLError("query failed", errors=errors_list)
        assert error.errors == errors_list

    def test_errors_defaults_to_empty_list(self):
        error = GraphQLError("query failed")
        assert error.errors == []

    def test_operation_optional(self):
        error = GraphQLError("query failed")
        assert error.operation is None

    def test_inherits_from_migrator_error(self):
        error = GraphQLError("test")
        assert isinstance(error, MigratorError)


class TestObservationNotFoundError:
    def test_message_and_sequential_id(self):
        error = ObservationNotFoundError(
            "observation not found", sequential_id=12345
        )
        assert error.message == "observation not found"
        assert error.sequential_id == 12345

    def test_sequential_id_optional(self):
        error = ObservationNotFoundError("not found")
        assert error.sequential_id is None

    def test_inherits_from_migrator_error(self):
        error = ObservationNotFoundError("test")
        assert isinstance(error, MigratorError)


class TestInvalidFilenameError:
    def test_message_and_filename(self):
        error = InvalidFilenameError("invalid pattern", filename="abc.pdf")
        assert error.message == "invalid pattern"
        assert error.filename == "abc.pdf"

    def test_filename_optional(self):
        error = InvalidFilenameError("invalid")
        assert error.filename is None

    def test_inherits_from_migrator_error(self):
        error = InvalidFilenameError("test")
        assert isinstance(error, MigratorError)


class TestExceptionHierarchy:
    def test_all_exceptions_inherit_from_migrator_error(self):
        exceptions = [
            ConfigurationError("test"),
            AuthenticationError("test"),
            RateLimitError("test"),
            DownloadError("test"),
            UploadError("test"),
            GraphQLError("test"),
            ObservationNotFoundError("test"),
            InvalidFilenameError("test"),
        ]
        for exc in exceptions:
            assert isinstance(exc, MigratorError)
            assert isinstance(exc, Exception)

    def test_exceptions_can_be_caught_by_base_class(self):
        with pytest.raises(MigratorError):
            raise ConfigurationError("test")

        with pytest.raises(MigratorError):
            raise RateLimitError("test")

    def test_exceptions_have_message_attribute(self):
        exceptions = [
            MigratorError("msg1"),
            ConfigurationError("msg2"),
            AuthenticationError("msg3"),
            RateLimitError("msg4"),
            DownloadError("msg5"),
            UploadError("msg6"),
            GraphQLError("msg7"),
            ObservationNotFoundError("msg8"),
            InvalidFilenameError("msg9"),
        ]
        for i, exc in enumerate(exceptions, 1):
            assert exc.message == f"msg{i}"