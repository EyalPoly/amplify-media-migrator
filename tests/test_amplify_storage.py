from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from amplify_media_migrator.targets.amplify_storage import AmplifyStorageClient
from amplify_media_migrator.utils.exceptions import (
    AuthenticationError,
    UploadError,
)

BUCKET = "test-media-bucket"
REGION = "us-east-1"
IDENTITY_POOL_ID = "us-east-1:test-identity-pool-id"
USER_POOL_ID = "us-east-1_TestPool"
ID_TOKEN = "test-id-token-abc123"


@pytest.fixture
def client() -> AmplifyStorageClient:
    return AmplifyStorageClient(
        bucket=BUCKET,
        region=REGION,
        identity_pool_id=IDENTITY_POOL_ID,
        user_pool_id=USER_POOL_ID,
    )


@pytest.fixture
def mock_s3() -> MagicMock:
    return MagicMock()


@pytest.fixture
def connected_client(
    client: AmplifyStorageClient, mock_s3: MagicMock
) -> AmplifyStorageClient:
    client._client = mock_s3
    return client


def _make_client_error(
    code: str, message: str = "error", operation: str = "PutObject"
) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        operation,
    )


class TestInit:
    def test_stores_bucket_and_region(self) -> None:
        c = AmplifyStorageClient(bucket="my-bucket", region="eu-west-1")
        assert c._bucket == "my-bucket"
        assert c._region == "eu-west-1"

    def test_default_region(self) -> None:
        c = AmplifyStorageClient(bucket="my-bucket")
        assert c._region == "us-east-1"

    def test_not_connected_initially(self, client: AmplifyStorageClient) -> None:
        assert client._client is None

    def test_stores_pool_ids(self, client: AmplifyStorageClient) -> None:
        assert client._identity_pool_id == IDENTITY_POOL_ID
        assert client._user_pool_id == USER_POOL_ID


class TestConnect:
    @patch("amplify_media_migrator.targets.amplify_storage.boto3")
    def test_exchanges_token_for_credentials(
        self, mock_boto3: MagicMock, client: AmplifyStorageClient
    ) -> None:
        mock_identity_client = MagicMock()
        mock_s3_client = MagicMock()

        mock_identity_client.get_id.return_value = {
            "IdentityId": "us-east-1:identity-id-123"
        }
        mock_identity_client.get_credentials_for_identity.return_value = {
            "Credentials": {
                "AccessKeyId": "AKID",
                "SecretKey": "SECRET",
                "SessionToken": "TOKEN",
            }
        }

        def client_factory(service: str, **kwargs: object) -> MagicMock:
            if service == "cognito-identity":
                return mock_identity_client
            if service == "s3":
                return mock_s3_client
            raise ValueError(f"Unexpected service: {service}")

        mock_boto3.client.side_effect = client_factory

        client.connect(ID_TOKEN)

        expected_logins = {
            f"cognito-idp.{REGION}.amazonaws.com/{USER_POOL_ID}": ID_TOKEN
        }
        mock_identity_client.get_id.assert_called_once_with(
            IdentityPoolId=IDENTITY_POOL_ID,
            Logins=expected_logins,
        )
        mock_identity_client.get_credentials_for_identity.assert_called_once_with(
            IdentityId="us-east-1:identity-id-123",
            Logins=expected_logins,
        )
        mock_boto3.client.assert_any_call(
            "s3",
            region_name=REGION,
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
            aws_session_token="TOKEN",
        )
        assert client._client is mock_s3_client

    @patch("amplify_media_migrator.targets.amplify_storage.boto3")
    def test_client_error_raises_auth_error(
        self, mock_boto3: MagicMock, client: AmplifyStorageClient
    ) -> None:
        mock_identity_client = MagicMock()
        mock_identity_client.get_id.side_effect = _make_client_error(
            "NotAuthorizedException", "Invalid token"
        )
        mock_boto3.client.return_value = mock_identity_client

        with pytest.raises(AuthenticationError, match="Identity Pool"):
            client.connect(ID_TOKEN)


class TestEnsureConnected:
    def test_raises_when_not_connected(self, client: AmplifyStorageClient) -> None:
        with pytest.raises(UploadError, match="Not connected"):
            client._ensure_connected()

    def test_returns_client_when_connected(
        self, connected_client: AmplifyStorageClient, mock_s3: MagicMock
    ) -> None:
        assert connected_client._ensure_connected() is mock_s3


class TestHandleClientError:
    def test_access_denied_raises_auth_error(self) -> None:
        with pytest.raises(AuthenticationError, match="AccessDenied"):
            AmplifyStorageClient._handle_client_error(
                _make_client_error("AccessDenied")
            )

    def test_expired_token_raises_auth_error(self) -> None:
        with pytest.raises(AuthenticationError, match="ExpiredToken"):
            AmplifyStorageClient._handle_client_error(
                _make_client_error("ExpiredToken")
            )

    def test_invalid_key_raises_auth_error(self) -> None:
        with pytest.raises(AuthenticationError, match="InvalidAccessKeyId"):
            AmplifyStorageClient._handle_client_error(
                _make_client_error("InvalidAccessKeyId")
            )

    def test_auth_error_has_provider(self) -> None:
        with pytest.raises(AuthenticationError) as exc_info:
            AmplifyStorageClient._handle_client_error(
                _make_client_error("AccessDenied")
            )
        assert exc_info.value.provider == "cognito"

    def test_other_error_raises_upload_error(self) -> None:
        with pytest.raises(UploadError, match="NoSuchBucket"):
            AmplifyStorageClient._handle_client_error(
                _make_client_error("NoSuchBucket"),
                key="test.jpg",
                bucket="bad-bucket",
            )

    def test_upload_error_preserves_key_and_bucket(self) -> None:
        with pytest.raises(UploadError) as exc_info:
            AmplifyStorageClient._handle_client_error(
                _make_client_error("InternalError"),
                key="media/obs-1/photo.jpg",
                bucket="my-bucket",
            )
        assert exc_info.value.key == "media/obs-1/photo.jpg"
        assert exc_info.value.bucket == "my-bucket"


class TestUploadFile:
    def test_uploads_data(
        self, connected_client: AmplifyStorageClient, mock_s3: MagicMock
    ) -> None:
        url = connected_client.upload_file(
            data=b"photo bytes",
            key="media/obs-1/photo.jpg",
            content_type="image/jpeg",
        )

        mock_s3.put_object.assert_called_once_with(
            Bucket=BUCKET,
            Key="media/obs-1/photo.jpg",
            Body=b"photo bytes",
            ContentType="image/jpeg",
        )
        assert (
            url == f"https://{BUCKET}.s3.{REGION}.amazonaws.com/media/obs-1/photo.jpg"
        )

    def test_client_error_raises_upload_error(
        self, connected_client: AmplifyStorageClient, mock_s3: MagicMock
    ) -> None:
        mock_s3.put_object.side_effect = _make_client_error("InternalError")

        with pytest.raises(UploadError):
            connected_client.upload_file(
                data=b"data", key="test.jpg", content_type="image/jpeg"
            )

    def test_not_connected_raises(self, client: AmplifyStorageClient) -> None:
        with pytest.raises(UploadError, match="Not connected"):
            client.upload_file(data=b"data", key="test.jpg", content_type="image/jpeg")


class TestUploadFileMultipart:
    @patch("amplify_media_migrator.targets.amplify_storage.TransferConfig")
    def test_uploads_with_config(
        self,
        mock_transfer_config: MagicMock,
        connected_client: AmplifyStorageClient,
        mock_s3: MagicMock,
        tmp_path: Path,
    ) -> None:
        file_path = tmp_path / "video.mp4"
        file_path.write_bytes(b"video data")
        mock_config = MagicMock()
        mock_transfer_config.return_value = mock_config

        url = connected_client.upload_file_multipart(
            file_path=file_path,
            key="media/obs-1/video.mp4",
            content_type="video/mp4",
            chunk_size_mb=16,
        )

        mock_transfer_config.assert_called_once_with(
            multipart_threshold=16 * 1024 * 1024,
            multipart_chunksize=16 * 1024 * 1024,
        )
        mock_s3.upload_file.assert_called_once_with(
            str(file_path),
            BUCKET,
            "media/obs-1/video.mp4",
            ExtraArgs={"ContentType": "video/mp4"},
            Config=mock_config,
        )
        assert (
            url == f"https://{BUCKET}.s3.{REGION}.amazonaws.com/media/obs-1/video.mp4"
        )

    def test_client_error_raises_upload_error(
        self,
        connected_client: AmplifyStorageClient,
        mock_s3: MagicMock,
        tmp_path: Path,
    ) -> None:
        file_path = tmp_path / "video.mp4"
        file_path.write_bytes(b"data")
        mock_s3.upload_file.side_effect = _make_client_error("InternalError")

        with pytest.raises(UploadError):
            connected_client.upload_file_multipart(
                file_path=file_path,
                key="video.mp4",
                content_type="video/mp4",
            )


class TestFileExists:
    def test_returns_true_when_exists(
        self, connected_client: AmplifyStorageClient, mock_s3: MagicMock
    ) -> None:
        mock_s3.head_object.return_value = {"ContentLength": 1024}

        assert connected_client.file_exists("media/obs-1/photo.jpg") is True
        mock_s3.head_object.assert_called_once_with(
            Bucket=BUCKET, Key="media/obs-1/photo.jpg"
        )

    def test_returns_false_when_not_found(
        self, connected_client: AmplifyStorageClient, mock_s3: MagicMock
    ) -> None:
        mock_s3.head_object.side_effect = _make_client_error("404", "Not Found")

        assert connected_client.file_exists("nonexistent.jpg") is False

    def test_other_error_raises_upload_error(
        self, connected_client: AmplifyStorageClient, mock_s3: MagicMock
    ) -> None:
        mock_s3.head_object.side_effect = _make_client_error("AccessDenied")

        with pytest.raises(AuthenticationError):
            connected_client.file_exists("protected.jpg")

    def test_not_connected_raises(self, client: AmplifyStorageClient) -> None:
        with pytest.raises(UploadError, match="Not connected"):
            client.file_exists("test.jpg")


class TestGetUrl:
    def test_generates_correct_url(
        self, connected_client: AmplifyStorageClient
    ) -> None:
        url = connected_client.get_url("media/obs-123/photo.jpg")
        assert (
            url == f"https://{BUCKET}.s3.{REGION}.amazonaws.com/media/obs-123/photo.jpg"
        )

    def test_different_region(self) -> None:
        c = AmplifyStorageClient(bucket="eu-bucket", region="eu-west-1")
        url = c.get_url("media/obs-1/photo.jpg")
        assert (
            url == "https://eu-bucket.s3.eu-west-1.amazonaws.com/media/obs-1/photo.jpg"
        )


class TestDeleteFile:
    def test_deletes_object(
        self, connected_client: AmplifyStorageClient, mock_s3: MagicMock
    ) -> None:
        connected_client.delete_file("media/obs-1/photo.jpg")

        mock_s3.delete_object.assert_called_once_with(
            Bucket=BUCKET, Key="media/obs-1/photo.jpg"
        )

    def test_client_error_raises(
        self, connected_client: AmplifyStorageClient, mock_s3: MagicMock
    ) -> None:
        mock_s3.delete_object.side_effect = _make_client_error("InternalError")

        with pytest.raises(UploadError):
            connected_client.delete_file("test.jpg")

    def test_not_connected_raises(self, client: AmplifyStorageClient) -> None:
        with pytest.raises(UploadError, match="Not connected"):
            client.delete_file("test.jpg")
