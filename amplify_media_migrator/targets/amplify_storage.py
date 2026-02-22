import logging
from pathlib import Path
from typing import Any, NoReturn, Optional

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from ..utils.exceptions import AuthenticationError, UploadError

logger = logging.getLogger(__name__)


class AmplifyStorageClient:
    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        identity_pool_id: str = "",
        user_pool_id: str = "",
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._identity_pool_id = identity_pool_id
        self._user_pool_id = user_pool_id
        self._client: Optional[Any] = None

    def connect(self, id_token: str) -> None:
        login_provider = (
            f"cognito-idp.{self._region}.amazonaws.com/{self._user_pool_id}"
        )
        logins = {login_provider: id_token}

        try:
            identity_client = boto3.client("cognito-identity", region_name=self._region)

            identity_response = identity_client.get_id(
                IdentityPoolId=self._identity_pool_id,
                Logins=logins,
            )
            identity_id = identity_response["IdentityId"]

            credentials_response = identity_client.get_credentials_for_identity(
                IdentityId=identity_id,
                Logins=logins,
            )
            creds = credentials_response["Credentials"]

            self._client = boto3.client(
                "s3",
                region_name=self._region,
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretKey"],
                aws_session_token=creds["SessionToken"],
            )
            logger.info("Connected to S3 via Cognito Identity Pool")

        except ClientError as e:
            raise AuthenticationError(
                f"Failed to obtain S3 credentials via Identity Pool: {e}",
                provider="cognito",
            ) from e

    def _ensure_connected(self) -> Any:
        if self._client is None:
            raise UploadError(
                "Not connected to S3. Call connect() first.",
                bucket=self._bucket,
            )
        return self._client

    @staticmethod
    def _handle_client_error(
        error: ClientError,
        key: Optional[str] = None,
        bucket: Optional[str] = None,
    ) -> NoReturn:
        code = error.response["Error"]["Code"]

        if code in ("AccessDenied", "InvalidAccessKeyId", "ExpiredToken"):
            raise AuthenticationError(
                f"S3 authentication error ({code}): {error}",
                provider="cognito",
            ) from error

        raise UploadError(
            f"S3 error ({code}): {error}",
            bucket=bucket,
            key=key,
        ) from error

    def upload_file(
        self,
        data: bytes,
        key: str,
        content_type: str,
    ) -> str:
        s3 = self._ensure_connected()
        try:
            s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
        except ClientError as e:
            self._handle_client_error(e, key=key, bucket=self._bucket)

        url = self.get_url(key)
        logger.debug("Uploaded %s to %s", key, url)
        return url

    def upload_file_multipart(
        self,
        file_path: Path,
        key: str,
        content_type: str,
        chunk_size_mb: int = 8,
    ) -> str:
        s3 = self._ensure_connected()
        config = TransferConfig(
            multipart_threshold=chunk_size_mb * 1024 * 1024,
            multipart_chunksize=chunk_size_mb * 1024 * 1024,
        )
        try:
            s3.upload_file(
                str(file_path),
                self._bucket,
                key,
                ExtraArgs={"ContentType": content_type},
                Config=config,
            )
        except ClientError as e:
            self._handle_client_error(e, key=key, bucket=self._bucket)

        url = self.get_url(key)
        logger.debug("Multipart uploaded %s to %s", key, url)
        return url

    def file_exists(self, key: str) -> bool:
        s3 = self._ensure_connected()
        try:
            s3.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            self._handle_client_error(e, key=key, bucket=self._bucket)

    def get_url(self, key: str) -> str:
        return f"https://{self._bucket}.s3.{self._region}.amazonaws.com/{key}"

    def delete_file(self, key: str) -> None:
        s3 = self._ensure_connected()
        try:
            s3.delete_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            self._handle_client_error(e, key=key, bucket=self._bucket)
        logger.debug("Deleted %s from %s", key, self._bucket)
