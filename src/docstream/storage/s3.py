"""S3-compatible object storage (AWS S3, MinIO, or any S3 API).

This is what makes the services stateless. With local disk, the gateway and the
extraction worker must share a filesystem — trivial in one process, awkward in
Docker, and genuinely hard in Kubernetes where pods land on different nodes and
the default ReadWriteOnce volume can't be shared. Object storage removes the
constraint: any pod can resolve any URI from anywhere.

URIs are ``s3://<bucket>/<key>``, so they're self-describing and travel on the
events exactly like the local ``file://`` URIs did.

boto3 is synchronous, so every call is dispatched to a worker thread. That keeps
the async contract without pulling in aioboto3, whose botocore pinning tends to
fight other dependencies.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class S3Storage:
    def __init__(
        self,
        bucket: str,
        *,
        endpoint_url: str | None = None,
        access_key: str = "",
        secret_key: str = "",
        region: str = "us-east-1",
    ) -> None:
        import boto3

        self.bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or None,  # None => real AWS
            aws_access_key_id=access_key or None,
            aws_secret_access_key=secret_key or None,
            region_name=region,
        )

    # --- async interface ---------------------------------------------------- #
    async def save(self, document_id: str, filename: str, data: bytes) -> str:
        return await asyncio.to_thread(self.save_sync, document_id, filename, data)

    async def read(self, uri: str) -> bytes:
        return await asyncio.to_thread(self.read_sync, uri)

    # --- sync implementation ------------------------------------------------ #
    def save_sync(self, document_id: str, filename: str, data: bytes) -> str:
        key = self._key_for(document_id, filename)
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"

    def read_sync(self, uri: str) -> bytes:
        bucket, key = self.parse_uri(uri)
        response = self._client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def ensure_bucket(self) -> None:
        """Create the bucket if it doesn't exist. Idempotent; safe on startup."""
        from botocore.exceptions import ClientError

        try:
            self._client.head_bucket(Bucket=self.bucket)
            return
        except ClientError:
            pass
        try:
            self._client.create_bucket(Bucket=self.bucket)
            logger.info("created bucket %s", self.bucket)
        except ClientError as exc:  # already created by another replica
            if exc.response.get("Error", {}).get("Code") not in {
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            }:
                raise

    # --- helpers ------------------------------------------------------------ #
    @staticmethod
    def _key_for(document_id: str, filename: str) -> str:
        safe_name = Path(filename).name or "document.bin"
        return f"{document_id}/{safe_name}"

    @staticmethod
    def parse_uri(uri: str) -> tuple[str, str]:
        """Split ``s3://bucket/key/parts`` into ``(bucket, key)``."""
        parsed = urlparse(uri)
        if parsed.scheme != "s3":
            raise ValueError(f"not an s3:// URI: {uri!r}")
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"malformed s3 URI: {uri!r}")
        return bucket, key
