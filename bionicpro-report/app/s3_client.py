"""
S3-compatible MinIO client for Python microservice.

Features:
- Initializes boto3 S3 client configured for MinIO
- Automatic bucket creation (idempotent)
- Functions: upload_fileobj, upload_bytes, exists, download_to_file, generate_presigned_url
- Key structure: /reports/{type}/{year}/{month}/{client_id}/{hash}.csv

Notes:
- Configure via environment variables or pass a dict config
- Uses TransferConfig for multipart uploads for large files
"""

from __future__ import annotations

import os
import io
import json
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig


DEFAULT_MULTIPART_THRESHOLD = 50 * 1024 * 1024  # 50 MB
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


class MinioS3Client:
    """S3 client wrapper for MinIO (S3-compatible).

    Initialization options (via env or explicit config dict):
      - endpoint_url: e.g. http://minio:9000
      - aws_access_key_id
      - aws_secret_access_key
      - region_name (optional)
      - bucket (default bucket name; still created lazily)
      - use_ssl (bool)
      - addressing_style ('path' recommended for MinIO)
      - multipart_threshold
      - multipart_chunksize
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or {}
        # Read from environment if not provided
        endpoint = cfg.get("endpoint_url") or os.getenv("S3_ENDPOINT_URL")
        key = cfg.get("aws_access_key_id") or os.getenv("S3_ACCESS_KEY")
        secret = cfg.get("aws_secret_access_key") or os.getenv("S3_SECRET_KEY")
        region = cfg.get("region_name") or os.getenv("S3_REGION")
        use_ssl = (
            cfg.get("use_ssl")
            if "use_ssl" in cfg
            else (os.getenv("S3_USE_SSL", "false").lower() == "true")
        )
        addressing = cfg.get("addressing_style") or os.getenv(
            "S3_ADDRESSING_STYLE", "path"
        )
        self.default_bucket = cfg.get("bucket") or os.getenv("S3_DEFAULT_BUCKET")

        if not endpoint or not key or not secret:
            raise ValueError(
                "S3 endpoint and credentials must be provided via config or environment variables"
            )

        botocore_config = BotoConfig(
            signature_version="s3v4", s3={"addressing_style": addressing}
        )

        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            region_name=region,
            config=botocore_config,
            use_ssl=use_ssl,
        )

        # Transfer config for multipart
        multipart_threshold = cfg.get(
            "multipart_threshold",
            int(os.getenv("S3_MULTIPART_THRESHOLD", DEFAULT_MULTIPART_THRESHOLD)),
        )
        multipart_chunksize = cfg.get(
            "multipart_chunksize",
            int(os.getenv("S3_MULTIPART_CHUNKSIZE", DEFAULT_CHUNK_SIZE)),
        )

        self.transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold,
            multipart_chunksize=multipart_chunksize,
        )

    # -------- key utilities --------
    @staticmethod
    def _params_to_hash(params: Dict[str, Any]) -> str:
        """Deterministic short hash for a report parameters dict.

        Sort keys, dump compact JSON, sha256, return first 16 chars.
        """
        canonical = json.dumps(
            params, separators=(",", ":"), sort_keys=True, ensure_ascii=False
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return digest[:16]

    @staticmethod
    def build_report_key(
        report_type: str,
        client_id: str,
        params: Dict[str, Any],
        ext: str = "csv",
        when: datetime | None = None,
    ) -> str:
        """Build object key with required structure:

        /reports/{type}/{year}/{month}/{client_id}/{hash}.csv
        """
        if when is None:
            when = datetime.utcnow()
        year = when.year
        month = f"{when.month:02d}"
        h = MinioS3Client._params_to_hash(params)
        # ensure no leading slash
        return f"{report_type}/{year}/{month}/{client_id}/{h}.{ext.lstrip('.') }"

    # -------- bucket management --------
    def ensure_bucket(self, bucket_name: Optional[str] = None) -> None:
        """Create bucket if it doesn't exist. Idempotent: handles race conditions.

        For S3-compatible stores like MinIO, region constraints may be different; use safe create pattern.
        """
        bucket = bucket_name or self.default_bucket
        if not bucket:
            raise ValueError("Bucket name required")

        # Check existence
        try:
            self.s3.head_bucket(Bucket=bucket)
            return
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            # head_bucket may return 404 or 403
            # proceed to create if not found
        try:
            # MinIO ignores location constraint in many setups; safe to call without CreateBucketConfiguration
            self.s3.create_bucket(Bucket=bucket)
        except ClientError as e:
            err_code = e.response.get("Error", {}).get("Code")
            # If bucket already exists and is owned by us, fine; otherwise re-raise
            if err_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                return
            raise

    # -------- object operations --------
    def exists(self, key: str, bucket: Optional[str] = None) -> bool:
        bucket = bucket or self.default_bucket
        if not bucket:
            raise ValueError("Bucket name required")
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            status = int(
                e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            )
            if status == 404:
                return False
            # For permission denied (403) or other errors, re-raise
            raise

    def upload_bytes(
        self,
        key: str,
        data: bytes,
        bucket: Optional[str] = None,
        content_type: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Upload bytes payload. Uses put_object for small objects and multipart through upload_fileobj for larger ones."""
        bucket = bucket or self.default_bucket
        if not bucket:
            raise ValueError("Bucket name required")
        self.ensure_bucket(bucket)
        extra_args = extra_args or {}
        if content_type:
            extra_args["ContentType"] = content_type
        # For simplicity, use put_object for bytes
        self.s3.put_object(Bucket=bucket, Key=key, Body=data, **extra_args)

    def upload_fileobj(
        self,
        key: str,
        fileobj: io.BufferedReader,
        bucket: Optional[str] = None,
        content_type: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Upload a file-like object (streaming) using upload_fileobj to support multipart."""
        bucket = bucket or self.default_bucket
        if not bucket:
            raise ValueError("Bucket name required")
        self.ensure_bucket(bucket)
        extra_args = extra_args or {}
        if content_type:
            extra_args["ContentType"] = content_type
        # boto3's upload_fileobj uses multipart under the hood when TransferConfig applies
        self.s3.upload_fileobj(
            Fileobj=fileobj,
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args,
            Config=self.transfer_config,
        )

    def download_to_file(
        self, key: str, dest_path: str, bucket: Optional[str] = None
    ) -> None:
        bucket = bucket or self.default_bucket
        if not bucket:
            raise ValueError("Bucket name required")
        # download_file streams to disk
        self.s3.download_file(Bucket=bucket, Key=key, Filename=dest_path)

    def generate_presigned_url(
        self,
        key: str,
        expires_in: int = 3600,
        bucket: Optional[str] = None,
        method: str = "get_object",
    ) -> str:
        bucket = bucket or self.default_bucket
        if not bucket:
            raise ValueError("Bucket name required")
        return self.s3.generate_presigned_url(
            ClientMethod=method,
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )


# # ----------------- Example quick-run (only for local testing) -----------------
# if __name__ == "__main__":
#     # Quick sanity: build a client from environment variables and upload a small test
#     client = MinioS3Client()
#     bucket = client.default_bucket or "reports"
#     client.ensure_bucket(bucket)
#     params = {"report": "revenue", "from": "2025-01-01", "to": "2025-01-31"}
#     key = client.build_report_key(
#         "revenue", datetime(2025, 1, 31), "client-123", params
#     )
#     print("Uploading test to:", key)
#     client.upload_bytes(key, b"id,value\n1,100\n")
#     print("Exists?", client.exists(key))
#     print("Presigned URL:", client.generate_presigned_url(key, expires_in=600))
