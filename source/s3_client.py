import time
import logging
import boto3
from botocore.exceptions import ClientError
from source.config import settings

logger = logging.getLogger("dls.s3")

class S3Client:
    def __init__(self):
        self.s3 = None
        self._connect()

    def _connect(self):
        # Retry connection for startup sync
        retries = 5
        while retries > 0:
            try:
                self.s3 = boto3.client(
                    's3',
                    endpoint_url=settings.S3_ENDPOINT,
                    aws_access_key_id=settings.S3_ACCESS_KEY,
                    aws_secret_access_key=settings.S3_SECRET_KEY,
                    region_name='us-east-1' # Default mock region for MinIO
                )
                self._ensure_bucket_exists()
                logger.info(f"Connected to MinIO/S3 storage at {settings.S3_ENDPOINT}")
                return
            except Exception as e:
                logger.warning(f"Failed to connect to S3. Retrying in 2s... Error: {e}")
                retries -= 1
                time.sleep(2)
        logger.error("Could not connect to S3. Running in fallback/mock mode.")
        self.s3 = None

    def _ensure_bucket_exists(self):
        if not self.s3:
            return
        try:
            self.s3.head_bucket(Bucket=settings.S3_BUCKET)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                try:
                    self.s3.create_bucket(Bucket=settings.S3_BUCKET)
                    logger.info(f"Created MinIO archive bucket: '{settings.S3_BUCKET}'")
                except Exception as ex:
                    logger.error(f"Failed to create bucket: {ex}")
            else:
                logger.error(f"Error checking bucket existence: {e}")

    def upload_archive(self, file_key: str, data_str: str) -> bool:
        """Uploads archived log string directly as a file object to MinIO."""
        if not self.s3:
            logger.warning(f"S3 unavailable. Mock-uploading {file_key} ({len(data_str)} bytes)")
            return True
        try:
            self.s3.put_object(
                Bucket=settings.S3_BUCKET,
                Key=file_key,
                Body=data_str.encode('utf-8'),
                ContentType='application/json'
            )
            logger.info(f"Archived logs to S3 cold storage: {file_key}")
            return True
        except Exception as e:
            logger.error(f"S3 upload failed for {file_key}: {e}")
            return False

    def list_archives(self):
        """Lists metadata of all objects in the archive bucket."""
        if not self.s3:
            return []
        try:
            res = self.s3.list_objects_v2(Bucket=settings.S3_BUCKET)
            return res.get('Contents', [])
        except Exception as e:
            logger.error(f"S3 list objects failed: {e}")
            return []

    def download_archive(self, file_key: str) -> str:
        """Downloads and returns the text contents of an archived file."""
        if not self.s3:
            return ""
        try:
            res = self.s3.get_object(Bucket=settings.S3_BUCKET, Key=file_key)
            return res['Body'].read().decode('utf-8')
        except Exception as e:
            logger.error(f"S3 download failed for {file_key}: {e}")
            return ""

    def delete_archive(self, file_key: str) -> bool:
        """Deletes an archived file from cold storage (enforces cold retention)."""
        if not self.s3:
            return True
        try:
            self.s3.delete_object(Bucket=settings.S3_BUCKET, Key=file_key)
            logger.info(f"Deleted expired archive from S3 cold storage: {file_key}")
            return True
        except Exception as e:
            logger.error(f"S3 delete failed for {file_key}: {e}")
            return False

# Singleton instance
s3_client = S3Client()
