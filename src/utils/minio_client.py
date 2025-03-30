import json
import io
import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

class MinioClient:
    """
    A wrapper class for interacting with MinIO storage.

    Provides methods to upload JSON data directly to MinIO.
    """

    def __init__(self,secure: bool = False):
        """
        Initialize MinIO client with credentials fetched from Vault.
        """
        load_dotenv()
        minio_endpoint = os.getenv("MINIO_ENDPOINT","minio:9000")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY","minio")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY","minio")

        try:

            # Initialize MinIO client
            self.minio = Minio(
                endpoint=minio_endpoint,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=secure
            )
        except Exception as e:
            raise Exception(f"Error connecting to Vault or MinIO: {str(e)}")

    def upload_json(self, bucket: str, object_name: str, json_data: dict) -> None:
        """
        Uploads a JSON object to the specified MinIO bucket.

        Args:
            bucket (str): The name of the MinIO bucket.
            object_name (str): The name to assign to the uploaded object.
            json_data (dict): The JSON data to upload.

        Raises:
            S3Error: If there is an issue uploading the file.
        """
        # Convert JSON object to a byte stream
        json_bytes = json.dumps(json_data, indent=4).encode("utf-8")
        json_stream = io.BytesIO(json_bytes)
        json_size = len(json_bytes)

        # Ensure the bucket exists before uploading
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

        try:
            result = self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=json_stream,
                length=json_size,
                content_type="application/json"
            )

            print(
                f"Uploaded {result.object_name} to bucket {bucket}; "
                f"ETag: {result.etag}, Version ID: {result.version_id}"
            )

        except S3Error as err:
            print(f"MinIO Upload Error: {err}")
